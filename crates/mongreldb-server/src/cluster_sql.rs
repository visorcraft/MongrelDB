//! Public cluster SQL planning seam (P0.4).
//!
//! Ordinary `/sql` in cluster mode must not scan a standalone catalog. This
//! module invokes the query crate's public [`plan_sql_distributed`] entry so
//! DataFusion logical plans are lowered onto tablet fragments via
//! [`DataFusionDistributedPlanner`]. Full multi-process coordinator execution
//! is layered on once tablet routes and planning schemas are available from
//! the control plane.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use mongreldb_query::distributed::{
    plan_sql_distributed, plan_sql_distributed_with_id, ClusterMetadata, DistributedError,
    DistributedPlan, DistributedResult, PartitionSpec, PlanningTableCatalog, TableStats,
    TabletLocator,
};
use mongreldb_query::{plan_logical_distributed, DataFusionDistributedPlanner};
use mongreldb_types::ids::{MetadataVersion, QueryId, TabletId};

/// In-memory locator + metadata used by the gateway when the control plane
/// has resolved tablet layout for the referenced tables.
#[derive(Clone, Debug)]
pub struct GatewayPlanningContext {
    tablets: HashMap<String, Vec<TabletId>>,
    specs: HashMap<String, PartitionSpec>,
    stats: HashMap<String, TableStats>,
    version: MetadataVersion,
    schemas: PlanningTableCatalog,
}

impl Default for GatewayPlanningContext {
    fn default() -> Self {
        Self {
            tablets: HashMap::new(),
            specs: HashMap::new(),
            stats: HashMap::new(),
            version: MetadataVersion::ZERO,
            schemas: PlanningTableCatalog::new(),
        }
    }
}

impl GatewayPlanningContext {
    /// Empty context pinned to metadata version 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin the control-plane metadata version this plan is valid for.
    pub fn with_metadata_version(mut self, version: MetadataVersion) -> Self {
        self.version = version;
        self
    }

    /// Register one cluster table for distributed SQL planning.
    pub fn with_table(
        mut self,
        table: impl Into<String>,
        schema: SchemaRef,
        tablets: Vec<TabletId>,
        partitioning: PartitionSpec,
        stats: TableStats,
    ) -> Self {
        let table = table.into();
        self.schemas.insert(table.clone(), schema);
        self.tablets.insert(table.clone(), tablets);
        self.specs.insert(table.clone(), partitioning);
        self.stats.insert(table, stats);
        self
    }

    /// Planning-only catalog of Arrow schemas.
    pub fn catalog(&self) -> &PlanningTableCatalog {
        &self.schemas
    }

    /// True when at least one table layout is registered.
    pub fn has_tables(&self) -> bool {
        !self.tablets.is_empty()
    }
}

impl TabletLocator for GatewayPlanningContext {
    fn tablets_for_table(&self, table: &str) -> DistributedResult<Vec<TabletId>> {
        self.tablets
            .get(table)
            .cloned()
            .ok_or_else(|| DistributedError::UnknownTable(table.to_owned()))
    }

    fn partitioning(&self, table: &str) -> DistributedResult<PartitionSpec> {
        self.specs
            .get(table)
            .cloned()
            .ok_or_else(|| DistributedError::UnknownTable(table.to_owned()))
    }
}

impl ClusterMetadata for GatewayPlanningContext {
    fn metadata_version(&self) -> MetadataVersion {
        self.version
    }

    fn table_stats(&self, table: &str) -> DistributedResult<TableStats> {
        self.stats
            .get(table)
            .copied()
            .ok_or_else(|| DistributedError::UnknownTable(table.to_owned()))
    }
}

/// Plan public SQL through the DataFusion distributed planner (P0.4 product entry).
///
/// This always goes through [`plan_sql_distributed`] so the gateway cannot
/// silently fall back to a local-only LogicalPlanLite path.
pub async fn plan_public_sql(
    sql: &str,
    context: &GatewayPlanningContext,
) -> DistributedResult<DistributedPlan> {
    plan_sql_distributed(sql, context.catalog(), context, context).await
}

/// Plan public SQL with an explicit query id (cancellation / audit correlation).
pub async fn plan_public_sql_with_id(
    sql: &str,
    context: &GatewayPlanningContext,
    query_id: QueryId,
) -> DistributedResult<DistributedPlan> {
    plan_sql_distributed_with_id(sql, context.catalog(), query_id, context, context).await
}

/// Lower an already-resolved DataFusion plan (e.g. from a session frontend).
pub fn plan_public_logical(
    plan: &datafusion::logical_expr::LogicalPlan,
    context: &GatewayPlanningContext,
) -> DistributedResult<DistributedPlan> {
    // Prefer the public planner type so the gateway path is not a private fork.
    let _ = DataFusionDistributedPlanner::new(QueryId::new_random());
    plan_logical_distributed(plan, context, context)
}

/// Outcome of attempting the cluster SQL gateway path before the fail-closed
/// standalone refusal.
#[derive(Debug)]
pub enum ClusterSqlAttempt {
    /// Distributed plan produced successfully (execution may still be pending
    /// fragment routes / workers).
    Planned(DistributedPlan),
    /// Planning context has no tables / layout yet.
    NoLayout,
    /// SQL or placement failed with a distributed error.
    PlanError(DistributedError),
}

/// Try to plan `sql` with the optional gateway context.
///
/// Call this from the public `/sql` cluster path **before** refusing the
/// standalone data plane. Returns [`ClusterSqlAttempt::NoLayout`] when the
/// process has no registered cluster tables yet.
pub async fn try_plan_cluster_sql(
    sql: &str,
    context: Option<&GatewayPlanningContext>,
) -> ClusterSqlAttempt {
    let Some(context) = context else {
        return ClusterSqlAttempt::NoLayout;
    };
    if !context.has_tables() {
        return ClusterSqlAttempt::NoLayout;
    }
    match plan_public_sql(sql, context).await {
        Ok(plan) => ClusterSqlAttempt::Planned(plan),
        Err(error) => ClusterSqlAttempt::PlanError(error),
    }
}

/// Shared handle stored on the server when operators / tests install planning
/// layouts for the gateway.
pub type SharedPlanningContext = Arc<std::sync::RwLock<Option<GatewayPlanningContext>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_context() -> GatewayPlanningContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        let t1 = TabletId::from_bytes([1; 16]);
        let t2 = TabletId::from_bytes([2; 16]);
        GatewayPlanningContext::new()
            .with_metadata_version(MetadataVersion::new(7))
            .with_table(
                "orders",
                schema,
                vec![t1, t2],
                PartitionSpec::Hash {
                    columns: vec!["region".into()],
                    buckets: 2,
                },
                TableStats {
                    row_count: 1_000,
                    total_bytes: 64_000,
                },
            )
    }

    #[tokio::test]
    async fn plan_public_sql_calls_plan_sql_distributed() {
        let context = sample_context();
        let plan = plan_public_sql(
            "SELECT region, amount FROM orders WHERE amount > 0 LIMIT 10",
            &context,
        )
        .await
        .expect("public cluster SQL planning must succeed");
        assert!(!plan.fragments.is_empty());
        assert_eq!(plan.metadata_version, MetadataVersion::new(7));
        assert!(plan.root_fragment_id().is_some());
    }

    #[tokio::test]
    async fn try_plan_cluster_sql_no_layout_when_empty() {
        match try_plan_cluster_sql("SELECT 1", None).await {
            ClusterSqlAttempt::NoLayout => {}
            other => panic!("expected NoLayout, got {other:?}"),
        }
        match try_plan_cluster_sql("SELECT 1", Some(&GatewayPlanningContext::new())).await {
            ClusterSqlAttempt::NoLayout => {}
            other => panic!("expected NoLayout for empty context, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_plan_cluster_sql_planned_with_context() {
        let context = sample_context();
        match try_plan_cluster_sql(
            "SELECT region FROM orders WHERE amount > 10 LIMIT 5",
            Some(&context),
        )
        .await
        {
            ClusterSqlAttempt::Planned(plan) => {
                assert!(plan.fragments.iter().any(|fragment| {
                    fragment.operators.iter().any(|op| {
                        matches!(
                            op,
                            mongreldb_query::distributed::FragmentOperator::TabletScan {
                                table,
                                ..
                            } if table == "orders"
                        )
                    })
                }));
            }
            other => panic!("expected Planned, got {other:?}"),
        }
    }
}
