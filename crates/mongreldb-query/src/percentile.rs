use arrow::array::{Array, ArrayRef, AsArray, Float64Array, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use std::fmt::Debug;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

const P_EQUAL_EPSILON: f64 = 0.001;

pub(crate) fn percentile_udafs() -> Vec<AggregateUDF> {
    vec![
        AggregateUDF::from(PercentileFunc::median()),
        AggregateUDF::from(PercentileFunc::percentile()),
        AggregateUDF::from(PercentileFunc::percentile_cont()),
        AggregateUDF::from(PercentileFunc::percentile_disc()),
    ]
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PercentileFunc {
    name: &'static str,
    kind: PercentileKind,
    signature: Signature,
}

impl PercentileFunc {
    fn median() -> Self {
        Self {
            name: "median",
            kind: PercentileKind::Median,
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }

    fn percentile() -> Self {
        Self {
            name: "percentile",
            kind: PercentileKind::Percentile,
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }

    fn percentile_cont() -> Self {
        Self {
            name: "percentile_cont",
            kind: PercentileKind::PercentileCont,
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }

    fn percentile_disc() -> Self {
        Self {
            name: "percentile_disc",
            kind: PercentileKind::PercentileDisc,
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PercentileFunc {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let expected = self.kind.arg_count();
        if arg_types.len() != expected {
            return Err(DataFusionError::Plan(format!(
                "{} expects {expected} argument{}, got {}",
                self.name,
                if expected == 1 { "" } else { "s" },
                arg_types.len()
            )));
        }
        if !arg_types.iter().all(is_numeric_or_null) {
            return Err(DataFusionError::Plan(format!(
                "{} arguments must be numeric",
                self.name
            )));
        }
        Ok(vec![DataType::Float64; expected])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let values_field = Field::new_list_field(DataType::Float64, true);
        let mut fields: Vec<FieldRef> = vec![Arc::new(Field::new(
            format!("{}_values", args.name),
            DataType::List(Arc::new(values_field)),
            true,
        ))];
        if self.kind.uses_percentile_arg() {
            let p_field = Field::new_list_field(DataType::Float64, true);
            fields.push(Arc::new(Field::new(
                format!("{}_percentiles", args.name),
                DataType::List(Arc::new(p_field)),
                true,
            )));
        }
        Ok(fields)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileAccumulator {
            kind: self.kind,
            values: Vec::new(),
            percentiles: Vec::new(),
        }))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum PercentileKind {
    Median,
    Percentile,
    PercentileCont,
    PercentileDisc,
}

impl PercentileKind {
    fn arg_count(self) -> usize {
        match self {
            PercentileKind::Median => 1,
            PercentileKind::Percentile
            | PercentileKind::PercentileCont
            | PercentileKind::PercentileDisc => 2,
        }
    }

    fn uses_percentile_arg(self) -> bool {
        self.arg_count() == 2
    }

    fn percentile_bounds(self) -> (f64, f64) {
        match self {
            PercentileKind::Median
            | PercentileKind::PercentileCont
            | PercentileKind::PercentileDisc => (0.0, 1.0),
            PercentileKind::Percentile => (0.0, 100.0),
        }
    }

    fn percentile_fraction(self, p: Option<f64>) -> Result<f64> {
        match self {
            PercentileKind::Median => Ok(0.5),
            PercentileKind::Percentile => {
                Ok(validate_percentile(self, required_percentile_arg(p)?)? / 100.0)
            }
            PercentileKind::PercentileCont | PercentileKind::PercentileDisc => {
                validate_percentile(self, required_percentile_arg(p)?)
            }
        }
    }

    fn is_discrete(self) -> bool {
        matches!(self, PercentileKind::PercentileDisc)
    }
}

#[derive(Debug)]
struct PercentileAccumulator {
    kind: PercentileKind,
    values: Vec<f64>,
    percentiles: Vec<f64>,
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != self.kind.arg_count() {
            return Err(DataFusionError::Execution(format!(
                "percentile accumulator expected {} arguments, got {}",
                self.kind.arg_count(),
                values.len()
            )));
        }

        let y = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("percentile Y must be Float64".into()))?;
        let p = if self.kind.uses_percentile_arg() {
            Some(
                values[1]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("percentile P must be Float64".into())
                    })?,
            )
        } else {
            None
        };

        for row in 0..y.len() {
            if let Some(p) = p {
                if p.is_null(row) {
                    return Err(DataFusionError::Execution(
                        "percentile P must not be NULL".into(),
                    ));
                }
                let value = p.value(row);
                validate_percentile(self.kind, value)?;
                self.percentiles.push(value);
            }
            if y.is_null(row) {
                continue;
            }
            let value = y.value(row);
            if value.is_nan() {
                continue;
            }
            if !value.is_finite() {
                return Err(DataFusionError::Execution(
                    "percentile Y must not be infinite".into(),
                ));
            }
            self.values.push(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        let fraction = self.kind.percentile_fraction(self.constant_percentile()?)?;
        let result = percentile_value(&mut self.values, fraction, self.kind.is_discrete());
        Ok(ScalarValue::Float64(result))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<f64>()
            + self.percentiles.capacity() * size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut state = vec![ScalarValue::List(Arc::new(float_list(&self.values)))];
        if self.kind.uses_percentile_arg() {
            state.push(ScalarValue::List(Arc::new(float_list(&self.percentiles))));
        }
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        self.extend_from_list_state(&states[0], StatePart::Values)?;
        if self.kind.uses_percentile_arg() {
            if states.len() < 2 {
                return Err(DataFusionError::Execution(
                    "percentile merge state is missing P values".into(),
                ));
            }
            self.extend_from_list_state(&states[1], StatePart::Percentiles)?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != self.kind.arg_count() {
            return Err(DataFusionError::Execution(format!(
                "percentile accumulator expected {} retraction arguments, got {}",
                self.kind.arg_count(),
                values.len()
            )));
        }

        let y = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Execution("percentile Y must be Float64".into()))?;
        let p = if self.kind.uses_percentile_arg() {
            Some(
                values[1]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("percentile P must be Float64".into())
                    })?,
            )
        } else {
            None
        };

        for row in 0..y.len() {
            if let Some(p) = p {
                if !p.is_null(row) {
                    remove_one(&mut self.percentiles, p.value(row));
                }
            }
            if !y.is_null(row) {
                let value = y.value(row);
                if !value.is_nan() {
                    remove_one(&mut self.values, value);
                }
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

impl PercentileAccumulator {
    fn constant_percentile(&self) -> Result<Option<f64>> {
        let Some(first) = self.percentiles.first().copied() else {
            return Ok(None);
        };
        for &value in &self.percentiles[1..] {
            if (value - first).abs() >= P_EQUAL_EPSILON {
                return Err(DataFusionError::Execution(
                    "percentile P must be the same for every row in the aggregate".into(),
                ));
            }
        }
        Ok(Some(first))
    }

    fn extend_from_list_state(&mut self, state: &ArrayRef, part: StatePart) -> Result<()> {
        let lists = state.as_list::<i32>();
        for maybe_values in lists.iter().flatten() {
            let values = maybe_values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("percentile state must be Float64 list".into())
                })?;
            for value in values.iter().flatten() {
                match part {
                    StatePart::Values => self.values.push(value),
                    StatePart::Percentiles => self.percentiles.push(value),
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum StatePart {
    Values,
    Percentiles,
}

fn is_numeric_or_null(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Float64
            | DataType::Float32
            | DataType::Float16
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn validate_percentile(kind: PercentileKind, value: f64) -> Result<f64> {
    let (lo, hi) = kind.percentile_bounds();
    if !value.is_finite() || value < lo || value > hi {
        return Err(DataFusionError::Execution(format!(
            "percentile P must be between {lo} and {hi} inclusive"
        )));
    }
    Ok(value)
}

fn required_percentile_arg(value: Option<f64>) -> Result<f64> {
    value.ok_or_else(|| DataFusionError::Execution("percentile P is required".into()))
}

fn percentile_value(values: &mut [f64], fraction: f64, discrete: bool) -> Option<f64> {
    values.sort_by(|a, b| a.total_cmp(b));
    let n = values.len();
    if n == 0 {
        return None;
    }
    if n == 1 {
        return Some(values[0]);
    }
    let rank = fraction * (n - 1) as f64;
    let lower = rank.floor() as usize;
    if discrete {
        return Some(values[lower]);
    }
    let upper = rank.ceil() as usize;
    if lower == upper {
        return Some(values[lower]);
    }
    let weight = rank - lower as f64;
    Some(values[lower] + (values[upper] - values[lower]) * weight)
}

fn float_list(values: &[f64]) -> ListArray {
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
    let values_array = Float64Array::from(values.to_vec());
    ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        offsets,
        Arc::new(values_array),
        None,
    )
}

fn remove_one(values: &mut Vec<f64>, value: f64) {
    if let Some(index) = values.iter().position(|candidate| *candidate == value) {
        values.swap_remove(index);
    }
}
