FROM rust:1-bookworm AS build

RUN apt-get update \
	&& apt-get install -y --no-install-recommends protobuf-compiler \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
# Packaged deployments opt into the cluster/native/OIDC/vault/embedding
# features; the default feature set is the standalone HTTP-only build.
RUN cargo build --release --locked --manifest-path crates/mongreldb-server/Cargo.toml --bin mongreldb-server --features cluster,native-rpc,oidc,vault-kms,remote-embedding

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/mongreldb-server /usr/local/bin/mongreldb-server

VOLUME ["/data"]
EXPOSE 8453
ENTRYPOINT ["mongreldb-server"]
CMD ["/data", "8453"]
