FROM rust:1-bookworm AS build

RUN apt-get update \
	&& apt-get install -y --no-install-recommends protobuf-compiler \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release --locked --manifest-path crates/mongreldb-server/Cargo.toml --bin mongreldb-server

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=build /src/crates/mongreldb-server/target/release/mongreldb-server /usr/local/bin/mongreldb-server

VOLUME ["/data"]
EXPOSE 8453
ENTRYPOINT ["mongreldb-server"]
CMD ["/data", "8453"]
