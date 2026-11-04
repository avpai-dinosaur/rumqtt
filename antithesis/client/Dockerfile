# syntax=docker/dockerfile:1
# Build context: the rumqtt repository root (see antithesis/docker-compose.yaml).
FROM rust:1-bookworm AS log-forward
RUN rustup target add x86_64-unknown-linux-musl
COPY antithesis/driver/log-forward/ /src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cd /src && cargo build --release --target x86_64-unknown-linux-musl

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
# Exploration assertions inside rumqttd/rumqttc, applied only to the Antithesis build.
RUN patch -p1 < antithesis/patches/sut-assertions.patch
WORKDIR /src/antithesis/client
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/antithesis/client/target \
    cargo build --release \
    && cp target/x86_64-unknown-linux-gnu/release/mqtt-client /mqtt-client \
    && nm /mqtt-client | grep -q "T antithesis_load_libvoidstar" \
    && readelf -S /mqtt-client | grep -q sancov

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /mqtt-client /usr/local/bin/mqtt-client
RUN mkdir -p /symbols && ln -s /usr/local/bin/mqtt-client /symbols/mqtt-client
COPY --from=log-forward /src/target/x86_64-unknown-linux-musl/release/log-forward /usr/local/bin/
COPY antithesis/driver/forward-logs /usr/local/bin/forward-logs
RUN chmod 755 /usr/local/bin/forward-logs
EXPOSE 8080
ENTRYPOINT ["forward-logs", "mqtt-client"]
