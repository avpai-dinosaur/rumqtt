# Antithesis build of rumqttd: coverage-instrumented, with SDK + symbols.
# Build context is the repo root (sut/rumqtt), e.g.:
#   docker build -f antithesis/broker/Dockerfile -t $REPOSITORY/rumqttd:latest .
#
# glibc (not alpine/musl): the instrumentation runtime and SDK dlopen()
# /usr/lib/libvoidstar.so, which fails in a statically linked musl binary.
FROM rust:bookworm as builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/rumqtt
COPY . .
# The workspace release profile uses lto + strip; neither is compatible with
# compile-time sancov instrumentation and symbolization, so override them here.
ENV CARGO_PROFILE_RELEASE_LTO=false \
    CARGO_PROFILE_RELEASE_STRIP=false \
    CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
RUN cargo build --release -p rumqttd --features antithesis \
    --config 'build.target = "x86_64-unknown-linux-gnu"' \
    --config 'target.x86_64-unknown-linux-gnu.rustflags = [ \
        "-Ccodegen-units=1", \
        "-Cpasses=sancov-module", \
        "-Cllvm-args=-sanitizer-coverage-level=3", \
        "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", \
        "-Cllvm-args=-sanitizer-coverage-pc-table", \
        "-Clink-args=-Wl,--build-id", \
    ]'
# Fail the build early if instrumentation didn't take.
RUN BIN=target/x86_64-unknown-linux-gnu/release/rumqttd \
    && nm "$BIN" | grep -q 'T antithesis_load_libvoidstar' \
    && readelf -S "$BIN" | grep -q __sancov_guards

FROM debian:bookworm-slim
COPY --from=builder /usr/src/rumqtt/target/x86_64-unknown-linux-gnu/release/rumqttd /usr/local/bin/rumqttd
RUN mkdir /symbols && ln -s /usr/local/bin/rumqttd /symbols/rumqttd
ENV RUST_LOG="info" \
    NO_COLOR=1
ENTRYPOINT ["rumqttd"]
