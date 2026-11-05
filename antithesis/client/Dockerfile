# Build context is the repo root (sut/rumqtt), e.g.:
#   docker build -f antithesis/client/Dockerfile -t $REPOSITORY/rumqttc:latest .
#
# glibc (not alpine/musl): the Antithesis SDK dlopen()s /usr/lib/libvoidstar.so
# at runtime, which fails in a statically linked musl binary and silently drops
# all SDK events (setup_complete, assertions).
FROM rust:bookworm as builder
WORKDIR /usr/src/rumqtt
COPY . .
RUN cargo build --release -p antithesis-client

FROM debian:bookworm-slim
COPY --from=builder /usr/src/rumqtt/target/release/client /usr/local/bin/client
ENTRYPOINT ["client"]
