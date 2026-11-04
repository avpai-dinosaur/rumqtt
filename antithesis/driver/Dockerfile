# syntax=docker/dockerfile:1
# Build context: the rumqtt repository root (see antithesis/docker-compose.yaml).
FROM rust:1-bookworm AS build
WORKDIR /src
COPY antithesis/driver/ .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release && cp target/release/driver /driver

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /driver /usr/local/bin/driver
COPY antithesis/driver/first_start_workload /opt/antithesis/test/v1/main/first_start_workload
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
EXPOSE 7777/udp 7778/udp
CMD ["/usr/local/bin/driver"]
