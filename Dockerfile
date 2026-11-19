# syntax=docker/dockerfile:1
FROM rust:1.100-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    find crates -name '*.rs' -exec touch {} + \
    && cargo build --locked --release --package rusta-broker \
    && install -s -m 0755 target/release/rusta-broker /usr/local/bin/rusta

FROM debian:bookworm-slim
RUN install -d -o 65532 -g 65532 /var/lib/rusta
COPY --from=build /usr/local/bin/rusta /usr/local/bin/rusta
ENV RUSTA_DATA=/var/lib/rusta
USER 65532:65532
EXPOSE 9092
ENTRYPOINT ["/usr/local/bin/rusta"]
