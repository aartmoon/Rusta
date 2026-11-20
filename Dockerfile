# syntax=docker/dockerfile:1@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
FROM rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rusta-target-${TARGETARCH},target=/src/target,sharing=locked \
    set -eu; \
    case "$TARGETARCH" in \
        amd64) protoc_arch=x86_64 ;; \
        arm64) protoc_arch=aarch_64 ;; \
        *) echo "Unsupported build architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    host="$(rustc -vV | sed -n 's/^host: //p')"; \
    cargo fetch --locked --target "$host"; \
    # Use the compiler bundled by the locked Raft dependency on both platforms. \
    set -- /usr/local/cargo/registry/src/*/protobuf-build-0.14.1/bin/protoc-linux-"$protoc_arch"; \
    test "$#" -eq 1; \
    test -x "$1"; \
    find crates -name '*.rs' -exec touch {} +; \
    PROTOC="$1" cargo build --locked --release --package rusta-broker; \
    install -s -m 0755 target/release/rusta-broker /usr/local/bin/rusta

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
ARG RUSTA_VERSION
ARG RUSTA_REVISION
LABEL org.opencontainers.image.source="https://github.com/aartmoon/Rusta" \
      org.opencontainers.image.version="${RUSTA_VERSION}" \
      org.opencontainers.image.revision="${RUSTA_REVISION}"
RUN install -d -o 65532 -g 65532 /var/lib/rusta
COPY --from=build /usr/local/bin/rusta /usr/local/bin/rusta
ENV RUSTA_DATA=/var/lib/rusta
USER 65532:65532
EXPOSE 9092
ENTRYPOINT ["/usr/local/bin/rusta"]
