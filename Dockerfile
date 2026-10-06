# syntax=docker/dockerfile:1

# Build stage (cargo-chef for caching)
FROM rust:1.94-bookworm AS chef

WORKDIR /app

RUN cargo install cargo-chef --locked

FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
COPY bin/ bin/
COPY skills/ skills/
COPY tests/ tests/
COPY xtask/ xtask/
# Compiled into demo-wiki via include_str!
COPY docs/architecture/living-wiki.mdx docs/architecture/living-wiki.mdx

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

WORKDIR /app

# Reduce memory pressure during Docker builds
ENV CARGO_PROFILE_RELEASE_LTO=false
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
# cargo-chef does not reconstruct excluded path dependencies.
COPY vendor/ vendor/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo chef cook --locked --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ vendor/
COPY bin/ bin/
COPY skills/ skills/
COPY tests/ tests/
COPY xtask/ xtask/
# Compiled into demo-wiki via include_str!
COPY docs/architecture/living-wiki.mdx docs/architecture/living-wiki.mdx

# Build release binaries with BuildKit cache mounts for fast incremental builds
# Enable meerkat-extraction for AgenticExtractor with OutputSchema
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release -p elephant-bin --features meerkat-extraction && \
    cp /app/target/release/elephant /tmp/

# Runtime stage
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false elephant

WORKDIR /app

# Copy binaries from builder (from /tmp/ since target is a cache mount)
COPY --from=builder /tmp/elephant /usr/local/bin/

# Copy migrations (SQL files)
COPY --from=builder /app/crates/migrations/sql/ /app/migrations/
COPY --from=builder /app/skills/ /app/skills/
COPY LICENSE LICENSE-MIT LICENSE-APACHE THIRD_PARTY_NOTICES.md /usr/share/licenses/elephant/

# Set ownership
RUN chown -R elephant:elephant /app

USER elephant

# Default environment
ENV RUST_LOG=info
ENV ELEPHANT_HOST=0.0.0.0
ENV ELEPHANT_PORT=3000
ENV SURREALDB_URL=ws://surrealdb:8000
ENV SURREALDB_NAMESPACE=elephant
ENV SURREALDB_DATABASE=main
ENV PIPELINE_SKILLS_DIR=/app/skills

EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/v1/health || exit 1

CMD ["elephant", "serve"]
