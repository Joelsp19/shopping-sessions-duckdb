# Shopping Sessions: native Rust pipeline (team Ardere).
# The scorers are linux/amd64 (Intel Xeon, Cascade Lake or newer). They build
# this file natively; no FROM line pins a platform.

# ---- build: compile the single static-ish binary (build time is never timed)
FROM rust:1.87-slim-bookworm AS build
WORKDIR /src
# Cascade Lake and newer all have AVX2/BMI2 (x86-64-v3).
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY rust/Cargo.toml rust/Cargo.lock ./
# Fetch and build dependencies first, in their own cached layer.
RUN mkdir src && echo "fn main() {}" > src/main.rs && touch src/parse.rs src/out.rs \
    && cargo build --release --locked && rm -rf src
COPY rust/src ./src
RUN touch src/main.rs && cargo build --release --locked && strip target/release/sessions

# ---- run: just the binary. Root FS is read-only at run time; it writes only
# to $OUTPUT_DIR and needs no temp space (everything stays in memory).
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
CMD ["/app/sessions"]
