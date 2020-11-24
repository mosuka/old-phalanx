FROM rust:1.48.0-slim-buster AS builder

ARG BAYARD_VERSION

WORKDIR /repo

RUN set -ex \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           build-essential \
           cmake \
           pkg-config \
           libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN rustup component add rustfmt --toolchain 1.48.0-x86_64-unknown-linux-gnu

RUN cargo build --release


FROM debian:buster-slim

WORKDIR /

RUN set -ex \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /repo/target/release/phalanx /usr/local/bin/.
COPY --from=builder /repo/etc/* /etc/phalanx/

EXPOSE 5000 8000

ENTRYPOINT [ "phalanx" ]
CMD [ "index" ]