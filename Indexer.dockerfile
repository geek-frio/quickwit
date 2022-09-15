FROM reg.qa.91jkys.com/lang/rust-ci:latest as builder

ARG CARGO_FEATURES=release-feature-set

RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
    sed -i s@/security.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \

RUN apt-get -y install cmake

COPY . ./quickwit

WORKDIR /quickwit

RUN --mount=type=cache,target=/root/.cargo/registry \
        --mount=type=cache,target=/root/.cargo/git \
        --mount=type=cache,target=/home/root/src/target \
    echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo +stable build \
         -r --features $CARGO_FEATURES \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;

# Change the default configuration file in order to make the REST,
# gRPC, and gossip services accessible outside of Docker container.
COPY ./config/quickwit.yaml ./config/quickwit.yaml
RUN sed -i 's/#[ ]*listen_address: 127.0.0.1/listen_address: 0.0.0.0/g' ./config/quickwit.yaml

FROM ubuntu:22.04 AS quickwit

RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
    sed -i s@/security.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \

RUN apt-get -y update \
    && apt-get -y install libpq-dev \
                          libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=builder /quickwit/bin/quickwit /usr/local/bin/quickwit
COPY --from=builder /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata

ENTRYPOINT ["/usr/local/bin/quickwit"]