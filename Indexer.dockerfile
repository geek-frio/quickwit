FROM reg.qa.91jkys.com/lang/rust-ci:9.16 as builder

ARG CARGO_FEATURES=quickwit-metastore/postgres,openssl-support

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

FROM ubuntu:22.04 AS quickwit

RUN apt-get -y update \
    && apt-get -y install libpq-dev \
                          libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
COPY --from=builder /quickwit/bin/quickwit /usr/local/bin/quickwit

CMD ["/usr/local/bin/quickwit --version"]