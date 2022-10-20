FROM reg.qa.91jkys.com/lang/rust-ci:10.19 as builder

ARG CARGO_FEATURES=quickwit-metastore/postgres,openssl-support

ENV SCCACHE_REDIS redis://172.17.0.1 \
    RUSTC_WRAPPER=sccache

COPY . ./quickwit

WORKDIR /quickwit

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo +stable build \
         -r --features $CARGO_FEATURES \
    && mkdir -p /quickwit/bin \
    && mv target/release/quickwit  /quickwit/bin/quickwit

FROM ubuntu:22.04 AS quickwit

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          libpq-dev \
                          libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /quickwit/bin/quickwit /usr/local/bin/quickwit

CMD ["/usr/local/bin/quickwit --version"]
