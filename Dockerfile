###############################################################################
# BUILD STAGE

FROM golang:alpine

ADD ./kafkacat.go /go

RUN apk --no-cache update && apk --no-cache add \
    build-base \
    gcc \
    && apk --no-cache add \
        git \
        pkgconf \
        bash \
    && mkdir librdkafka \
    && git clone https://github.com/edenhill/librdkafka.git librdkafka \
    && cd librdkafka \
    && ./configure \
    && make \
    && make install \
    && cd .. \
    && rm -rf librdkafka \
    && cd /go \
    && go get github.com/confluentinc/confluent-kafka-go/kafka \
    && go get gopkg.in/alecthomas/kingpin.v2 \
    && go build ./kafkacat.go

###############################################################################
# PACKAGE STAGE

FROM alpine

RUN apk --no-cache update && apk --no-cache add --virtual build-dependencies \
    build-base \
    gcc \
    && apk --no-cache add \
        git \
        pkgconf \
        bash \
    && mkdir librdkafka \
    && git clone https://github.com/edenhill/librdkafka.git librdkafka \
    && cd librdkafka \
    && ./configure \
    && make \
    && make install \
    && cd .. \
    && rm -rf librdkafka \
    && apk del \
        build-dependencies \
        git \
        bash

ENTRYPOINT ["./kafkacat"]

COPY --from=0 /go/kafkacat /kafkacat
