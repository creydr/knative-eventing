# DO NOT EDIT! Generated Dockerfile for cmd/in_memory/channel_dispatcher.
FROM registry.ci.openshift.org/openshift/release:golang-1.18 as builder

COPY . .

RUN mkdir -p /var/run/ko && \
    mkdir -p cmd/in_memory/channel_dispatcher/kodata && \
    go build -o /usr/bin/main ./cmd/in_memory/channel_dispatcher && \
    cp -r cmd/in_memory/channel_dispatcher/kodata /var/run/ko

FROM registry.access.redhat.com/ubi8/ubi-minimal
USER 65532

COPY --from=builder /usr/bin/main /usr/bin/main
COPY --from=builder /var/run/ko /var/run/ko
ENTRYPOINT ["/usr/bin/main"]
