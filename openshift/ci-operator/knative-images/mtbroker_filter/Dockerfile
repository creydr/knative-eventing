# DO NOT EDIT! Generated Dockerfile for cmd/broker/filter.
FROM registry.ci.openshift.org/openshift/release:golang-1.18 as builder

COPY . .

RUN mkdir -p /var/run/ko && \
    mkdir -p cmd/broker/filter/kodata && \
    go build -o /usr/bin/main ./cmd/broker/filter && \
    cp -r cmd/broker/filter/kodata /var/run/ko

FROM registry.access.redhat.com/ubi8/ubi-minimal
USER 65532

COPY --from=builder /usr/bin/main /usr/bin/main
COPY --from=builder /var/run/ko /var/run/ko
ENTRYPOINT ["/usr/bin/main"]
