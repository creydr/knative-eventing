# DO NOT EDIT! Generated Dockerfile for test/test_images/request-sender.
FROM registry.ci.openshift.org/openshift/release:golang-1.18 as builder

COPY . .

RUN mkdir -p /var/run/ko && \
    mkdir -p test/test_images/request-sender/kodata && \
    go build -o /usr/bin/main ./test/test_images/request-sender && \
    cp -r test/test_images/request-sender/kodata /var/run/ko

FROM registry.access.redhat.com/ubi8/ubi-minimal
USER 65532

COPY --from=builder /usr/bin/main /usr/bin/main
COPY --from=builder /var/run/ko /var/run/ko
ENTRYPOINT ["/usr/bin/main"]
