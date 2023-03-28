# DO NOT EDIT! Generated Dockerfile for vendor/knative.dev/pkg/apiextensions/storageversion/cmd/migrate.
FROM registry.ci.openshift.org/openshift/release:golang-1.18 as builder

COPY . .

RUN mkdir -p /var/run/ko && \
    mkdir -p vendor/knative.dev/pkg/apiextensions/storageversion/cmd/migrate/kodata && \
    go build -o /usr/bin/main ./vendor/knative.dev/pkg/apiextensions/storageversion/cmd/migrate && \
    cp -r vendor/knative.dev/pkg/apiextensions/storageversion/cmd/migrate/kodata /var/run/ko

FROM registry.access.redhat.com/ubi8/ubi-minimal
USER 65532

COPY --from=builder /usr/bin/main /usr/bin/main
COPY --from=builder /var/run/ko /var/run/ko
ENTRYPOINT ["/usr/bin/main"]
