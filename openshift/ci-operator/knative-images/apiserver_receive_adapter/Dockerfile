# Do not edit! This file was generated via Makefile
FROM registry.ci.openshift.org/openshift/release:golang-1.18 as builder

COPY . .
RUN make install test-install

FROM openshift/origin-base
USER 65532

COPY ./cmd/apiserver_receive_adapter/kodata /var/run/ko
COPY --from=builder /go/bin/apiserver_receive_adapter /usr/bin/apiserver_receive_adapter
ENTRYPOINT ["/usr/bin/apiserver_receive_adapter"]
