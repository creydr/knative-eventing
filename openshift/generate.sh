#!/usr/bin/env bash

set -euo pipefail

repo_root_dir=$(dirname "$(realpath "${BASH_SOURCE[0]}")")/..

generate_bin=$(mktemp -q /tmp/generate-XXXXXXXX)
GO111MODULE=off go build -o "$generate_bin" github.com/openshift-knative/hack/cmd/generate

$generate_bin \
  --root-dir "${repo_root_dir}" \
  --generators dockerfile \
  --dockerfile-image-builder-fmt "registry.ci.openshift.org/openshift/release:rhel-8-release-golang-%s-openshift-4.16"

