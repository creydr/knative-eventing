#!/bin/bash

set -e

function generate_dockefiles() {
  local target_dir=$1; shift
  # Remove old images and re-generate, avoid stale images hanging around.
  for img in $@; do
    local image_base=$(basename $img)
    local kodata_path="$img/kodata"
    mkdir -p $target_dir/$image_base
    if [ -d "$kodata_path" ]
    then
        bin=$image_base kodata_path=$kodata_path envsubst < openshift/ci-operator/Dockerfile_with_kodata.in > $target_dir/$image_base/Dockerfile
    else
        bin=$image_base envsubst < openshift/ci-operator/Dockerfile.in > $target_dir/$image_base/Dockerfile
    fi
  done
}

generate_dockefiles $@

# shellcheck disable=SC2002
go_version=$(grep "^go.*" "go.mod" | awk '{print $2}')

sed -i "s|registry.ci.openshift.org/openshift/release:golang.*|registry.ci.openshift.org/openshift/release:golang-${go_version}|g" openshift/ci-operator/build-image/Dockerfile
