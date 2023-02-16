#!/usr/bin/env bash

# Updates the image resolver file (usually image.yaml) with the actuall image
# names (e.g. in CI or for a release)
function update_image_resolver_file() {
  if [ "${BASH_VERSINFO:-0}" -lt 4 ]; then 
    echo "Bash version >= 4 required"
    exit 1
  fi

  echo $@

  local image_resolver_file=$1
  local release=${2:"ci"}
  declare -A images

  echo "Updating image resolver file ${image_resolver_file}"

  if [ "$release" == "ci" ]; then
    images[knative.dev/eventing/test/test_images/print]=${KNATIVE_EVENTING_TEST_PRINT}
    images[knative.dev/eventing/test/test_images/heartbeats]=${KNATIVE_EVENTING_TEST_HEARTBEATS}
    images[knative.dev/reconciler-test/cmd/eventshub]=${KNATIVE_EVENTING_TEST_EVENTSHUB}
  else
    image_prefix="registry.ci.openshift.org/openshift/knative-${release}:knative-eventing"

    images[knative.dev/eventing/test/test_images/print]="${image_prefix}-print"
    images[knative.dev/eventing/test/test_images/heartbeats]="${image_prefix}-heartbeats"
    images[knative.dev/reconciler-test/cmd/eventshub]="${image_prefix}-eventshub"
  fi

  for key in "${!images[@]}"; do
    image=${images[$key]}

    echo "Replacing ${key} image"
    sed -i "s|^${key}: .*|${key}: ${image}|g" "${image_resolver_file}"
  done
}
