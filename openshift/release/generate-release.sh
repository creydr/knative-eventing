#!/usr/bin/env bash

set -euo pipefail

source $(dirname $0)/resolve.sh

root_dir=$(dirname $0)/../..

release=$(yq r openshift/project.yaml project.tag)
release=${release/knative-/}

echo "Release: $release"

"${root_dir}"/hack/update-codegen.sh
git apply "${root_dir}"/openshift/patches/020-mutemetrics.patch

./openshift/generate.sh

artifacts_dir="openshift/release/artifacts"
rm -rf $artifacts_dir
mkdir -p $artifacts_dir

rm -rf config/channels/in-memory-channel/configmaps/observability.yaml
rm -rf config/channels/in-memory-channel/configmaps/tracing.yaml
rm -rf config/channels/in-memory-channel/100-namespace.yaml

image_prefix="registry.ci.openshift.org/openshift/knative-${release}:knative-eventing-"
tag=""

eventing_core="${artifacts_dir}/eventing-core.yaml"
eventing_crds="${artifacts_dir}/eventing-crds.yaml"
in_memory_channel="${artifacts_dir}/in-memory-channel.yaml"
mt_channel_broker="${artifacts_dir}/mt-channel-broker.yaml"
eventing_post_install="${artifacts_dir}/eventing-post-install.yaml"
eventing_tls_networking="${artifacts_dir}/eventing-tls-networking.yaml"

# Eventing CRDs
resolve_resources config/core/resources "${eventing_crds}" "$image_prefix" "$tag"
# Eventing core
resolve_resources config "${eventing_core}" "$image_prefix" "$tag"
# Eventing post-install
resolve_resources config/post-install "${eventing_post_install}" "$image_prefix" "$tag"
# In memory channel
resolve_resources config/channels/in-memory-channel "${in_memory_channel}" "$image_prefix" "$tag"
resolve_resources config/channels/in-memory-channel/configmaps "${in_memory_channel}" "$image_prefix" "$tag"
resolve_resources config/channels/in-memory-channel/deployments "${in_memory_channel}" "$image_prefix" "$tag"
resolve_resources config/channels/in-memory-channel/resources "${in_memory_channel}" "$image_prefix" "$tag"
resolve_resources config/channels/in-memory-channel/roles "${in_memory_channel}" "$image_prefix" "$tag"
resolve_resources config/channels/in-memory-channel/webhooks "${in_memory_channel}" "$image_prefix" "$tag"
# MT Broker
resolve_resources config/brokers/mt-channel-broker "${mt_channel_broker}" "$image_prefix" "$tag"
# TLS
resolve_resources config/core-tls "${eventing_tls_networking}" "$image_prefix" "$tag"
resolve_resources config/brokers/mt-channel-broker-tls "${eventing_tls_networking}" "$image_prefix" "$tag"
resolve_resources config/channels/in-memory-channel-tls "${eventing_tls_networking}" "$image_prefix" "$tag"
