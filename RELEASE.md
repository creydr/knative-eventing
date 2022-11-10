# Openshift Knative Eventing Release procedure

The Openshift Knative Eventing release cut is mostly automated and requires only two manual steps for enabling the CI runs on the `openshift/release` repository.

No manual creation of a midstream `release-v1.x` branch is needed. The nightly Jenkins job creates a `release` branch as soon as the upstream has created a new release tag. The code for this script is located in this [script](./openshift/release/mirror-upstream-branches.sh) and mirrors the upstream release tag to our midstream `release` branches.

## Enable CI for the release branch

* Create a fork and clone of https://github.com/openshift/release into your `$GOPATH`
* On your `openshift/knative-eventing` root folder checkout the new `release-vX.Y` branch and run:

```bash
# Invoke CI config generation, and mirroring images

make update-ci
```

The above `make update-ci` adds new CI configuration to the `openshift/release` repository and afterwards shows which new files were added, like below:

```bash
make[1]: Leaving directory '/home/matzew/go/src/github.com/openshift/release'
┌────────────────────────────────────────────────────────────┐
│ Summary...                                                 │
└────────────────────────────────────────────────────────────┘
│─── New files in /home/matzew/go/src/github.com/openshift/release
ci-operator/config/openshift/knative-eventing/openshift-knative-eventing-release-v1.4__410.yaml
ci-operator/config/openshift/knative-eventing/openshift-knative-eventing-release-v1.4__48.yaml
ci-operator/config/openshift/knative-eventing/openshift-knative-eventing-release-v1.4__49.yaml
ci-operator/jobs/openshift/knative-eventing/openshift-knative-eventing-release-v1.4-periodics.yaml
ci-operator/jobs/openshift/knative-eventing/openshift-knative-eventing-release-v1.4-postsubmits.yaml
ci-operator/jobs/openshift/knative-eventing/openshift-knative-eventing-release-v1.4-presubmits.yaml
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Commit changes to /home/matzew/go/src/github.com/openshift/release and create a PR                                     │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
➜  eventing git:(release-v1.4)
```

As stated by the `make` target, these changes need to be PR'd against that repository. Once the PR is merged, the CI jobs for the new `release-vX.Y` repo is done.

### Serverless Operator

_Making use of the midstream release on the serverless operator is discussed on its own release manual..._
