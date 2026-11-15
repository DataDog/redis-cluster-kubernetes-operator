---
name: bump-kubernetes-dependencies
description: Upgrade k8s.io modules and controller-runtime together, with license inventory and envtest. Use for Kubernetes dependency or Dependabot bumps.
---

# Bump Kubernetes dependencies

1. Pick versions that match: controller-runtime `v0.N` pairs with one
   `k8s.io/*` minor (see the controller-runtime release notes). Bump
   `k8s.io/api`, `apimachinery`, `client-go`, `apiextensions-apiserver` and
   `sigs.k8s.io/controller-runtime` in one change:
   ```sh
   go get k8s.io/api@vX k8s.io/apimachinery@vX k8s.io/client-go@vX \
     k8s.io/apiextensions-apiserver@vX sigs.k8s.io/controller-runtime@vY
   go mod tidy
   ```
2. If the Kubernetes minor changes, update the envtest version
   (`setup-envtest ... use 1.NN.x`) in `AGENTS.md` and tell maintainers that
   `.github/workflows/test.yml`, KIND node images and chart `kubeVersion` need
   the same bump.
3. `make generate-deepcopy`; never regenerate the CRD. `make verify-crd-contract`.
4. Update `LICENSE-3rdparty.csv` for added, removed or relicensed modules;
   `make verify-license-inventory`.
5. Run unit tests with envtest (`AGENTS.md`) and `ci/run-e2e-lane.sh smoke`.
6. PR body: old and new versions, release notes read, behavior changes found.
