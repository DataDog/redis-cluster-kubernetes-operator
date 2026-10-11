# Contributing

Thank you for helping improve redis-cluster-kubernetes-operator.

## Before opening a change

- Discuss significant behavior or CRD changes in an issue first. The `cache.datadoghq.com/v1` API fields and leader-election identity are compatibility contracts.
- Do not include credentials, private endpoints, internal deployment names, customer data, or generated local configuration.
- Keep optional integrations behind provider interfaces. The default operator must run without external CRDs or private services.
- Add tests for behavior changes and preserve bounded metric labels and typed command execution.

## Local checks

Use Go 1.25 or later, Helm 3, and (for integration work) KIND with Kubernetes 1.33.

```sh
go test ./...
go vet ./...
helm lint .
helm template redis-cluster-kubernetes-operator . --namespace redis-operator
make verify-crd-contract
make verify-license-inventory
```

Changes affecting the CRD should also run `make verify-crd-contract` and a
clean-cluster installation test. Use `make local-up` for an interactive cluster
or `make cleanroom-smoke` for the disposable packaged-operator gate. Backup,
authentication, toolbox, and command-execution changes require security review.

## Pull requests

Keep changes focused, explain compatibility and security impact, and document verification. By contributing, you agree that your contribution is licensed under Apache-2.0.
