---
name: release-checklist
description: Checklist for cutting an operator release (chart version, image, CRD, notes). Use when preparing or reviewing a release.
---

# Release checklist

Confirm each item and report it; stop on the first failure.

1. `main` is green: latest `Test` run on `main` and the last nightly lanes.
2. `Chart.yaml` `version` and `appVersion` bumped (SemVer; breaking values or
   CRD changes need a major or documented migration).
3. `make verify-crd-contract` passes. If the CRD changed on purpose, the
   pinned sha in `Makefile` changed in a reviewed PR and the release notes say
   how to upgrade the cluster-wide CRD.
4. `make verify-license-inventory`, `helm lint . --strict --kube-version 1.33.0`.
5. `ci/run-cleanroom.sh kind smoke` passes on the release commit.
6. Release notes: user-visible changes, upgrade steps, known issues, and the
   supported Redis and Kubernetes versions.
7. Tag and publish only through the repository's release workflow, by a
   maintainer. Agents prepare the checklist and notes; they do not push tags
   or run the EKS qualification workflow.
