---
name: redis-cluster-health
description: Read-only health check of RedisClusters managed by the Datadog redis-cluster-kubernetes-operator. Use when the user asks whether a cache is healthy, why it is failing, or wants a recurring health report.
---

# Check Redis Cluster health

Read-only. Do not change anything; report what is wrong and the fix.

## 1. Find the clusters

```sh
kubectl get redisclusters --all-namespaces \
  -o custom-columns=NS:.metadata.namespace,NAME:.metadata.name,PRIMARIES:.spec.primaries,REPLICAS:.spec.replicas,ROLLOUT:.status.blueGreen.phase
```

## 2. Check each one

```sh
scripts/verify-cluster.sh --namespace NS --name NAME --timeout 0 --json
```

`--timeout 0` checks once instead of waiting.

## 3. On a failing check

Collect, for the first non-`pass` check only:

| Check | Look at |
| --- | --- |
| `rollout` | `kubectl get rediscluster NAME -n NS -o jsonpath='{.status.blueGreen}'`; a rollout older than `BlueGreenOverallTimeout` (`constants/bluegreen.go`) is stuck |
| `pods` | `kubectl get pods -n NS -o wide` and `kubectl describe pod` for pods not Ready |
| `cluster-info`, `topology` | `redis-cli CLUSTER INFO` and `CLUSTER NODES` on a Ready pod |
| any | operator logs: `kubectl logs -n NS -l app.kubernetes.io/component=operator --since=30m` |

## 4. Report

One line per cluster: `NS/NAME ok` or `NS/NAME <check>: <detail> — <fix>`.
List failing clusters first. Do not run repair commands; hand them to the user.
