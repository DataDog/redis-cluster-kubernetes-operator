# Agent guide

To install the operator or create a Redis Cluster, follow
[skills/redis-cluster-setup/SKILL.md](skills/redis-cluster-setup/SKILL.md). To
check health, follow [skills/redis-cluster-health/SKILL.md](skills/redis-cluster-health/SKILL.md).
[llms.txt](llms.txt) indexes the docs.

`scripts/preflight.sh` and `scripts/verify-cluster.sh` print `--json` with a
`fix` for every failing check; act on those instead of guessing.

Do not delete RedisClusters, PVCs, StatefulSets or namespaces, and do not edit
`status`.
