# Open Source Removal Inventory

This inventory records internal-only material removed during the standalone repository sanitization. It intentionally omits operational values, infrastructure identifiers, incident details, and credentials.

| Category | Removed | Reason |
| --- | ---: | --- |
| Datacenter value inventories | 12 | Contained private deployment and backup configuration. |
| Installation value inventories | 137 | Contained internal service, cluster, environment, and deployment topology. |
| Bazel BUILD files | 24 | Depended on private monorepo build rules and targets. |
| Bazel and CNAB helpers | 3 | Depended on private build, deployment, registry, and workflow infrastructure. |
| Service catalog metadata | 2 | Contained internal ownership, contact, dashboard, and deployment metadata. |
| Deployment automation metadata | 2 | Contained private rollout, placement, and release automation. |
| Internal Helm lint values | 1 | Modeled private CNAB and platform-specific chart inputs. |
| Internal incident report | 1 | Contained production incident and infrastructure details. |
| Internal CODEOWNERS entry | 1 | Named an internal repository ownership group. |
| **Total** | **183** | |

Compatibility identifiers such as the `cache.datadoghq.com/v1` API group/version and the existing leader-election ID were retained because they are public compatibility contracts, not private operational material.
