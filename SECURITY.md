# Security policy

## Reporting a vulnerability

Do not open a public issue for suspected vulnerabilities. Report them through Datadog's security vulnerability intake at https://www.datadoghq.com/security/ and include the repository name, affected version or commit, reproduction details, and potential impact. Do not include live credentials or customer data.

Datadog will acknowledge and triage reports through its established security process. Public disclosure should be coordinated with the security team.

## Supported versions

This project is beta. Security fixes are provided only for the latest published release when releases exist; Phase 1 does not publish a release. Redis 8.6 and Kubernetes 1.33 are the only initial compatibility targets.

## Security-sensitive design

The toolbox, Redis authentication, object-store access, backup/restore artifacts, RBAC, and executable invocation are security-sensitive. Changes to these areas require an assigned Security reviewer. Live secrets must not appear in command arguments, URLs, logs, errors, examples, or test fixtures.

Before a public release, complete the repository's release gates: organizational approval, Apache-2.0 confirmation, Security review, full secret/private-reference scanning, dependency license review, and clean KIND installation verification.
