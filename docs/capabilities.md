<!-- SPDX-License-Identifier: Apache-2.0 -->

# Capability checklist

This checklist describes implemented behavior and the local evidence for it. It is not an external certification or a guarantee of suitability for every deployment.

| Capability | Implementation and verification |
| --- | --- |
| Strict contracts | StrictModel rejects extra fields and applies strict validation. Task/result envelopes and operator parameters use concrete Pydantic models. |
| Shared processors | BaseProcessor supplies parameter/output validation and process-local lifecycle hooks. |
| Bounded multiprocessing | Every stage uses the shared spawned worker loop, bounded queues and a limit on pending results. |
| Open operator scheduling | Trusted explicit registration, concrete parameter models, ordered enabled calls and optional setup/teardown hooks. |
| Plugin-owned algorithms | The framework supplies no inference engine or weights; operators produce typed findings through the extension interface. |
| End-to-end Redis I/O | Claim task metadata, load the image key, process selected branches, aggregate and publish normal/health results. |
| Recovery | Unconfirmed claims remain available; an explicit stopped-state command restores FIFO input order. |
| Runtime controls | HTTP start/stop, health/version, strict configuration updates, revisions and optional atomic persistence. |
| Application boundaries | Application services own orchestration; the Redis adapter owns database I/O; shared messages define the wire contracts. |
| IDE workflow | Root main.py and an independent simulate_client.py, with named VS Code configurations and subprocess debugging. |
| Task logs | Admitted image tasks emit downward receipt and upward publication INFO records; rejected envelopes have separate diagnostics. |
| Project practices | Apache-2.0/SPDX metadata, contributor/conduct/security policies, dependency inventory, package builds and CI definitions. |

## Operating limits

- Producers must emit the documented generic message schema.
- Empty default findings are not a quality decision.
- Each processing list has one service owner. Recovery requires that owner to stop.
- Delivery is at least once; consumers and side-effecting operators must tolerate replay.
- Processing workers run on one host. General DAG execution, distributed ownership and cross-server transactions are outside the framework.
- Hosted CI definitions require execution on the chosen repository host before a release.

Detailed checks and observed outcomes are in the [verification record](verification.md).
