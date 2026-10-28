<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

User-visible changes are recorded here. The project follows Semantic Versioning; the 0.x API may change between minor versions. New release sections use ISO 8601 dates.

## Unreleased

### Added

- Complete Simplified Chinese README with relative language-switch links and inclusion in source distributions.
- Operational Redis task/image ingestion, branch dispatch, finding aggregation, normal/health publication and explicit claim recovery.
- Generic strict inspection geometry/measurement/result models without detection algorithms.
- Background inspection coordination and HTTP start/stop/configuration/version/status controls.
- Concrete plugin parameter schemas and optional atomic configuration persistence.
- Real Redis integration tests and a pinned Redis CI service.
- Configurable output database for normal/health results, with documented at-least-once cross-database delivery.
- Loopback browser demo with real Redis batch submission, live worker status and published inspection/health results.
- Direct root main.py service startup and a separate simulate_client.py image producer/consumer for IDE debugging.

- Strict Pydantic task, configuration, image, and result contracts.
- Spawn-based sequential stages, configurable workers, bounded queues, and bounded unclaimed results.
- Shared processor lifecycle, task-error isolation, startup supervision, and shutdown cleanup.
- Explicit CVO registration with typed parameters and duck-typed operators.
- Pure-image transport, metadata-only examples, JSONL and optional HTTP adapters.
- Apache-2.0 licensing and English contributor, conduct, security, and development policies.
- Linux/Windows CI configuration for Python 3.11–3.14.

The package metadata currently uses 0.1.0 for local build verification. No public release has been published.

### Changed

- Separate application services and HTTP controls from `transports/redis`; shared task and result messages now live in `messages.py`.
- Application classes include `TaskService`, `ServiceController`, `ServiceConfig`, `ServiceStatus` and `RunReport`; wire messages use `TaskMessage`, `ResultMessage` and `ImageReference`. See the development guide for the package layout.

### Fixed

- Log the downward receipt only after duplicate admission, keeping rejected duplicate envelopes from adding unmatched per-image INFO records.
- Emit only the downward receipt and upward publication lines per image at INFO, with intermediate processing details at DEBUG.
- Use the timestamp/module/line/level layout, readable lifecycle messages, console colors and daily log-file rotation.
- Show Redis task lifecycle events with task identifiers, process/thread context and branch publication outcomes in the service console.
- Preserve replacement task reservations when an older same-identifier submission fails.
- Report teardown exceptions and worker crashes during shutdown.
