<!-- SPDX-License-Identifier: Apache-2.0 -->

# Verification record

Local verification: 2026-10-28, Asia/Shanghai. Platform: Windows 11, CPython 3.14.0.

## Automated checks

| Check | Observed outcome |
| --- | --- |
| Full pytest suite with isolated real Redis | 96 passed, no skips or warnings |
| Ruff lint and formatting | Passed |
| Compileall for source and examples | Passed |
| Repository language, SPDX and metadata checks | Passed |
| Source and wheel builds | Passed |
| Distribution contents | LICENSE/NOTICE present; environments, private settings, runtime logs and output images excluded |
| README translations | Matching executable examples, reciprocal language navigation and valid local links |

The local environment includes Pydantic 2.14.1, FastAPI 0.162.0, Uvicorn 0.54.0, redis-py 7.4.1, pytest 9.1.1 and Ruff 0.17.2. Resolved dependency metadata is recorded in [dependency-inventory.json](dependency-inventory.json). Tests requiring Redis are enabled for the recorded full run.

The Redis integration fixture starts a temporary Redis 8.10.2 test server on a random loopback port, uses unique owned key prefixes and terminates its own process afterward. It never runs FLUSHDB. No Redis server executable is included in the distributions.

## Runtime coverage

Real spawned processes exercise multiple stages and workers, a continuous image stream, strict nested input/output validation, bounded queues, capacity reservations, duplicate identifiers, initialization failure, worker exits, task failure isolation, draining shutdown and forced shutdown. Late-result tests verify dispatch identity isolation when a task identifier is reused.

CVO coverage includes ordered duck-typed operators, concrete parameter schemas, unknown registrations, private intermediate data, optional resource hooks, invalid output and an empty byte-preserving chain. The SDK example processes six synthetic PNG frames with two workers.

Redis tests cover metadata claims, separate image lookup, raw/Base64 values, targeted branches, background dispatch without result retrieval, deterministic aggregation, normal/health publication, rejected envelopes, missing images, timeouts, queue saturation, explicit recovery and interrupted acknowledgement. Both same-database atomic publication and cross-database at-least-once delivery are exercised.

HTTP tests cover health/version, start/stop, stopped-state configuration changes, revisions, persistence and concurrent status reads. The direct IDE integration starts main.py separately from simulate_client.py and verifies image/result persistence, preservation of unrelated messages and source-image retention. A client timeout leaves its queued task and image available.

Logging coverage verifies two INFO records per admitted image task, downward/upward arrows, matching task/image identifiers, elapsed time, process/thread context, Unicode output with an initially ASCII stream and a plain UTF-8 rotating file. A duplicate in-flight envelope is rejected without adding an unmatched receipt.

## Distribution and review

The package includes its own framework, examples, tests and documentation. Dependencies are installed separately. English code comments and primary developer documentation use SPDX identifiers; the designated Chinese README is a localization exception. Source review and automated checks do not establish ownership of contributions or replace a maintainer's rights review.

The [workflow guide](workflow.md), [capability checklist](capabilities.md) and [architecture diagrams](architecture.md) describe implemented responsibilities and operating boundaries.

## Checks requiring a host

Linux/Windows CI for Python 3.11–3.14 and the Linux Redis integration job are configured but have not executed remotely. There is no claim of a passing hosted matrix or external certification. No remote deployment, tag or public package release was created by local checks.

Before publication, maintainers must configure the repository identity and a working private security-reporting channel, run hosted CI, review distribution contents and authorize the release.
