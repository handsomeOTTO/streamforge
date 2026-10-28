<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing to StreamForge

Thank you for helping improve the project. Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Before changing code

Search existing issues and describe the problem, expected outcome, and relevant constraints. Discuss substantial API changes before implementation. Keep changes focused; avoid unrelated refactors. Use synthetic examples without customer data, internal addresses, credentials, model weights, or engineering files.

Code comments, docstrings, primary documentation, issue templates, and review explanations must be in English. The designated Simplified Chinese README translation, `README.zh-CN.md`, and its language-switch link in `README.md` are explicit localization exceptions. Keep translated commands, configuration fields and examples synchronized with the English README. Use clear names and document observable behavior rather than narrating each line.

## Development workflow

1. Create a branch from the current main branch.
2. Install `.[api,dev]` in a virtual environment.
3. Add a failing regression or behavior test, implement the smallest change, then run all checks in [the development guide](docs/development.md).
4. Update documentation and the Unreleased changelog when behavior changes.
5. Open a pull request explaining the problem, resulting behavior, tests, and compatibility impact. Use the PR template.

Use meaningful commit subjects, preferably `fix:`, `feat:`, `docs:`, `test:`, or `chore:`. Do not combine formatting changes with unrelated behavioral changes. CI must pass before merge, and a maintainer should review each change. Maintainers must not approve their own substantive changes without another reviewer when one is available.

## Contribution rights

Only contribute work you are entitled to submit under Apache-2.0. Keep third-party attribution and license notices intact. Disclose copied or adapted material and its source/license; architectural inspiration does not authorize source redistribution. Contributions are accepted under the project's license. Do not impersonate another contributor or sign declarations on their behalf.

Disclose material AI assistance in the PR and verify its output yourself. Contributors remain responsible for tests, provenance, licensing, and sensitive-data removal. A separate contributor agreement is not required.

## Compatibility and review

The supported Python range starts at 3.11. Test multiprocessing changes with real spawn workers and verify cleanup, backpressure, and errors. Keep processing messages and configuration strictly typed. Operator-private data must not become a required framework-wide layer schema.

Maintainers may request changes, explain a rejection, or ask that a large proposal be split. New contributors can start with documentation or a focused reproducible bug. Use the security process for vulnerabilities rather than a public issue containing exploit details.
