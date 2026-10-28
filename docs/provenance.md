<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source and dependency provenance

## Project source

This repository contains the StreamForge framework, examples, tests and documentation. No third-party implementation is vendored. The one-pixel PNG fixture is generated for tests and demonstrations. Model weights and detection algorithms are not included.

Maintainers must verify distribution rights and retain applicable notices before publishing contributions or releases. Development uses AI assistance; maintainers review and validate contributions before publication. Source review and automated checks do not establish copyright ownership or replace legal review.

## Licensing

Project code, examples, tests, and original documentation are offered under Apache-2.0, as recorded in LICENSE, NOTICE, SPDX source headers, and package metadata. See the [official license text](https://www.apache.org/licenses/LICENSE-2.0) for terms. Contributor submissions must be authorized for that license; the project license cannot override rights in someone else's material.

No third-party implementation is vendored. Dependencies are installed separately and retain their own licenses and notices. The following table covers direct dependencies, not an assertion about every possible transitive version:

| Dependency | Purpose | Upstream license |
| --- | --- | --- |
| Pydantic | Core validation | [MIT](https://github.com/pydantic/pydantic/blob/main/LICENSE) |
| FastAPI | Optional HTTP adapter | [MIT](https://github.com/fastapi/fastapi/blob/master/LICENSE) |
| Uvicorn | Optional HTTP server | [BSD-3-Clause](https://github.com/encode/uvicorn/blob/main/LICENSE.md) |
| redis-py | Optional Redis transport | [MIT](https://github.com/redis/redis-py/blob/master/LICENSE) |
| pytest | Development tests | [MIT](https://github.com/pytest-dev/pytest/blob/main/LICENSE) |
| HTTPX | HTTP testing | [BSD-3-Clause](https://github.com/encode/httpx/blob/master/LICENSE.md) |
| HTTPX2 | Current Starlette testing | [BSD-3-Clause](https://github.com/pydantic/httpx2/blob/main/LICENSE.md) |
| build | Development packaging | [MIT](https://github.com/pypa/build/blob/main/LICENSE) |
| Ruff | Development checks | [MIT](https://github.com/astral-sh/ruff/blob/main/LICENSE) |
| setuptools | Build backend | [MIT](https://github.com/pypa/setuptools/blob/main/LICENSE) |

For a release, record resolved dependencies using `python -m pip inspect`, check their installed license metadata and upstream notices, and retain any required notices when redistributing dependencies. Do not assume a permissive top-level dependency makes every optional/transitive dependency identical. Record actual checks in the verification report.

The checked local environment is recorded in [dependency-inventory.json](dependency-inventory.json), including transitive dependencies and tools. It includes Certifi under MPL-2.0, Typing Extensions under PSF-2.0, and other dependencies with their own expressions. Those packages are installed separately and are not embedded in the project wheel.
