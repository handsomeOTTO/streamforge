<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security policy

## Supported versions

During the 0.1 series, security fixes target the latest 0.1.x release. Older releases are unsupported. There is no service-level response guarantee for this community project.

## Reporting a vulnerability

Use the repository host's private vulnerability reporting feature when enabled. Otherwise contact a maintainer through a private contact published on their profile. If no private route is available, open a minimal issue requesting one; do not include exploit steps, secrets, personal information, or affected customer data. Maintainers must enable private reporting or publish an operational contact before a public release.

Include the affected version, environment, impact, a minimal synthetic reproduction, and any suggested mitigation. Coordinate disclosure with maintainers and allow time for investigation. Do not test against someone else's systems without authorization.

## Trust boundaries

- Processor/operator code is trusted application code with the process owner's permissions. Spawn is isolation of state, not a security sandbox.
- Plugins are registered explicitly in Python. Requests cannot choose arbitrary import paths or executable code.
- Images are opaque bytes. Base64 validation does not certify their format or safety for downstream decoders.
- The bundled HTTP service binds to loopback by default and provides no authentication, TLS, or global request-rate limit. Operators deploying it across a trust boundary must provide those controls.
- Public errors may contain plugin exception messages. Plugins must avoid including credentials, private file paths, or confidential payloads in exceptions.
- Pending limits bound task counts, not total memory bytes. Size queues and payload limits for the deployment.
- Redis task producers and control clients are trusted deployment participants. Constrain Redis ACLs and allowed key namespaces; an image reference is a request to read a key using the service's configured credentials.
- Recovery can replay a previously started operator. Run it only after the processing-list owner stops and make side effects idempotent.

Do not commit credentials or private data. Dependency changes require review, compatible licenses, and the normal test checks. Security updates should explain affected versions and mitigations without exposing private reports.
