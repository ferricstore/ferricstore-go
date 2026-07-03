# Security

## Supported Versions

Security fixes are provided for the latest released major version.

## Reporting A Vulnerability

Do not open a public issue for a vulnerability report.

Email the maintainers with:

- affected version or commit
- reproduction steps
- impact
- suggested fix, if known

Use the GitHub repository security advisory flow when it is available for this repo.

## Local Development

`docker-compose.yml` sets `FERRICSTORE_PROTECTED_MODE=false` for local development only. Do not use that setting for production deployments without proper network isolation and authentication controls.

Use `ferrics://` and server-side ACL/TLS settings for credentials across a network. Avoid putting production passwords directly in source code; prefer your platform secret manager or environment-derived configuration.
