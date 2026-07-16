# Contributing

## Requirements

- Go 1.24 or newer; the security-patched dev toolchain is pinned by `mise.toml`
- Docker for integration/dev server work

```bash
brew install mise
mise trust ./mise.toml
mise exec -- go test ./...
```

## Development

Start a local FerricStore server:

```bash
docker compose up -d ferricstore
```

Run unit tests:

```bash
mise exec -- go test ./...
```

Run examples:

```bash
mise exec -- go run ./examples/durable_queue
mise exec -- go run ./examples/state_workflow
```

Run formatting and the local correctness checks before sending a change:

```bash
mise exec -- gofmt -w .
mise exec -- go mod tidy
mise exec -- go vet ./...
mise exec -- go test ./...
mise exec -- go test -race ./...
./scripts/api-compat.sh
./scripts/fuzz-smoke.sh
./scripts/stress.sh
./scripts/integration-docker.sh
./scripts/integration-security-docker.sh
./scripts/integration-cluster-docker.sh
```

The Docker suites respectively cover the released server command surface, protected mode/ACL/TLS/mTLS, and real multi-node routing/failover. `scripts/api-compat.sh` compares exported declarations with the tag stored in `.api-baseline`; update that file only after publishing the new compatibility baseline.

## API Guidelines

- Prefer typed helpers for stable FerricStore/FerricFlow commands.
- Keep `Client.Command` as the escape hatch for commands that do not have polished typed helpers yet.
- Do not add another persistence layer; FerricStore is the durable backend.
- Keep worker helpers explicit about state transitions and error policy.
- Add a failing test first for every bug fix, including command shape, response decoding, cancellation, and ownership behavior where relevant.
- Keep production Go files at or below the repository's 525-line architecture ceiling by splitting responsibilities.

## Compatibility

This module follows semantic versioning once tagged. Breaking changes should wait for a major version bump unless the API has not been released yet.
