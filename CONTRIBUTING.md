# Contributing

## Requirements

- Go 1.24 or newer; the dev toolchain is pinned by `mise.toml`
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

Run formatting before sending a change:

```bash
mise exec -- gofmt -w .
mise exec -- go mod tidy
mise exec -- go test ./...
```

## API Guidelines

- Prefer typed helpers for stable FerricStore/FerricFlow commands.
- Keep `Client.Command` as the escape hatch for commands that do not have polished typed helpers yet.
- Do not add another persistence layer; FerricStore is the durable backend.
- Keep worker helpers explicit about state transitions and error policy.
- Add unit tests for command shape and response decoding when adding a helper.

## Compatibility

This module follows semantic versioning once tagged. Breaking changes should wait for a major version bump unless the API has not been released yet.
