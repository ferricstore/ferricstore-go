# Release

Go modules are released by the guarded `workflow_dispatch` release workflow.
The workflow runs every gate before creating the public semver tag.

## Checklist

1. Update `CHANGELOG.md`.
2. Run:

   ```bash
   mise exec -- gofmt -w .
   mise exec -- go mod tidy
   mise exec -- go mod verify
   mise exec -- go vet ./...
   mise exec -- go test ./...
   mise exec -- go test -race ./...
   mise exec -- go list ./...
   ./scripts/api-compat.sh
   ./scripts/fuzz-smoke.sh
   ./scripts/stress.sh
   mise exec -- go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
   ./scripts/integration-docker.sh
   ./scripts/integration-security-docker.sh
   ./scripts/integration-cluster-docker.sh
   ```

3. Confirm `git diff --check` is clean and `go mod tidy` did not change `go.mod` or `go.sum` unexpectedly.
4. Commit and push the release changes to `main`.
5. Check the intended version and target locally:

   ```bash
   VERSION=v0.12.1
   ./scripts/release-preflight.sh "$VERSION" "$(git rev-parse origin/main)"
   ```

6. Dispatch the release from `main` instead of pushing a tag manually:

   ```bash
   gh workflow run release.yml --ref main -f version="$VERSION"
   ```

7. Verify the module is available:

   ```bash
   GOPROXY=https://proxy.golang.org go list -m "github.com/ferricstore/ferricstore-go@${VERSION}"
   ```

The workflow refuses a version that targets a different commit or conflicts
with an immutable Go-proxy version. It rechecks immediately before publishing.
A retry at the same version and commit resumes the release without creating or
pushing a second tag.

The workflow creates release notes after the module tag resolves through the
public Go proxy. It repeats formatting, tidy, dependency verification, vet,
unit, race, API compatibility, fuzz, stress/performance, vulnerability,
released-server, security, and multi-node integration gates before publishing.

After a release is visible through the Go proxy, update `.api-baseline` on `main` to that tag so the next release is checked against the newest public API.
