# Release

Go modules are released by pushing a semver tag.

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
4. Commit the release changes.
5. Tag:

   ```bash
   git tag v0.9.0
   git push origin main --tags
   ```

6. Verify the module is available:

   ```bash
   GOPROXY=https://proxy.golang.org go list -m github.com/ferricstore/ferricstore-go@v0.9.0
   ```

The GitHub release workflow creates release notes for pushed `v*` tags.
It repeats formatting, tidy, dependency verification, vet, unit, race, API compatibility, fuzz, stress/performance, vulnerability, released-server, security, and multi-node integration gates before publishing.

After a release is visible through the Go proxy, update `.api-baseline` on `main` to that tag so the next release is checked against the newest public API.
