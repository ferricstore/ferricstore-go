# Release

Go modules are released by pushing a semver tag.

## Checklist

1. Update `CHANGELOG.md`.
2. Run:

   ```bash
   mise exec -- gofmt -w .
   mise exec -- go mod tidy
   mise exec -- go test ./...
   mise exec -- go test -race ./...
   mise exec -- go list ./...
   ```

3. Commit the release changes.
4. Tag:

   ```bash
   git tag v0.1.0
   git push origin main --tags
   ```

5. Verify the module is available:

   ```bash
   GOPROXY=https://proxy.golang.org go list -m github.com/ferricstore/ferricstore-go@v0.1.0
   ```

The GitHub release workflow creates release notes for pushed `v*` tags.
