# API Docs

Go API docs are generated from exported package comments and can be viewed locally:

```bash
mise exec -- go doc github.com/ferricstore/ferricstore-go
mise exec -- go doc github.com/ferricstore/ferricstore-go.Client
mise exec -- go doc github.com/ferricstore/ferricstore-go.Workflow
```

After the first tagged release, public docs are available through pkg.go.dev:

```text
https://pkg.go.dev/github.com/ferricstore/ferricstore-go
```

Keep exported types and methods named clearly. Add comments when a type is not self-explanatory or when the command behavior is easy to misuse.
