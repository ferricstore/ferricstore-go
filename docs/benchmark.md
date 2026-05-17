# DBOS-Style Benchmark

Go version of the Python benchmark:

```bash
$(mise which go) run ./cmd/dbos-style-benchmark \
  --mode queued \
  --transport pipeline \
  --flows 10000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 100
```

Modes:

* `--transport pipeline`: normal SDK calls over a buffered Redis executor. Mixed
  commands before flush are sent in one Redis pipeline.
* `--transport many`: `FLOW.CREATE_MANY` and `FLOW.COMPLETE_MANY`.

The queued benchmark creates and drains live, like the current Python benchmark.
Use it to compare client/runtime overhead between Python and Go against the same
FerricStore server.
