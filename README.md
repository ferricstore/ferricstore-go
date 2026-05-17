# FerricStore Go SDK

Small Go SDK for FerricStore Flow commands plus DBOS-style throughput benchmark.

## Install

```bash
go get github.com/ferricstore/ferricstore-go
```

## Basic Usage

```go
ctx := context.Background()
client := ferricstore.NewClient("127.0.0.1:6379")

_ = client.Create(ctx, ferricstore.CreateOptions{
	ID:           "flow-1",
	Type:         "agent",
	State:        "queued",
	PartitionKey: "tenant-a:flow-1",
	Payload:      []byte("payload"),
	ReturnRecord: false,
})

jobs, _ := client.ClaimDue(ctx, ferricstore.ClaimDueOptions{
	Type:         "agent",
	State:        "queued",
	Worker:       "worker-1",
	PartitionKey: "tenant-a:flow-1",
	Limit:        100,
})
```

## Benchmark

```bash
go run ./cmd/dbos-style-benchmark \
  --mode queued \
  --transport pipeline \
  --flows 10000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 100
```

`pipeline` uses normal SDK calls over a buffered Redis executor. `many` uses
`FLOW.CREATE_MANY` and `FLOW.COMPLETE_MANY`.
