package ferricstore

type nativeReplayPolicy uint8

const (
	nativeReplayDefault nativeReplayPolicy = iota
	nativeReplayNever
)
