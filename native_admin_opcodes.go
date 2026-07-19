package ferricstore

// FerricStore 0.8.0 administration opcodes. The native framing protocol is v1.
const (
	nativeOpClusterHealth      = 0x0301
	nativeOpClusterStats       = 0x0302
	nativeOpClusterKeySlot     = 0x0303
	nativeOpClusterSlots       = 0x0304
	nativeOpClusterStatus      = 0x0305
	nativeOpClusterJoin        = 0x0306
	nativeOpClusterLeave       = 0x0307
	nativeOpClusterFailover    = 0x0308
	nativeOpClusterPromote     = 0x0309
	nativeOpClusterDemote      = 0x030A
	nativeOpClusterRole        = 0x030B
	nativeOpFerricStoreKeyInfo = 0x030C
	nativeOpFerricStoreConfig  = 0x030D
	nativeOpFerricStoreHotness = 0x030E
	nativeOpFerricStoreMetrics = 0x030F
	nativeOpFerricStoreBlobGC  = 0x0310
)
