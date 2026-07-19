package ferricstore

func buildV080AdminNativeCommand(name string, args []any) (nativeCommand, bool, error) {
	opcode, ok := v080AdminOpcode(name)
	if !ok {
		return nativeCommand{}, false, nil
	}
	encodedArgs, err := nativeCommandArgs(args)
	if err != nil {
		return nativeCommand{}, true, err
	}
	payload := map[string]any{"args": encodedArgs}
	if name == "CLUSTER.KEYSLOT" || name == "FERRICSTORE.KEY_INFO" {
		if len(args) != 1 {
			return nativeCommand{}, false, nil
		}
		if !nativeBinaryCandidate(args[0]) {
			return nativeCommand{}, false, nil
		}
		// CLUSTER.KEYSLOT executes args but authorizes/routes by key in the
		// exact v0.8.0 schema. KEY_INFO accepts the same canonical shape.
		payload["key"] = args[0]
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func v080AdminOpcode(name string) (uint16, bool) {
	switch name {
	case "CLUSTER.HEALTH":
		return nativeOpClusterHealth, true
	case "CLUSTER.STATS":
		return nativeOpClusterStats, true
	case "CLUSTER.KEYSLOT":
		return nativeOpClusterKeySlot, true
	case "CLUSTER.SLOTS":
		return nativeOpClusterSlots, true
	case "CLUSTER.STATUS":
		return nativeOpClusterStatus, true
	case "CLUSTER.JOIN":
		return nativeOpClusterJoin, true
	case "CLUSTER.LEAVE":
		return nativeOpClusterLeave, true
	case "CLUSTER.FAILOVER":
		return nativeOpClusterFailover, true
	case "CLUSTER.PROMOTE":
		return nativeOpClusterPromote, true
	case "CLUSTER.DEMOTE":
		return nativeOpClusterDemote, true
	case "CLUSTER.ROLE":
		return nativeOpClusterRole, true
	case "FERRICSTORE.KEY_INFO":
		return nativeOpFerricStoreKeyInfo, true
	case "FERRICSTORE.CONFIG":
		return nativeOpFerricStoreConfig, true
	case "FERRICSTORE.HOTNESS":
		return nativeOpFerricStoreHotness, true
	case "FERRICSTORE.METRICS":
		return nativeOpFerricStoreMetrics, true
	case "FERRICSTORE.BLOBGC":
		return nativeOpFerricStoreBlobGC, true
	default:
		return 0, false
	}
}
