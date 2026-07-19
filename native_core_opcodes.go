package ferricstore

// FerricStore 0.8.0 typed core-data opcodes. The native framing protocol is v1.
const (
	nativeOpCAS                  = 0x0106
	nativeOpLock                 = 0x0107
	nativeOpUnlock               = 0x0108
	nativeOpExtend               = 0x0109
	nativeOpRateLimitAdd         = 0x010A
	nativeOpFetchOrCompute       = 0x010B
	nativeOpFetchOrComputeResult = 0x010C
	nativeOpFetchOrComputeError  = 0x010D
	nativeOpHSet                 = 0x0110
	nativeOpHGet                 = 0x0111
	nativeOpHMGet                = 0x0112
	nativeOpHGetAll              = 0x0113
	nativeOpLPush                = 0x0120
	nativeOpRPush                = 0x0121
	nativeOpLPop                 = 0x0122
	nativeOpRPop                 = 0x0123
	nativeOpLRange               = 0x0124
	nativeOpSAdd                 = 0x0130
	nativeOpSRem                 = 0x0131
	nativeOpSMembers             = 0x0132
	nativeOpSIsMember            = 0x0133
	nativeOpZAdd                 = 0x0140
	nativeOpZRem                 = 0x0141
	nativeOpZRange               = 0x0142
	nativeOpZScore               = 0x0143
)
