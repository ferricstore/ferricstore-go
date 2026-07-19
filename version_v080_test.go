package ferricstore

import (
	"os"
	"strings"
	"testing"
)

func TestV080PackageAndServerContractVersions(t *testing.T) {
	if SDKVersion != "0.8.2" {
		t.Fatalf("SDKVersion = %q", SDKVersion)
	}
	if MinimumServerVersion != "0.8.0" {
		t.Fatalf("MinimumServerVersion = %q", MinimumServerVersion)
	}
	if NativeProtocolVersion != 1 || nativeRequestVersion != 1 {
		t.Fatalf("native protocol versions = exported:%d wire:%d", NativeProtocolVersion, nativeRequestVersion)
	}
}

func TestV080ChangelogHasReleaseHeading(t *testing.T) {
	contents, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "## 0.8.2 - ") {
		t.Fatal("CHANGELOG.md does not identify the 0.8.2 release")
	}
}

func TestV080ReleaseGuideUsesCurrentTag(t *testing.T) {
	contents, err := os.ReadFile("RELEASE.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	if !strings.Contains(text, "git tag v0.8.2") ||
		!strings.Contains(text, "ferricstore-go@v0.8.2") {
		t.Fatal("RELEASE.md does not use the v0.8.2 tag")
	}
	if strings.Contains(text, "v0.1.0") {
		t.Fatal("RELEASE.md still contains the stale v0.1.0 tag")
	}
}

func TestV080KeepsNativeWireProtocolV1Constants(t *testing.T) {
	if nativeMagic != "FSNP" || nativeHeaderLen != 24 ||
		nativeRequestVersion != 0x01 || nativeResponseVersion != 0x81 {
		t.Fatalf(
			"native wire = magic:%q header:%d request:%#x response:%#x",
			nativeMagic, nativeHeaderLen, nativeRequestVersion, nativeResponseVersion,
		)
	}
	wantOpcodes := map[string]struct{ got, want uint16 }{
		"HELLO":            {nativeOpHello, 0x0001},
		"PIPELINE":         {nativeOpPipeline, 0x000E},
		"GET":              {nativeOpGet, 0x0101},
		"MGET":             {nativeOpMGet, 0x0104},
		"FLOW.VALUE.PUT":   {nativeOpFlowValuePut, 0x020B},
		"FLOW.VALUE.MGET":  {nativeOpFlowValueMGet, 0x020C},
		"FLOW.SIGNAL":      {nativeOpFlowSignal, 0x020D},
		"FLOW.CREATE_MANY": {nativeOpFlowCreateMany, 0x020F},
	}
	for name, opcode := range wantOpcodes {
		if opcode.got != opcode.want {
			t.Errorf("%s opcode = %#x, want wire-v1 opcode %#x", name, opcode.got, opcode.want)
		}
	}
}
