package ferricstore

import (
	"bufio"
	"net"
	"testing"
)

func TestNativeConnectionGenerationSkipsZeroOnWrap(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()

	exec := NewNativeExecutor("unused", WithNativeHeartbeat(0, 0))
	exec.connectionGeneration = ^uint64(0)
	contract, err := parseNativeHelloContract(nativeHelloForTest(), nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	exec.mu.Lock()
	exec.installNativeConnectionLocked(&nativeConnectedTransport{
		conn:          clientConn,
		reader:        bufio.NewReader(clientConn),
		writer:        bufio.NewWriter(clientConn),
		helloResponse: nativeHelloForTest(),
		contract:      contract,
	})
	generation := exec.connectionGeneration
	exec.mu.Unlock()
	defer func() { _ = exec.Close() }()

	if generation == 0 {
		t.Fatal("native connection generation wrapped to the reserved zero value")
	}
}
