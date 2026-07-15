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
	exec.mu.Lock()
	exec.installNativeConnectionLocked(&nativeConnectedTransport{
		conn:            clientConn,
		reader:          bufio.NewReader(clientConn),
		writer:          bufio.NewWriter(clientConn),
		startupResponse: map[string]any{"ready": true},
	})
	generation := exec.connectionGeneration
	exec.mu.Unlock()
	defer func() { _ = exec.Close() }()

	if generation == 0 {
		t.Fatal("native connection generation wrapped to the reserved zero value")
	}
}
