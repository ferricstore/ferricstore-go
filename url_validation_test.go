package ferricstore

import (
	"testing"
	"time"
)

func TestURLConstructorsRejectInvalidAuthorities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{name: "empty host", url: "ferric://:6388"},
		{name: "zero port", url: "ferric://localhost:0"},
		{name: "port above TCP range", url: "ferric://localhost:65536"},
		{name: "invalid bare port", url: "localhost:not-a-port"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("native", func(t *testing.T) {
				exec, err := NewNativeExecutorFromURL(tt.url)
				if exec != nil {
					_ = exec.Close()
				}
				if err == nil {
					t.Fatalf("NewNativeExecutorFromURL(%q) succeeded", tt.url)
				}
			})

			t.Run("topology", func(t *testing.T) {
				exec, err := NewTopologyNativeExecutor([]string{tt.url})
				if exec != nil {
					_ = exec.Close()
				}
				if err == nil {
					t.Fatalf("NewTopologyNativeExecutor(%q) succeeded", tt.url)
				}
			})
		})
	}
}

func TestNativeExecutorURLRejectsInvalidTimeouts(t *testing.T) {
	t.Parallel()

	for _, rawURL := range []string{
		"ferric://localhost?timeout=-1s",
		"ferric://localhost?timeout=",
		"ferric://localhost?timeout=%zz",
	} {
		t.Run(rawURL, func(t *testing.T) {
			exec, err := NewNativeExecutorFromURL(rawURL)
			if exec != nil {
				_ = exec.Close()
			}
			if err == nil {
				t.Fatalf("NewNativeExecutorFromURL(%q) succeeded", rawURL)
			}
		})
	}
}

func TestNativeTimeoutOptionNormalizesNegativeDuration(t *testing.T) {
	t.Parallel()

	exec := NewNativeExecutor("localhost", WithNativeTimeout(-time.Second))
	t.Cleanup(func() { _ = exec.Close() })

	if exec.opts.Timeout != 0 || exec.opts.Dialer.Timeout != 0 {
		t.Fatalf("negative timeout was not disabled: timeout=%s dialer=%s", exec.opts.Timeout, exec.opts.Dialer.Timeout)
	}
}

func TestNativeURLKeepsDefaultPortProvenanceForTLSOption(t *testing.T) {
	t.Parallel()

	for _, rawURL := range []string{"localhost", "ferric://localhost"} {
		t.Run(rawURL, func(t *testing.T) {
			exec, err := NewNativeExecutorFromURL(rawURL, WithNativeTLS(nil))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = exec.Close() })
			if exec.opts.Addr != "localhost:6389" {
				t.Fatalf("TLS option did not select the TLS default port: %s", exec.opts.Addr)
			}
		})
	}

	exec, err := NewNativeExecutorFromURL("localhost:7000", WithNativeTLS(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Close() })
	if exec.opts.Addr != "localhost:7000" {
		t.Fatalf("TLS option replaced an explicit port: %s", exec.opts.Addr)
	}
}

func TestURLConstructorsRejectIgnoredComponentsAndAmbiguousOptions(t *testing.T) {
	t.Parallel()

	for _, rawURL := range []string{
		"ferric://localhost/database",
		"ferric://localhost#fragment",
		"ferric://localhost?unknown=value",
		"ferric://localhost?timeout=1s&timeout=2s",
	} {
		t.Run(rawURL, func(t *testing.T) {
			if exec, err := NewNativeExecutorFromURL(rawURL); err == nil {
				_ = exec.Close()
				t.Fatalf("native URL constructor accepted %q", rawURL)
			}
			if exec, err := NewTopologyNativeExecutor([]string{rawURL}); err == nil {
				_ = exec.Close()
				t.Fatalf("topology URL constructor accepted %q", rawURL)
			}
		})
	}
}

func TestTopologySeedURLPreservesTimeout(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{"ferric://localhost?timeout=125ms"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Close() })

	adapter, err := exec.adapterForURL(exec.seedURLs[0])
	if err != nil {
		t.Fatal(err)
	}
	if adapter.opts.Timeout != 125*time.Millisecond || adapter.opts.Dialer.Timeout != 125*time.Millisecond {
		t.Fatalf("topology seed timeout = %s/%s, want 125ms", adapter.opts.Timeout, adapter.opts.Dialer.Timeout)
	}
}
