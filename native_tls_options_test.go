package ferricstore

import "testing"

func TestNewNativeExecutorUsesTLSDefaultPortForBareAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{name: "hostname", addr: "db.example", want: "db.example:6389"},
		{name: "empty", want: "127.0.0.1:6389"},
		{name: "bare IPv6", addr: "2001:db8::1", want: "[2001:db8::1]:6389"},
		{name: "explicit port", addr: "db.example:7443", want: "db.example:7443"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := NewNativeExecutor(test.addr, WithNativeTLS(nil))
			defer func() { _ = exec.Close() }()
			if got := exec.opts.Addr; got != test.want {
				t.Fatalf("native TLS address = %q; want %q", got, test.want)
			}
		})
	}
}

func TestNewNativeExecutorTLSKeepsOptionAddressOverride(t *testing.T) {
	exec := NewNativeExecutor(
		"db.example",
		WithNativeTLS(nil),
		func(options *NativeOptions) { options.Addr = "proxy.example:7443" },
	)
	defer func() { _ = exec.Close() }()
	if got := exec.opts.Addr; got != "proxy.example:7443" {
		t.Fatalf("native option address = %q; want explicit override", got)
	}
}

func TestNewClientNativeTLSUsesTLSDefaultPort(t *testing.T) {
	client := NewClient("db.example", WithNativeOptions(WithNativeTLS(nil)))
	defer func() { _ = client.Close() }()
	native, ok := client.exec.(*NativeExecutor)
	if !ok {
		t.Fatalf("client executor = %T; want *NativeExecutor", client.exec)
	}
	if got := native.opts.Addr; got != "db.example:6389" {
		t.Fatalf("client native TLS address = %q; want %q", got, "db.example:6389")
	}
}

func TestNewClientNativeTLSKeepsExplicitPort(t *testing.T) {
	client := NewClient("db.example:7443", WithNativeOptions(WithNativeTLS(nil)))
	defer func() { _ = client.Close() }()
	native := client.exec.(*NativeExecutor)
	if got := native.opts.Addr; got != "db.example:7443" {
		t.Fatalf("client native TLS address = %q; want explicit port", got)
	}
}

func TestNativeTLSDefaultSurvivesOptionsValueReplacement(t *testing.T) {
	exec := NewNativeExecutor("db.example", func(options *NativeOptions) {
		*options = NativeOptions{Addr: options.Addr, TLS: true}
	})
	defer func() { _ = exec.Close() }()
	if got := exec.opts.Addr; got != "db.example:6389" {
		t.Fatalf("replacement native TLS address = %q; want %q", got, "db.example:6389")
	}
}
