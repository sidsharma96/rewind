package httpapi

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestConnectionLimitClosesExcessAndReleasesExactlyOnce(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limited, err := LimitConnections(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	dial := func() net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", base.Addr().String(), 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	first := dial()
	defer first.Close()
	owned, err := limited.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer owned.Close()
	accepted := make(chan net.Conn, 1)
	failed := make(chan error, 1)
	go func() {
		c, err := limited.Accept()
		if err != nil {
			failed <- err
			return
		}
		accepted <- c
	}()
	excess := dial()
	defer excess.Close()
	_ = excess.SetReadDeadline(time.Now().Add(3 * time.Second))
	var b [1]byte
	if n, err := excess.Read(b[:]); n != 0 || err != io.EOF {
		t.Fatal("excess connection was retained", n, err)
	}
	_ = owned.Close()
	_ = owned.Close()
	third := dial()
	defer third.Close()
	select {
	case c := <-accepted:
		_ = c.Close()
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("closed connection did not release capacity")
	}
}
