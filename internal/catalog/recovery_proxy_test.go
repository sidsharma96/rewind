package catalog

import (
	"bytes"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This is a bounded plaintext LOCAL test proxy, not a general PostgreSQL proxy.
// It forwards startup/authentication, never logs protocol contents, and drops one
// observed COMMIT response only after the publication barrier arms it.
type recoveryProxy struct {
	listener net.Listener
	address  string
	upstream string
	network  string
	armed    atomic.Bool
	dropped  atomic.Int32
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closed   bool
	permits  chan struct{}
	wg       sync.WaitGroup
}

func newRecoveryProxy(t *testing.T, cfg *pgxpool.Config) *recoveryProxy {
	t.Helper()
	c := cfg.ConnConfig
	local := strings.HasPrefix(c.Host, "/") || c.Host == "localhost" || net.ParseIP(c.Host).IsLoopback()
	if !local || c.TLSConfig != nil || len(c.Fallbacks) != 0 {
		t.Skip("lost-COMMIT response requires an explicitly plaintext loopback/Unix disposable PostgreSQL server; TLS is never downgraded")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("open protocol-proxy listener failed")
	}
	p := &recoveryProxy{listener: listener, address: listener.Addr().String(), network: "tcp",
		upstream: net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port))), conns: make(map[net.Conn]struct{}), permits: make(chan struct{}, 4)}
	if strings.HasPrefix(c.Host, "/") {
		p.network = "unix"
		p.upstream = filepath.Join(c.Host, ".s.PGSQL."+strconv.Itoa(int(c.Port)))
	}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(func() {
		p.mu.Lock()
		p.closed = true
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		_ = p.listener.Close()
		p.wg.Wait()
	})
	return p
}

func (p *recoveryProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		select {
		case p.permits <- struct{}{}: // At most four pairs, including cancellations.
		default:
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = client.Close()
			<-p.permits
			return
		}
		p.conns[client] = struct{}{}
		p.mu.Unlock()
		p.wg.Add(1)
		go p.forward(client)
	}
}

func (p *recoveryProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer func() { <-p.permits }()
	defer func() {
		_ = client.Close()
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
	}()
	server, err := net.DialTimeout(p.network, p.upstream, 3*time.Second)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = server.Close()
		return
	}
	p.conns[server] = struct{}{}
	p.mu.Unlock()
	defer func() {
		_ = server.Close()
		p.mu.Lock()
		delete(p.conns, server)
		p.mu.Unlock()
	}()
	if server.SetDeadline(time.Now().Add(30*time.Second)) != nil || client.SetDeadline(time.Now().Add(30*time.Second)) != nil {
		return
	}
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		_, _ = io.Copy(server, client)
		_ = server.Close() // Also stops Receive if the child has exited.
	}()
	defer func() {
		_ = client.Close()
		_ = server.Close()
		<-copied
	}()
	front := pgproto3.NewFrontend(server, client)
	front.SetMaxBodyLen(1 << 20)
	for {
		message, err := front.Receive()
		if err != nil {
			return
		}
		if complete, ok := message.(*pgproto3.CommandComplete); ok && bytes.Equal(complete.CommandTag, []byte("COMMIT")) && p.armed.CompareAndSwap(true, false) {
			// Do not forward CommandComplete. Require the server's idle boundary
			// as evidence this was a completed commit, not a guessed interruption.
			for i := 0; i < 16; i++ {
				next, err := front.Receive()
				if err != nil {
					return
				}
				if ready, ok := next.(*pgproto3.ReadyForQuery); ok {
					if ready.TxStatus == 'I' {
						p.dropped.Add(1)
					}
					return // Close both connections; pgx receives no commit result.
				}
				if _, ok := next.(*pgproto3.NoticeResponse); !ok {
					return
				}
			}
			return
		}
		frame, err := message.Encode(nil)
		if err != nil {
			return
		}
		if _, err := io.Copy(client, bytes.NewReader(frame)); err != nil {
			return
		}
	}
}
