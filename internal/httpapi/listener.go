package httpapi

import (
	"net"
	"sync"
)

// LimitConnections bounds accepted HTTP/1 connections, including idle/TLS peers.
// Excess connections close immediately; there is no waiting connection queue in
// Go. The OS listen backlog and transport buffers remain independent.
func LimitConnections(next net.Listener, max int) (net.Listener, error) {
	if next == nil || max <= 0 {
		return nil, ErrProtocol
	}
	return &limitedListener{Listener: next, active: make(chan struct{}, max)}, nil
}

type limitedListener struct {
	net.Listener
	active chan struct{}
}
type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (l *limitedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.active <- struct{}{}:
			return &limitedConn{Conn: conn, release: func() { <-l.active }}, nil
		default:
			_ = conn.Close()
		}
	}
}
func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
