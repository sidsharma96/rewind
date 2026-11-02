package catalog

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
)

type recoveryTraffic struct {
	GET, PUT          int
	Request, Response int64
}

func (a recoveryTraffic) minus(b recoveryTraffic) recoveryTraffic {
	return recoveryTraffic{a.GET - b.GET, a.PUT - b.PUT, a.Request - b.Request, a.Response - b.Response}
}

// Counts actual HTTP body reads/writes, excluding headers, TLS and SDK overhead.
// Snapshots are taken only while all workers are stopped or at an I/O-free barrier.
type recoveryObjects struct {
	peer *publicationObjects
	mu   sync.Mutex
	work sync.WaitGroup
	data recoveryTraffic
	lost bool
	drop bool
}

type recoveryRequestBody struct {
	io.ReadCloser
	owner *recoveryObjects
}

func (b recoveryRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.owner.mu.Lock()
	b.owner.data.Request += int64(n)
	b.owner.mu.Unlock()
	return n, err
}

func (p *recoveryObjects) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.work.Add(1)
	defer p.work.Done()
	p.mu.Lock()
	if r.Method == http.MethodGet {
		p.data.GET++
	}
	if r.Method == http.MethodPut {
		p.data.PUT++
	}
	p.mu.Unlock()
	r.Body = recoveryRequestBody{ReadCloser: r.Body, owner: p}
	defer r.Body.Close() // Hijacking transfers connection/body cleanup to this peer.
	response := httptest.NewRecorder()
	p.peer.ServeHTTP(response, r)
	p.mu.Lock()
	drop := p.drop && !p.lost && r.Method == http.MethodPut && response.Code == http.StatusOK
	if drop {
		p.drop = false
	}
	p.mu.Unlock()
	if drop {
		// The peer already stored the conditional PUT. Lose the actual HTTP
		// reply so the SDK, rather than a Go result wrapper, observes failure.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			err = conn.Close()
		}
		p.mu.Lock()
		p.lost = err == nil
		p.mu.Unlock()
		return
	}
	for key, values := range response.Header() {
		w.Header()[key] = values
	}
	w.WriteHeader(response.Code)
	n, _ := w.Write(response.Body.Bytes())
	p.mu.Lock()
	p.data.Response += int64(n)
	p.mu.Unlock()
}

func (p *recoveryObjects) snapshot() recoveryTraffic {
	p.work.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data
}
