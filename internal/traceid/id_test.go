package traceid

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestHexIdentityAndValueOwnership(t *testing.T) {
	traceHex := "00ff102030405060708090a0b0c0d0ef"
	spanHex := "00ff1020304050ab"
	a, err := ParseTrace(traceHex)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseTrace(strings.ToUpper(traceHex))
	if err != nil || a != b || a.String() != traceHex {
		t.Fatal("hex case changed trace identity")
	}
	x, err := ParseSpan(spanHex)
	if err != nil {
		t.Fatal(err)
	}
	y, err := ParseSpan(strings.ToUpper(spanHex))
	if err != nil || x != y || x.String() != spanHex {
		t.Fatal("hex case changed span identity")
	}
	type execution struct {
		Trace Trace
		Span  Span
	}
	key := execution{a, x}
	bindings := map[execution]string{key: "alice"}
	if bindings[execution{b, y}] != "alice" {
		t.Fatal("equivalent hex keys did not resolve the same binding")
	}
	traceBytes, _ := hex.DecodeString(traceHex)
	spanBytes, _ := hex.DecodeString(spanHex)
	ownedTrace, err := TraceFromBytes(traceBytes)
	if err != nil || ownedTrace != a {
		t.Fatal("trace bytes changed in conversion")
	}
	ownedSpan, err := SpanFromBytes(spanBytes)
	if err != nil || ownedSpan != x {
		t.Fatal("span bytes changed in conversion")
	}
	clear(traceBytes)
	clear(spanBytes)
	if ownedTrace != a || ownedSpan != x {
		t.Fatal("returned key retained mutable input")
	}
	key.Span[7]++
	if _, found := bindings[key]; found {
		t.Fatal("distinct span collapsed into existing binding")
	}
}

func TestInvalidIDsReturnZero(t *testing.T) {
	for _, width := range []int{8, 16} {
		for _, raw := range [][]byte{nil, {}, make([]byte, width-1), bytes.Repeat([]byte{1}, width+1), make([]byte, width)} {
			if width == 16 {
				id, err := TraceFromBytes(raw)
				if !errors.Is(err, ErrInvalid) || id != (Trace{}) {
					t.Fatalf("raw trace length %d accepted", len(raw))
				}
			} else {
				id, err := SpanFromBytes(raw)
				if !errors.Is(err, ErrInvalid) || id != (Span{}) {
					t.Fatalf("raw span length %d accepted", len(raw))
				}
			}
		}
		for _, text := range []string{
			"", strings.Repeat("0", 2*width), strings.Repeat("1", 2*width-1),
			strings.Repeat("1", 2*width+1), strings.Repeat("1", 2*width-1) + "z",
			" " + strings.Repeat("1", 2*width-1), "0x" + strings.Repeat("1", 2*width-2),
		} {
			if width == 16 {
				id, err := ParseTrace(text)
				if !errors.Is(err, ErrInvalid) || id != (Trace{}) {
					t.Fatal("invalid hex trace returned nonzero or success")
				}
			} else {
				id, err := ParseSpan(text)
				if !errors.Is(err, ErrInvalid) || id != (Span{}) {
					t.Fatal("invalid hex span returned nonzero or success")
				}
			}
		}
	}
	if (Trace{}).Valid() || (Span{}).Valid() {
		t.Fatal("zero arrays considered valid")
	}
	// Any one nonzero byte suffices; no byte position may be ignored.
	for i := range 16 {
		var id Trace
		id[i] = 1
		got, err := ParseTrace(id.String())
		if err != nil || got != id || !id.Valid() {
			t.Fatal("single nonzero trace byte lost")
		}
	}
	for i := range 8 {
		var id Span
		id[i] = 1
		got, err := ParseSpan(id.String())
		if err != nil || got != id || !id.Valid() {
			t.Fatal("single nonzero span byte lost")
		}
	}
}

func FuzzHexIdentity(f *testing.F) {
	for _, s := range []string{"", "00FF1020304050ab", "00ff102030405060708090a0b0c0d0ef", strings.Repeat("0", 32), strings.Repeat("a", 31) + "z"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Only exact-width inputs can succeed; bound the independent oracle too.
		var raw []byte
		var err error
		if len(s) == 16 || len(s) == 32 {
			raw, err = hex.DecodeString(s)
		}
		nonzero := false
		for _, b := range raw {
			nonzero = nonzero || b != 0
		}
		trace, traceErr := ParseTrace(s)
		span, spanErr := ParseSpan(s)
		wantTrace := len(s) == 32 && err == nil && nonzero
		wantSpan := len(s) == 16 && err == nil && nonzero
		if (traceErr == nil) != wantTrace || (spanErr == nil) != wantSpan {
			t.Fatal("parser disagreed with hex/width/nonzero oracle")
		}
		if wantTrace {
			if !bytes.Equal(trace[:], raw) || trace.String() != strings.ToLower(s) {
				t.Fatal("trace identity changed")
			}
		} else if trace != (Trace{}) || !errors.Is(traceErr, ErrInvalid) {
			t.Fatal("invalid trace leaked partially decoded value")
		}
		if wantSpan {
			if !bytes.Equal(span[:], raw) || span.String() != strings.ToLower(s) {
				t.Fatal("span identity changed")
			}
		} else if span != (Span{}) || !errors.Is(spanErr, ErrInvalid) {
			t.Fatal("invalid span leaked partially decoded value")
		}
	})
}
