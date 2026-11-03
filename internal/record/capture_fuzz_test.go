package record

import (
	"bytes"
	"reflect"
	"testing"
)

func FuzzCaptureDecode(f *testing.F) {
	for _, threshold := range []int{1, 32768} {
		_, b, err := captureCodec(f, threshold).Prepare(captureFixture(BodyRedacted, BodyPresent))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, stored []byte) {
		if len(stored) > 8192 {
			t.Skip()
		}
		before := bytes.Clone(stored)
		c := captureCodec(t, 32768)
		v, err := c.Decode(stored)
		if !bytes.Equal(stored, before) {
			t.Fatal("decoder mutated input")
		}
		if err != nil {
			if !reflect.DeepEqual(v, Capture{}) {
				t.Fatal("partial error value")
			}
			return
		}
		wantRequest, wantResponse := bytes.Clone(v.Request.Data), bytes.Clone(v.Response.Data)
		clear(stored)
		if !bytes.Equal(v.Request.Data, wantRequest) || !bytes.Equal(v.Response.Data, wantResponse) {
			t.Fatal("read borrows encoded input")
		}
		prepared, b, err := c.Prepare(v)
		if err != nil {
			t.Fatal(err)
		}
		read, err := c.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if equal, err := c.Compare(prepared, read); err != nil || !equal {
			t.Fatal("successful decode cannot roundtrip", err)
		}
	})
}

func FuzzCaptureRoundTrip(f *testing.F) {
	f.Add([]byte{0, 255}, []byte("response"), byte(3), byte(0))
	f.Add([]byte{}, []byte{}, byte(0), byte(3))
	f.Fuzz(func(t *testing.T, request, response []byte, requestState, responseState byte) {
		if len(request) > 2048 || len(response) > 2048 {
			t.Skip()
		}
		states := []BodyState{BodyMissing, BodyRedacted, BodyTruncated, BodyPresent}
		v := captureFixture(states[requestState%4], states[responseState%4])
		v.Request.Data = request
		v.Response.Data = response
		if v.Request.State == BodyMissing || v.Request.State == BodyRedacted {
			v.Request.Data = nil
		}
		if v.Response.State == BodyMissing || v.Response.State == BodyRedacted {
			v.Response.Data = nil
		}
		c := captureCodec(t, 1)
		_, b, err := c.Prepare(v)
		if err != nil {
			t.Fatal(err)
		}
		read, err := c.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if read.Trace != v.Trace || read.Span != v.Span || read.Request.State != v.Request.State || read.Response.State != v.Response.State || !bytes.Equal(read.Request.Data, v.Request.Data) || !bytes.Equal(read.Response.Data, v.Response.Data) {
			t.Fatal("roundtrip lost original state or exact bytes")
		}
	})
}
