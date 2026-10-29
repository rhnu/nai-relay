package relay

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func sseEvent(kind string, sample int) string {
	data, _ := json.Marshal(map[string]any{"event_type": kind, "samp_ix": sample, "image": "aW1hZ2U="})
	return "event: " + kind + "\ndata: " + string(data) + "\n\n"
}

func msgpackFrame(t *testing.T, event map[string]any) []byte {
	t.Helper()
	payload, err := msgpack.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 4, len(payload)+4)
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	return append(frame, payload...)
}

func binaryFinal(t *testing.T, sample int) []byte {
	return msgpackFrame(t, map[string]any{"event_type": "final", "samp_ix": sample, "image": []byte{1, 2, 3}})
}

func TestObserveSSEWaitsForEverySampleAndEOF(t *testing.T) {
	raw := "id: heartbeat\n\n" + sseEvent("intermediate", 0) + sseEvent("final", 1) + sseEvent("final", 0) + "event: extension\ndata: arbitrary payload\n\n"
	var forwarded bytes.Buffer
	if err := ObserveStream(io.TeeReader(strings.NewReader(raw), &forwarded), "sse", 2); err != nil {
		t.Fatal(err)
	}
	if forwarded.String() != raw {
		t.Fatal("SSE bytes changed or stream was not fully consumed")
	}
	large := `data: {"event_type":"final","samp_ix":0,"image":"` + strings.Repeat("A", 100000) + `"}` + "\n\n"
	if err := ObserveStream(strings.NewReader(large), "sse", 1); err != nil {
		t.Fatalf("large native image event: %v", err)
	}
}

func TestObserveSSEFailures(t *testing.T) {
	cases := map[string]string{
		"missing final":              sseEvent("final", 0),
		"provider error after final": sseEvent("final", 0) + sseEvent("final", 1) + "event: error\ndata: {\"message\":\"failed\"}\n\n",
		"duplicate final":            sseEvent("final", 0) + sseEvent("final", 0) + sseEvent("final", 1),
		"out of range":               sseEvent("final", 0) + sseEvent("final", 2),
		"conflicting identity":       "event: extension\ndata: {\"event_type\":\"final\",\"samp_ix\":0,\"image\":\"a\"}\n\n",
		"missing image":              "data: {\"event_type\":\"final\",\"samp_ix\":0}\n\n",
		"invalid JSON":               "event: final\ndata: not-json\n\n",
		"missing discriminator":      "data: {\"samp_ix\":0,\"image\":\"a\"}\n\n",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ObserveStream(strings.NewReader(raw), "sse", 2); err == nil {
				t.Fatal("accepted invalid stream")
			}
		})
	}
}

func TestObserveMessagePackRawBinaryAndByteArray(t *testing.T) {
	raw := msgpackFrame(t, map[string]any{"event_type": "intermediate", "samp_ix": 0, "image": []any{1, int16(2), uint16(255)}, "extension": map[string]any{"future": true}})
	raw = append(raw, binaryFinal(t, 1)...)
	raw = append(raw, binaryFinal(t, 0)...)
	raw = append(raw, msgpackFrame(t, map[string]any{"event_type": "extension", "image": "arbitrary extension payload", "samp_ix": "extension schema"})...)
	var forwarded bytes.Buffer
	if err := ObserveStream(io.TeeReader(bytes.NewReader(raw), &forwarded), "msgpack", 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forwarded.Bytes(), raw) {
		t.Fatal("MessagePack bytes changed or stream was not fully consumed")
	}
}

func TestObserveMessagePackFailures(t *testing.T) {
	final := binaryFinal(t, 0)
	trailing := append(append([]byte(nil), final...), msgpackFrame(t, map[string]any{"event_type": "error", "message": "failed"})...)
	duplicate := append(append([]byte(nil), final...), final...)
	badFrame := append(append([]byte(nil), final...), 0)
	binary.BigEndian.PutUint32(badFrame, uint32(len(badFrame)-4))
	cases := map[string][]byte{
		"missing final":       {},
		"trailing error":      trailing,
		"duplicate final":     duplicate,
		"truncated header":    {0, 0},
		"truncated payload":   final[:len(final)-1],
		"trailing frame data": badFrame,
		"huge frame":          {0xff, 0xff, 0xff, 0xff},
		"out of range":        binaryFinal(t, 1),
		"string image":        msgpackFrame(t, map[string]any{"event_type": "final", "samp_ix": 0, "image": "base64"}),
		"bad image array":     msgpackFrame(t, map[string]any{"event_type": "final", "samp_ix": 0, "image": []any{256}}),
		"missing type":        msgpackFrame(t, map[string]any{"samp_ix": 0, "image": []byte{1}}),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ObserveStream(bytes.NewReader(raw), "msgpack", 1); err == nil {
				t.Fatal("accepted invalid stream")
			}
		})
	}
}

type streamReadFailure struct{ io.Reader }

func (r streamReadFailure) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, errors.New("transport failed")
	}
	return n, err
}

func TestObserveStreamPropagatesTrailingTransportError(t *testing.T) {
	if err := ObserveStream(streamReadFailure{strings.NewReader(sseEvent("final", 0))}, "sse", 1); err == nil {
		t.Fatal("ignored SSE transport error")
	}
	if err := ObserveStream(streamReadFailure{bytes.NewReader(binaryFinal(t, 0))}, "msgpack", 1); err == nil {
		t.Fatal("ignored MessagePack transport error")
	}
}
