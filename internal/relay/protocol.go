package relay

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/tmaxmax/go-sse"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Observation bounds cover native 3MP image events while preventing a corrupt
// frame length from allocating an arbitrary amount of memory.
const maxObservedEvent = 64 << 20

type observedEvent struct {
	kind      string
	sample    int64
	hasSample bool
	hasImage  bool
}

// ObserveStream checks completion while the caller tees the original bytes to
// its client. Success requires a final event for every sample and transport EOF;
// reaching the last final does not hide later provider or transport errors.
func ObserveStream(r io.Reader, mode string, samples int) error {
	if samples < 1 || samples > 8 {
		return errors.New("invalid expected stream sample count")
	}
	completed := make([]bool, samples)
	accept := func(event observedEvent) error {
		switch event.kind {
		case "error":
			return errors.New("upstream image stream reported an error")
		case "intermediate", "final":
			if !event.hasSample || event.sample < 0 || event.sample >= int64(samples) {
				return errors.New("image stream sample is missing or outside expected range")
			}
			if !event.hasImage {
				return errors.New("image stream event has no image")
			}
			if event.kind == "final" {
				if completed[event.sample] {
					return errors.New("image stream repeated a final sample")
				}
				completed[event.sample] = true
			}
		case "":
			return errors.New("image stream event has no event type")
		}
		return nil
	}
	switch mode {
	case "", "sse":
		for event, err := range sse.Read(r, &sse.ReadConfig{MaxEventSize: maxObservedEvent}) {
			if err != nil {
				return fmt.Errorf("read SSE stream: %w", err)
			}
			// ID-only heartbeats have no data or business event.
			if event.Data == "" && event.Type == "" {
				continue
			}
			parsed, err := observeJSONEvent(event.Type, event.Data)
			if err != nil {
				return err
			}
			if err = accept(parsed); err != nil {
				return err
			}
		}
	case "msgpack":
		for {
			var header [4]byte
			n, err := io.ReadFull(r, header[:])
			if err == io.EOF && n == 0 {
				break
			}
			if err != nil {
				return fmt.Errorf("read MessagePack frame header: %w", err)
			}
			size := binary.BigEndian.Uint32(header[:])
			if size == 0 || size > maxObservedEvent {
				return errors.New("image stream frame size outside observation limit")
			}
			frame := io.LimitedReader{R: r, N: int64(size)}
			reader := bufio.NewReader(&frame)
			parsed, err := observeMessagePackEvent(reader)
			if err != nil {
				return fmt.Errorf("decode MessagePack image event: %w", err)
			}
			if _, err = reader.Peek(1); err != io.EOF {
				return errors.New("image stream frame has trailing data")
			}
			if frame.N != 0 {
				return io.ErrUnexpectedEOF
			}
			if err = accept(parsed); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported image stream mode %q", mode)
	}
	for _, done := range completed {
		if !done {
			return errors.New("image stream ended before all final samples arrived")
		}
	}
	return nil
}

func officialEvent(name string) bool {
	return name == "intermediate" || name == "final" || name == "error"
}

func observeJSONEvent(header, data string) (observedEvent, error) {
	if header == "message" {
		header = ""
	}
	var fields map[string]json.RawMessage
	err := json.Unmarshal([]byte(data), &fields)
	var kind string
	if raw, ok := fields["event_type"]; ok {
		if json.Unmarshal(raw, &kind) != nil || string(raw) == "null" {
			if !officialEvent(header) && header != "" {
				return observedEvent{kind: header}, nil
			}
			return observedEvent{}, errors.New("image stream event_type must be a string")
		}
	}
	// Extensions can carry their own arbitrary payload, but cannot disguise a
	// conflicting official discriminator.
	if header != "" && !officialEvent(header) && !officialEvent(kind) {
		return observedEvent{kind: header}, nil
	}
	if err != nil {
		return observedEvent{}, fmt.Errorf("decode SSE image event: %w", err)
	}
	if header != "" && kind != "" && header != kind {
		return observedEvent{}, errors.New("SSE event name conflicts with event_type")
	}
	if kind == "" {
		kind = header
	}
	event := observedEvent{kind: kind}
	if kind == "intermediate" || kind == "final" {
		if raw, ok := fields["samp_ix"]; ok && string(raw) != "null" {
			if err := json.Unmarshal(raw, &event.sample); err != nil {
				return observedEvent{}, fmt.Errorf("invalid stream sample: %w", err)
			}
			event.hasSample = true
		}
		var image string
		if raw, ok := fields["image"]; ok {
			if err := json.Unmarshal(raw, &image); err != nil {
				return observedEvent{}, fmt.Errorf("invalid SSE image value: %w", err)
			}
			event.hasImage = len(image) > 0
		}
	}
	return event, nil
}

// MessagePack is inspected field by field. Image binary data is skipped in
// place; arrays of bytes are accepted as the native protocol also permits them.
func observeMessagePackEvent(reader *bufio.Reader) (observedEvent, error) {
	d := msgpack.NewDecoder(reader)
	n, err := d.DecodeMapLen()
	if err != nil {
		return observedEvent{}, err
	}
	if n < 0 {
		return observedEvent{}, errors.New("event must be a map")
	}
	event := observedEvent{}
	seen := map[string]bool{}
	duplicate := false
	for range n {
		key, err := strictMessagePackString(d)
		if err != nil {
			return observedEvent{}, err
		}
		switch key {
		case "event_type", "samp_ix", "image":
			if seen[key] {
				duplicate = true
			}
			seen[key] = true
		}
		switch key {
		case "event_type":
			event.kind, err = strictMessagePackString(d)
		case "samp_ix":
			code, peekErr := d.PeekCode()
			if peekErr != nil {
				return observedEvent{}, peekErr
			}
			if !(msgpcode.IsFixedNum(code) || code >= msgpcode.Uint8 && code <= msgpcode.Int64) {
				err = d.Skip()
				event.hasSample = false
				break
			}
			event.sample, err = d.DecodeInt64()
			event.hasSample = err == nil
		case "image":
			event.hasImage, err = skipMessagePackImage(d, reader)
		default:
			err = d.Skip()
		}
		if err != nil {
			return observedEvent{}, err
		}
	}
	if duplicate && officialEvent(event.kind) {
		return observedEvent{}, errors.New("duplicate image event field")
	}
	return event, nil
}

func strictMessagePackString(d *msgpack.Decoder) (string, error) {
	code, err := d.PeekCode()
	if err != nil {
		return "", err
	}
	if !msgpcode.IsString(code) {
		return "", errors.New("event field must be a MessagePack string")
	}
	return d.DecodeString()
}

func skipMessagePackImage(d *msgpack.Decoder, reader *bufio.Reader) (bool, error) {
	code, err := d.PeekCode()
	if err != nil {
		return false, err
	}
	if msgpcode.IsBin(code) {
		n, err := d.DecodeBytesLen()
		if err != nil {
			return false, err
		}
		_, err = io.CopyN(io.Discard, reader, int64(n))
		return n > 0, err
	}
	if msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32 {
		n, err := d.DecodeArrayLen()
		if err != nil {
			return false, err
		}
		valid := n > 0
		for range n {
			code, err := d.PeekCode()
			if err != nil {
				return false, err
			}
			if !(msgpcode.IsFixedNum(code) || code >= msgpcode.Uint8 && code <= msgpcode.Int64) {
				valid = false
				if err = d.Skip(); err != nil {
					return false, err
				}
				continue
			}
			b, err := d.DecodeInt64()
			if err != nil {
				return false, err
			}
			if b < 0 || b > 255 {
				valid = false
			}
		}
		return valid, nil
	}
	return false, d.Skip()
}
