package relay

import (
	"bytes"
	"errors"
	"io"
	"os"
)

var errBodyTooLarge = errors.New("request body exceeds limit")

// requestBody preserves exact request bytes while bounding resident memory.
type requestBody struct {
	data []byte
	file *os.File
	size int64
}

func spoolBody(r io.Reader, memoryLimit, maxBytes int64, tempDir string) (*requestBody, error) {
	b := &requestBody{}
	var memory bytes.Buffer
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			b.size += int64(n)
			if b.size > maxBytes {
				b.Close()
				return nil, errBodyTooLarge
			}
			if b.file == nil && b.size > memoryLimit {
				f, createErr := os.CreateTemp(tempDir, "nai-relay-body-*")
				if createErr != nil {
					return nil, createErr
				}
				b.file = f
				if _, writeErr := f.Write(memory.Bytes()); writeErr != nil {
					b.Close()
					return nil, writeErr
				}
				memory.Reset()
			}
			var writeErr error
			if b.file != nil {
				_, writeErr = b.file.Write(buf[:n])
			} else {
				_, writeErr = memory.Write(buf[:n])
			}
			if writeErr != nil {
				b.Close()
				return nil, writeErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Close()
			return nil, err
		}
	}
	if b.file == nil {
		b.data = memory.Bytes()
	}
	return b, nil
}

func (b *requestBody) Reader() (io.Reader, error) {
	if b.file == nil {
		return bytes.NewReader(b.data), nil
	}
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return b.file, nil
}

func (b *requestBody) Close() {
	if b.file != nil {
		name := b.file.Name()
		_ = b.file.Close()
		_ = os.Remove(name)
		b.file = nil
	}
	b.data = nil
}
