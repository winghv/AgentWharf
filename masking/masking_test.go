package masking_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/masking"
)

func TestMaskerReplacesMultipleSecrets(t *testing.T) {
	t.Parallel()

	masker := masking.New([]string{"token-123", "password-value"})
	got := masker.MaskString("token-123 and password-value are hidden")
	if got != "[MASKED] and [MASKED] are hidden" {
		t.Fatalf("MaskString() = %q", got)
	}
}

func TestMaskerIgnoresShortAndEmptySecrets(t *testing.T) {
	t.Parallel()

	masker := masking.New([]string{"", "abc", "long-secret"})
	got := masker.MaskString("abc stays but long-secret is hidden")
	if got != "abc stays but [MASKED] is hidden" {
		t.Fatalf("MaskString() = %q", got)
	}
}

func TestReaderMasksAcrossChunkBoundaries(t *testing.T) {
	t.Parallel()

	source := &chunkedReader{chunks: []string{"prefix tok", "en-1", "23 suffix"}}
	reader := masking.NewReader(source, []string{"token-123"})
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "prefix [MASKED] suffix" {
		t.Fatalf("masked body = %q", string(body))
	}
}

func TestWriterMasksAcrossWrites(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	writer := masking.NewWriter(&buf, []string{"secret-value"})
	for _, chunk := range []string{"before sec", "ret-", "value after"} {
		if _, err := writer.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if buf.String() != "before [MASKED] after" {
		t.Fatalf("masked writer body = %q", buf.String())
	}
}

func TestReaderWithoutSecretsPassesThrough(t *testing.T) {
	t.Parallel()

	reader := masking.NewReader(strings.NewReader("plain text"), nil)
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "plain text" {
		t.Fatalf("body = %q", string(body))
	}
}

type chunkedReader struct {
	chunks []string
	index  int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.index >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.index])
	if n == len(r.chunks[r.index]) {
		r.index++
	} else {
		r.chunks[r.index] = r.chunks[r.index][n:]
	}
	return n, nil
}
