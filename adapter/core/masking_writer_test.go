package core_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/winghv/agentwharf/adapter/core"
)

// MaskWriter is the streaming half of the masking boundary: MaskEvent covers
// durable protocol payloads, and this covers raw Provider output on its way to
// a writer. It had no test at all, which matters because the interesting
// property is not "a secret in one Write call is masked" -- it is that a secret
// straddling two Write calls is still masked. A per-call implementation would
// pass the naive test and leak on the real stream, where chunk boundaries fall
// wherever the pipe happens to split.
func TestEventMaskerMaskWriterMasksSecretsAcrossChunkBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		chunks []string
		want   string
	}{
		{
			name:   "secret contained in a single write",
			chunks: []string{"token is secret-token here"},
			want:   "token is [MASKED] here",
		},
		{
			name:   "secret split across two writes",
			chunks: []string{"token is secret-", "token here"},
			want:   "token is [MASKED] here",
		},
		{
			name:   "secret split one byte at a time",
			chunks: strings.Split("prefix secret-token suffix", ""),
			want:   "prefix [MASKED] suffix",
		},
		{
			name:   "secret ends exactly at the final write with no trailing text",
			chunks: []string{"trailing ", "secret-token"},
			want:   "trailing [MASKED]",
		},
		{
			name:   "two different secrets in one stream",
			chunks: []string{"a secret-token b ", "db-password c"},
			want:   "a [MASKED] b [MASKED] c",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			masker := core.NewEventMasker([]string{"secret-token", "db-password"})
			var sink bytes.Buffer
			writer := masker.MaskWriter(&sink)

			for i, chunk := range tc.chunks {
				n, err := writer.Write([]byte(chunk))
				if err != nil {
					t.Fatalf("chunk %d Write: unexpected error: %v", i, err)
				}
				// The contract is io.Writer's: report every byte accepted, not
				// the smaller number actually forwarded downstream. A masking
				// writer buffers a partial secret, so a naive implementation
				// returning len(masked) would make callers like io.Copy report
				// a short write and fail a stream that is working correctly.
				if n != len(chunk) {
					t.Fatalf("chunk %d Write returned n=%d, want %d (io.Writer must report all bytes consumed, including any held back as a partial secret)", i, n, len(chunk))
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("Close: unexpected error: %v", err)
			}

			if got := sink.String(); got != tc.want {
				t.Fatalf("masked stream = %q, want %q", got, tc.want)
			}
			for _, secret := range []string{"secret-token", "db-password"} {
				if strings.Contains(sink.String(), secret) {
					t.Fatalf("masked stream %q still contains the raw secret %q", sink.String(), secret)
				}
			}
		})
	}
}

// Close is what flushes a trailing partial secret, so skipping it is the one
// way to leak through this writer. Asserted explicitly: without Close the held
// bytes must not have reached the sink, and after Close they must be masked
// rather than emitted raw.
func TestEventMaskerMaskWriterHoldsPartialSecretUntilClose(t *testing.T) {
	t.Parallel()

	masker := core.NewEventMasker([]string{"secret-token"})
	var sink bytes.Buffer
	writer := masker.MaskWriter(&sink)

	if _, err := writer.Write([]byte("head secret-")); err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if got := sink.String(); strings.Contains(got, "secret-") {
		t.Fatalf("sink = %q; the partial secret must be held back, not forwarded", got)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
	// "secret-" alone is not the secret, so flushing it verbatim is correct
	// here; what must never happen is the completed secret escaping.
	if got := sink.String(); got != "head secret-" {
		t.Fatalf("flushed stream = %q, want %q", got, "head secret-")
	}
}

// A nil *EventMasker is reachable: callers that never configured secrets hold
// the zero value, and MaskWriter is written to tolerate it. Without a test, a
// future edit that dereferences m before the nil check would panic on that
// path instead of degrading to pass-through.
func TestEventMaskerMaskWriterNilReceiverPassesThroughWithoutPanicking(t *testing.T) {
	t.Parallel()

	var masker *core.EventMasker
	var sink bytes.Buffer

	writer := masker.MaskWriter(&sink)
	if writer == nil {
		t.Fatal("MaskWriter on a nil receiver returned nil; callers write to it unconditionally")
	}
	if _, err := writer.Write([]byte("no secrets configured")); err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
	if got := sink.String(); got != "no secrets configured" {
		t.Fatalf("pass-through stream = %q, want %q", got, "no secrets configured")
	}
}

// The returned value is an io.WriteCloser, and Close is documented to close the
// underlying writer when it is itself a Closer. Adapter code relies on that to
// tear down a Provider pipe with one call, so it is asserted rather than
// assumed.
func TestEventMaskerMaskWriterClosePropagatesToUnderlyingCloser(t *testing.T) {
	t.Parallel()

	sink := &closeRecordingWriter{}
	masker := core.NewEventMasker([]string{"secret-token"})

	// Not asserting the io.WriteCloser type here: MaskWriter's signature already
	// returns io.WriteCloser, so the compiler enforces it and a runtime
	// assertion would only restate the declaration.
	writer := masker.MaskWriter(sink)
	if _, err := writer.Write([]byte("secret-token")); err != nil {
		t.Fatalf("Write: unexpected error: %v", err)
	}
	if sink.closed {
		t.Fatal("underlying writer closed before Close was called")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
	if !sink.closed {
		t.Fatal("Close did not propagate to the underlying io.Closer")
	}
	if got := sink.buf.String(); got != "[MASKED]" {
		t.Fatalf("flushed stream = %q, want %q", got, "[MASKED]")
	}
}

type closeRecordingWriter struct {
	buf    bytes.Buffer
	closed bool
}

func (w *closeRecordingWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *closeRecordingWriter) Close() error {
	w.closed = true
	return nil
}
