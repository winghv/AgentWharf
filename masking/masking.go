package masking

import (
	"io"
	"strings"
)

const DefaultReplacement = "[MASKED]"
const MinSecretLength = 4

type Masker struct {
	secrets     []string
	replacement string
	keepTail    int
}

func New(secrets []string) *Masker {
	return NewWithReplacement(secrets, DefaultReplacement)
}

func NewWithReplacement(secrets []string, replacement string) *Masker {
	if replacement == "" {
		replacement = DefaultReplacement
	}
	seen := make(map[string]struct{}, len(secrets))
	filtered := make([]string, 0, len(secrets))
	maxLen := 0
	for _, secret := range secrets {
		if len(secret) < MinSecretLength {
			continue
		}
		if _, ok := seen[secret]; ok {
			continue
		}
		seen[secret] = struct{}{}
		filtered = append(filtered, secret)
		if len(secret) > maxLen {
			maxLen = len(secret)
		}
	}
	keepTail := 0
	if maxLen > 1 {
		keepTail = maxLen - 1
	}
	return &Masker{secrets: filtered, replacement: replacement, keepTail: keepTail}
}

func (m *Masker) MaskString(text string) string {
	if m == nil || len(m.secrets) == 0 || text == "" {
		return text
	}
	for _, secret := range m.secrets {
		text = strings.ReplaceAll(text, secret, m.replacement)
	}
	return text
}

func NewReader(reader io.Reader, secrets []string) io.Reader {
	return &Reader{reader: reader, masker: New(secrets)}
}

type Reader struct {
	reader  io.Reader
	masker  *Masker
	pending []byte
	out     []byte
	eof     bool
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.out) == 0 && !r.eof {
		buf := make([]byte, 32*1024)
		n, err := r.reader.Read(buf)
		if n > 0 {
			r.out = append(r.out, maskStreamChunk(r.masker, &r.pending, buf[:n], false)...)
		}
		if err == io.EOF {
			r.eof = true
			r.out = append(r.out, maskStreamChunk(r.masker, &r.pending, nil, true)...)
			break
		}
		if err != nil {
			return 0, err
		}
	}
	if len(r.out) == 0 && r.eof {
		return 0, io.EOF
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

func NewWriter(writer io.Writer, secrets []string) *Writer {
	return &Writer{writer: writer, masker: New(secrets)}
}

type Writer struct {
	writer  io.Writer
	masker  *Masker
	pending []byte
	closed  bool
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	masked := maskStreamChunk(w.masker, &w.pending, p, false)
	if len(masked) > 0 {
		if _, err := w.writer.Write(masked); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	masked := maskStreamChunk(w.masker, &w.pending, nil, true)
	if len(masked) > 0 {
		if _, err := w.writer.Write(masked); err != nil {
			return err
		}
	}
	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func maskStreamChunk(masker *Masker, pending *[]byte, chunk []byte, flush bool) []byte {
	if masker == nil || len(masker.secrets) == 0 {
		out := append(append([]byte(nil), *pending...), chunk...)
		*pending = nil
		return out
	}

	data := append(append([]byte(nil), *pending...), chunk...)
	out := make([]byte, 0, len(data))
	for len(data) > 0 {
		index, secret := masker.nextMatch(data)
		if index >= 0 {
			out = append(out, data[:index]...)
			out = append(out, masker.replacement...)
			data = data[index+len(secret):]
			continue
		}
		if flush {
			out = append(out, data...)
			data = nil
			break
		}
		keep := masker.keepTail
		if keep > len(data) {
			keep = len(data)
		}
		safeLen := len(data) - keep
		out = append(out, data[:safeLen]...)
		data = data[safeLen:]
		break
	}
	*pending = append((*pending)[:0], data...)
	return out
}

func (m *Masker) nextMatch(data []byte) (int, string) {
	text := string(data)
	bestIndex := -1
	bestSecret := ""
	for _, secret := range m.secrets {
		index := strings.Index(text, secret)
		if index < 0 {
			continue
		}
		if bestIndex == -1 || index < bestIndex || (index == bestIndex && len(secret) > len(bestSecret)) {
			bestIndex = index
			bestSecret = secret
		}
	}
	return bestIndex, bestSecret
}
