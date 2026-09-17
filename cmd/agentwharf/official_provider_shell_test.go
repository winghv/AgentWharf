package main

import "testing"

func TestQuoteOfficialWindowsBatchLine(t *testing.T) {
	got, err := quoteOfficialWindowsBatchLine([]string{`C:\Program Files\Claude\claude.cmd`, "--model", `Claude "Sonnet"`})
	if err != nil {
		t.Fatal(err)
	}
	want := `"C:\Program Files\Claude\claude.cmd" "--model" "Claude ""Sonnet"""`
	if got != want {
		t.Fatalf("quoteOfficialWindowsBatchLine() = %q, want %q", got, want)
	}
}

func TestQuoteOfficialWindowsBatchLineRejectsControlCharacters(t *testing.T) {
	if _, err := quoteOfficialWindowsBatchLine([]string{"claude.cmd", "bad\rargument"}); err == nil {
		t.Fatal("control character was accepted")
	}
}
