package fixtures

import (
	"strconv"
	"testing"
)

func BenchmarkEventCountFixture(b *testing.B) {
	for _, count := range EventCountFixtures {
		b.Run(formatCount(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				events := EventCountFixture("fixture-session", count)
				if len(events) != count {
					b.Fatalf("fixture length = %d, want %d", len(events), count)
				}
			}
		})
	}
}

func formatCount(count int) string {
	if count >= 1000 {
		return strconv.Itoa(count/1000) + "k"
	}
	return "small"
}
