package store_test

import (
	"context"
	"testing"

	"github.com/winghv/agentwharf/store"
)

func TestAttentionSummaryPageStoreContract(t *testing.T) {
	var _ store.AttentionSummaryPageStore = (*pageStore)(nil)

	page, err := (&pageStore{}).AttentionSummaryPage(context.Background(), store.AttentionSummaryPageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("AttentionSummaryPage() error = %v", err)
	}
	if len(page.Summaries) != 0 || page.NextAfterSessionID != nil {
		t.Fatalf("empty page = %+v", page)
	}
}

type pageStore struct{}

func (*pageStore) AttentionSnapshot(context.Context, []string) ([]store.SessionAttentionSummary, error) {
	return nil, nil
}

func (*pageStore) AttentionSummaryPage(context.Context, store.AttentionSummaryPageRequest) (store.AttentionSummaryPage, error) {
	return store.AttentionSummaryPage{}, nil
}
