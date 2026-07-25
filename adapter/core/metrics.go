package core

import (
	"fmt"
	"sync/atomic"
)

// AdapterMetrics contains only bounded process-level counters. Session IDs,
// command content and credential material never become metric labels.
type AdapterMetrics struct {
	workers         atomic.Int64
	activeWorkers   atomic.Int64
	queuedWorkers   atomic.Int64
	receiptFailures atomic.Uint64
	maskedEvents    atomic.Uint64
}

type AdapterMetricSnapshot struct {
	Workers         int64
	ActiveWorkers   int64
	QueuedWorkers   int64
	ReceiptFailures uint64
	MaskedEvents    uint64
}

func NewAdapterMetrics() *AdapterMetrics { return &AdapterMetrics{} }

func (m *AdapterMetrics) Snapshot() AdapterMetricSnapshot {
	if m == nil {
		return AdapterMetricSnapshot{}
	}
	return AdapterMetricSnapshot{
		Workers:         m.workers.Load(),
		ActiveWorkers:   m.activeWorkers.Load(),
		QueuedWorkers:   m.queuedWorkers.Load(),
		ReceiptFailures: m.receiptFailures.Load(),
		MaskedEvents:    m.maskedEvents.Load(),
	}
}

func (m *AdapterMetrics) SetWorkerCounts(workers, active, queued int64) {
	if m == nil {
		return
	}
	if workers < 0 {
		workers = 0
	}
	if active < 0 {
		active = 0
	}
	if queued < 0 {
		queued = 0
	}
	m.workers.Store(workers)
	m.activeWorkers.Store(active)
	m.queuedWorkers.Store(queued)
}

func (m *AdapterMetrics) IncReceiptFailure() {
	if m != nil {
		m.receiptFailures.Add(1)
	}
}

func (m *AdapterMetrics) IncMaskedEvent() {
	if m != nil {
		m.maskedEvents.Add(1)
	}
}

func (s AdapterMetricSnapshot) Prometheus() string {
	return fmt.Sprintf("# HELP agentwharf_adapter_workers Current Adapter worker count.\n# TYPE agentwharf_adapter_workers gauge\nagentwharf_adapter_workers %d\n# HELP agentwharf_adapter_active_workers Current active Adapter workers.\n# TYPE agentwharf_adapter_active_workers gauge\nagentwharf_adapter_active_workers %d\n# HELP agentwharf_adapter_queued_workers Current queued Adapter workers.\n# TYPE agentwharf_adapter_queued_workers gauge\nagentwharf_adapter_queued_workers %d\n# HELP agentwharf_adapter_receipt_failures Durable receipt failures.\n# TYPE agentwharf_adapter_receipt_failures counter\nagentwharf_adapter_receipt_failures %d\n# HELP agentwharf_adapter_masked_events Events processed by the masking boundary.\n# TYPE agentwharf_adapter_masked_events counter\nagentwharf_adapter_masked_events %d\n", s.Workers, s.ActiveWorkers, s.QueuedWorkers, s.ReceiptFailures, s.MaskedEvents)
}
