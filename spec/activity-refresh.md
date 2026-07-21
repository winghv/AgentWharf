# Activity Projection Refresh

## Scope

This is an injected Hub/Control Plane boundary, not a WebSocket frame, REST
resource, durable event, Store schema change, or Adapter capability. It exists
only to recover delivery of an already Store-committed, provider-neutral
ActivitySink summary when the Control Plane has made its own projection fail
closed.

The request has no fields. In particular, it carries no Task, Run, VM, tenant,
provider, bearer, credential, content, grant, or command semantics. The Hub
does not learn why the Control Plane requested a refresh and the Control Plane
does not read Hub Store tables.

## Contract

The embedding boundary exposes `RequestActivityRefresh(context.Context) error`.
Calling it asks the Hub to perform one bounded, keyset-paged rescan using the
existing `AttentionSummaryPageStore` and the existing ActivitySink summary
mapping.

- The operation is idempotent. While a refresh scan is pending or running,
  concurrent calls coalesce into that scan and create no per-request goroutine,
  timer, queue, durable record, or Store write.
- A request is satisfied only after a complete bounded rescan has delivered
  Store-derived summaries to the existing ActivitySink. A cancellation or page
  or sink failure satisfies nothing and is returned to the caller.
- Hub unavailability, shutdown, malformed pages, Store errors, or sink errors
  fail closed. The Control Plane keeps its projection incomplete or stale and
  continues to reject idle suspension; it never infers inactivity from a failed
  request.
- The rescan preserves original Store-clock activity timestamps, Store snapshot
  time, sequence, ledger version, projection state, and blocker expiry. Request,
  dispatch, callback, and retry time never renew activity or a lease.

The existing periodic dispatcher remains the sole owner of Store scans and
ActivitySink delivery. This request only schedules an immediate coalesced scan;
it neither changes authorization, replay, live event routing, nor durable
session truth.
