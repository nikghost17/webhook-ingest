# SOLUTION.md

## What was broken, and why

Four defects, all traceable to the symptoms in the incident report.

**1. Data race in `stats.Cache.Record()` → drifting call counts**
`Record()` read and wrote the shared map without holding any lock, while
`Get()` correctly used `RLock`. Under concurrent webhook deliveries the
unsynchronized map writes corrupt counters and can panic with a concurrent
map write. Fixed by adding `mu.Lock()`/`defer mu.Unlock()` to `Record()`.

**2. Recording goroutine captured the HTTP request context → recordings
never marked processed, nothing in logs**
The fire-and-forget goroutine called `processRecording(ctx, rec)` where
`ctx` was the HTTP request context. That context is cancelled the moment
the handler writes its 200 response — which happens *before* the goroutine's
50 ms sleep even finishes. Every call to `MarkRecordingProcessed` therefore
failed with `context.Canceled`. The error was silently swallowed by a
`// TODO: handle` comment, so nothing appeared in logs. Fixed by switching
to `context.Background()` and logging the error.

**3. In-flight goroutines lost on deploy → work disappears on restart**
`srv.Shutdown()` waits for active HTTP handlers to return, but not for
fire-and-forget goroutines. A SIGTERM mid-flight silently discarded any
recording work in progress. Fixed by adding a `sync.WaitGroup` to `Service`
and calling `svc.Wait()` in `main` after `srv.Shutdown()` returns.

**4. TOCTOU race in idempotency check → duplicate records, double-counted stats**
`Ingest()` checked `EventExists` then called `InsertEvent` as two separate
round-trips with no atomicity between them. Two concurrent deliveries of the
same `event_id` could both observe "not exists", both insert, and both
increment `account_stats` — explaining every symptom in the ops report.
The `events` table also had only a plain index on `event_id`, not a UNIQUE
constraint, so the duplicate INSERT succeeded silently.

---

## Why Postgres over Redis for deduplication

**The approach chosen:** a `UNIQUE` constraint on `events.event_id` combined
with `INSERT ... ON CONFLICT (event_id) DO NOTHING` inside a transaction that
also runs `UpsertCall` and `IncrementAccountStats`.

**Alternatives considered:**

- **Redis `SETNX` as a fast-path guard.** Attractive because it avoids a DB
  round-trip for the common duplicate case. Rejected because it introduces a
  second source of truth: if Redis evicts the key (TTL expiry, memory pressure,
  restart without persistence) before Postgres has the row, a duplicate slips
  through. Correctness now depends on two systems staying in sync, which is
  harder to reason about and harder to test.

- **Application-level `EventExists` check (original code).** Rejected because
  it is not atomic — the TOCTOU window is exactly where the bug lives.

The Postgres-only approach wins because the UNIQUE constraint makes the
deduplication atomic at the storage layer. A duplicate `INSERT` either
succeeds or it doesn't; there is no race between check and insert. It also
means correctness does not depend on Redis availability — if Redis goes down
the service degrades gracefully (stats endpoint returns zeroes; ingestion
still works). Redis is a performance optimisation, not a correctness
requirement, and I would not use it for something that must be exactly-once.

---

