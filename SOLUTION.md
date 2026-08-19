# SOLUTION.md

## 1. What was broken, and why

Four defects, all traceable to the symptoms in the incident report.

---

### Bug 1 — Data race in `stats.Cache.Record()` → drifting call counts

`Record()` read and wrote the shared `m` map without holding any lock, while `Get()` correctly held an `RLock`. Under concurrent webhook deliveries, unsynchronised map writes corrupt counters and can panic with *concurrent map write*. This is why account call-counts drifted higher than the actual number of calls.

**Fix:** Added `mu.Lock()` / `defer mu.Unlock()` to `Record()`. A test (`TestCacheRecordConcurrent`) was added that fires 100 goroutines concurrently; it fails with the race detector enabled before the fix and passes after.

---

### Bug 2 — Recording goroutine captured the HTTP request context → recordings never marked processed, nothing in logs

The fire-and-forget goroutine called `processRecording(ctx, rec)` where `ctx` was the HTTP request context. That context is cancelled the moment the handler writes its `200` response — which happens *before* the goroutine's 50 ms sleep even finishes. Every subsequent call to `MarkRecordingProcessed` therefore failed with `context.Canceled`. The error was silently swallowed by a `// TODO: handle` comment, so nothing appeared in the logs.

**Fix:** Switched to `context.Background()` and added proper error logging. A test (`TestRecordingIsMarkedProcessed`) waits 500 ms after the `200` and asserts that `recording_processed` is `TRUE`; it fails before the fix and passes after.

---

### Bug 3 — In-flight goroutines lost on deploy → work disappears on restart

`srv.Shutdown()` waits for active HTTP handlers to return, but not for fire-and-forget goroutines. A `SIGTERM` mid-flight would silently discard any recording work still in progress.

**Fix:** Added a `sync.WaitGroup` to `Service` and called `svc.Wait()` in `main` after `srv.Shutdown()` returns. The WaitGroup is incremented before every goroutine is spawned and decremented via `defer wg.Done()`.

---

### Bug 4 — TOCTOU race in idempotency check → duplicate records, double-counted stats

`Ingest()` checked `EventExists` then called `InsertEvent` as two separate round-trips with no atomicity between them. Two concurrent deliveries of the same `event_id` could both observe "not exists", both insert, and both increment `account_stats` — explaining every symptom in the ops report. The `events` table also had only a plain index on `event_id`, not a `UNIQUE` constraint, so the duplicate `INSERT` succeeded silently.

**Fix:** Replaced the two-step check with a single `IngestTx` function that runs `INSERT … ON CONFLICT (event_id) DO NOTHING`, `UpsertCall`, and `IncrementAccountStats` inside one transaction. A new migration (`002_unique_event_id.sql`) adds the `UNIQUE` constraint that makes the conflict clause enforceable. Two tests cover this: `TestDuplicateDeliveryDoesNotDoubleCountStats` (sequential) and `TestConcurrentDuplicateDeliveryIsIdempotent` (20 goroutines firing the same event simultaneously).

---

## 2. Why Postgres over Redis for deduplication

**Chosen approach:** a `UNIQUE` constraint on `events.event_id` combined with `INSERT … ON CONFLICT (event_id) DO NOTHING` inside a transaction that also runs `UpsertCall` and `IncrementAccountStats`.

**Alternatives considered:**

- **Redis `SETNX` as a fast-path guard.** Attractive because it avoids a DB round-trip for the common duplicate case. Rejected because it introduces a second source of truth: if Redis evicts the key (TTL expiry, memory pressure, restart without persistence) before Postgres has the row, a duplicate slips through. Correctness now depends on two systems staying in sync — harder to reason about and harder to test.

- **Application-level `EventExists` check (the original code).** Rejected because it is not atomic — the TOCTOU window is exactly where the bug lives.

The Postgres-only approach wins because the `UNIQUE` constraint makes deduplication atomic at the storage layer. A duplicate `INSERT` either succeeds or it does not; there is no race between check and insert. It also means correctness does not depend on Redis availability — if Redis goes down the service degrades gracefully (stats endpoint returns zeroes; ingestion still works). Redis is a performance optimisation, not a correctness requirement, and I would not use it for something that must be exactly-once.

---

## 3. What I would change for 10,000 webhooks/second

At that throughput the single biggest bottleneck is the synchronous, 3-statement Postgres transaction on the hot path of every webhook delivery.

**Write path**

- **Async persistence with a write-ahead queue.** Accept the webhook, write to a durable in-process queue (e.g. a Kafka topic or even a Postgres-backed outbox table), return `200` immediately, and let a separate worker pool commit the transaction. This decouples ingestion latency from Postgres commit latency.
- **Batch inserts.** The worker pool can coalesce events into `COPY` or multi-row `INSERT` statements, amortising per-row overhead. At 10k rps with 10 ms batching windows, a single flush covers ~100 rows instead of 1.
- **Partitioned `events` table.** A single `events` table with a `UNIQUE` constraint is a write hotspot. Partitioning by `received_at` (e.g. monthly) spreads the index writes and allows old partitions to be dropped cheaply.

**Deduplication at scale**

- **Redis Bloom filter as a pre-flight guard.** At this volume, the Postgres `UNIQUE` constraint is still the source of truth, but a Bloom filter in Redis can reject obvious duplicates in microseconds before they even reach the DB. False positives (legitimate events incorrectly rejected) are controlled by choosing an appropriate error rate (e.g. 0.1%). This adds Redis to the correctness story, so its false-positive rate must be explicitly sized and monitored.
- **Idempotency key TTL.** For at-least-once providers, duplicates cluster within a small window (seconds to minutes). A Redis key with a 24-hour TTL covers the vast majority of redeliveries at a fraction of the memory cost of storing every `event_id` forever.

**Read path**

- The in-memory `stats.Cache` already absorbs read load well. At 10k rps the cache would be written to frequently enough that it stays warm — no change needed there.
- If the `GET /accounts/{id}/stats` endpoint becomes a hotspot (e.g. dashboard polling), adding a short-lived Redis cache in front of `AccountStats` would offload Postgres reads.

**Horizontal scaling**

- The service is stateless except for the in-memory cache. Running multiple replicas behind a load balancer is straightforward, but each replica's cache becomes a partial view. At scale, the cache should either be dropped in favour of always reading from Postgres/Redis, or the cache should be promoted to a shared Redis hash so all replicas see the same numbers.
