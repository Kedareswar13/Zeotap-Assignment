# Architecture (Assignment 2)

## Data Flow

1. **Ingestion (Producer)**
   - `RecordReader` streams CSV/JSONL line-by-line.
   - Emits `Record(lineNumber, fields)`.

2. **Fan-out Orchestrator**
   - For each input record, enqueues a `DispatchItem(record, attempt=0)` into **each sink’s bounded queue**.
   - If any sink is slow, `put()` blocks => **backpressure** and constant memory.

3. **Sinks (Consumers)**
   - Each sink has:
     - A `Transformer` strategy to generate sink-specific payload.
     - A `RateLimiter` (permits/sec) to avoid overwhelming downstream.
     - Retry loop up to `maxRetries`.

4. **DLQ**
   - After max retries, failures are written to `dlq.jsonl`.

5. **Observability**
   - Every `statusIntervalSeconds`, prints processed + throughput + per-sink success/failure.
