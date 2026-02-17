# Assignment 2 - High-Throughput Fan-Out Engine (Java)

This project implements a **streaming fan-out + transformation engine**:

- Reads large flat files (CSV or JSONL) without loading them into memory.
- Fans out each record to multiple sinks in parallel.
- Applies sink-specific transformations (Strategy pattern).
- Enforces per-sink rate limits and bounded queues (backpressure).
- Retries failed deliveries (max 3) and writes failures to a DLQ (JSONL).
- Prints status every 5 seconds (throughput + per-sink success/fail counts).

## Requirements

- Java 21+
- No Maven install required (uses `mvnw.cmd` wrapper)

## Build

```powershell
.\mvnw.cmd -U package
```

Jar output:

- `target/fanout-engine-1.0.0.jar`

## Run

```powershell
java -Xmx512m -jar target\fanout-engine-1.0.0.jar --config .\config\config.json
```

### Run with JSONL input

Edit `config/config.json`:

- `input.path`: `./data/sample.jsonl`
- `input.format`: `JSONL`

Then run the same command.

## Configuration

Config file: `config/config.json`

- **input.path**: path to CSV/JSONL
- **input.format**: `CSV` or `JSONL`
- **runtime.queueCapacityPerSink**: bounded queue size per sink (backpressure)
- **runtime.workerThreads**: workers per sink. `0` = use `availableProcessors()`.
- **runtime.maxRetries**: max retry attempts per record per sink
- **runtime.statusIntervalSeconds**: metrics print interval
- **sinks[].rateLimitPerSecond**: per-sink throttling
- **sinks[].failureProbability**: mock failure rate to exercise retries/DLQ
- **dlq.path**: JSONL file for permanently failed records

## DLQ Output

Failures are written as JSON Lines to `dlq.path` with:

- sink name
- line number
- attempts
- error
- original record

## Notes

- On Java 21+ you may see warnings about "restricted method" due to internal native loading by dependencies.
- These are warnings only; the application still runs.

## Data Flow (Text Diagram)

`FileRecordReader (streaming)`
-> `Orchestrator`
-> `per-sink bounded queue`
-> `SinkWorker (rate limit + retries)`
-> `MockSink`

## Key Design Decisions

- **Backpressure**: each sink has a bounded `ArrayBlockingQueue`. The producer blocks when queues are full.
- **Concurrency**: each sink runs its own worker(s) using Java 21 virtual threads.
- **Extensibility**: adding a new sink only requires implementing `Sink` + `Transformer` and registering it in the factory; orchestrator stays unchanged.

## Prompts

See `Prompts.txt`.
