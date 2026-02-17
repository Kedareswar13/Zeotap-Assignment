# Durable Execution Engine (Java)

This repository implements a **native durable execution engine** backed by SQLite.

## Requirements (Windows)

- **Java JDK**: 21+ (you already have Java installed)
- **No Maven install required** (this repo includes `mvnw.cmd` Maven Wrapper)

### Build tool (recommended): Maven Wrapper

This repo includes `mvnw.cmd`. It downloads Maven automatically the first time you build.

Verify Java:

```powershell
java -version
```

Build using wrapper:

```powershell
.\mvnw.cmd -DskipTests package
```

### Optional: Install Maven globally

Option A (recommended): Install via Chocolatey (PowerShell as Admin):

```powershell
choco install maven -y
```

Option B: Install manually

- Download: https://maven.apache.org/download.cgi
- Unzip
- Add `bin` folder to your `PATH`

Verify (if you installed Maven):

```powershell
java -version
mvn -v
```

## How it works

### Step primitive

Use `DurableContext.step(...)` to wrap any side-effecting operation.

Each step is persisted in SQLite (`steps` table) as:

- `workflow_id`
- `step_key` = `stepId#sequence`
- `status` (`RUNNING`, `COMPLETED`, `FAILED`)
- `output_json` + `output_class`
- `run_id` + `updated_at_ms`

If a step already has `COMPLETED` status for the same `workflow_id` + `step_key`, the engine returns the cached result and **does not re-run side effects**.

### Sequence tracking (loops / conditionals)

A step key is formed as:

`step_key = <fullStepId>#<sequence>`

- `fullStepId` is your `id` (optionally with a scope prefix).
- `sequence` is an auto-increment counter **per step id** inside the workflow execution.

This allows you to call the same step id multiple times in a loop and still uniquely identify each logical occurrence.

### Concurrency and thread safety

Parallel steps are supported (example uses `CompletableFuture`).

SQLite is protected by an internal `ReentrantLock` so concurrent threads do not corrupt writes and the engine avoids `SQLITE_BUSY` issues. SQLite is also configured with:

- WAL mode
- `busy_timeout=5000`

### Zombie step handling

If the process crashes after starting a step but before committing completion, the step stays in `RUNNING`.

When the workflow is resumed, a `RUNNING` step is only allowed to be taken over (re-run) if its `updated_at_ms` is older than a configurable `zombieTimeout` (default used by CLI: 10s). Otherwise the engine throws to prevent duplicating side effects.

## Run the example

From `Assignment 1` folder:

### 1) Build

```powershell
.\mvnw.cmd -DskipTests package
```

This produces a runnable fat jar:

- `app\target\app-1.0.0.jar`

### 2) Run normally

```bash
java -jar app/target/app-1.0.0.jar --workflowId onboarding-001 --employee Alice --db ./state.sqlite
```

If you previously crashed and see a RUNNING-step error, either:

- Wait for takeover: re-run with a higher `--zombieTimeoutMs` and wait that long
- Or force immediate takeover (demo-friendly): `--zombieTimeoutMs 0`

To start from scratch (delete workflow state from SQLite):

```bash
java -jar app/target/app-1.0.0.jar --reset true --workflowId onboarding-001 --employee Alice --db ./state.sqlite
```

### 3) Simulate a crash

Use one of these crash points:

- `after-create-record`
- `before-provision-laptop`
- `before-provision-access`
- `before-send-welcome-email`
- `after-workflow`

```bash
java -jar app/target/app-1.0.0.jar --workflowId onboarding-001 --employee Alice --db ./state.sqlite --crashAt before-send-welcome-email
```

### 4) Resume

Run again with the same `--workflowId` and `--db`:

```bash
java -jar app/target/app-1.0.0.jar --workflowId onboarding-001 --employee Alice --db ./state.sqlite
```

Completed steps will be skipped and only the remaining steps will execute.

## Notes

- Prefer using `step(id, Class<T>, fn)` for type-safe replay.
- Use `ctx.scoped("...")` to namespace step ids in parallel branches.
