# caput
 trajectory collection + analysis pipeline.

Goal:

```text
collect 10k-50k finished token trajectories
↓
find historical edges
↓
build live LONG/SHORT signal engine
```

---

# SIMPLE MENTAL MODEL

There are only 3 important stages.

## 1. COLLECT

Collect live tokens + finished trajectories.

Files:

- official_api_collector.py
- fast_finished_collector.py

Result:

- token_snapshots
- official_api_token_state
- finished_tokens

---

## 2. ANALYZE

Convert trajectories into features + historical strategies.

Files:

- analyze.py
- paper_sim.py
- sweep.py
- clusters.py
- auto_run.py

Result:

- trajectory_features
- paper_trades
- strategy_sweep
- trajectory_clusters

---

## 3. LIVE SIGNALS (later)

Use historical edges on new live tokens.

Files:

- live_features.py
- live_signals.py
- telegram_bot.py

Result:

```text
LONG / SHORT / SKIP
risk
suggested leverage
historical setup match
```

---

# CURRENT STAGE

Right now the ONLY important thing is:

```text
collect as many finished trajectories as possible
```

Target:

```text
10k minimum
50k ideal
```

Do NOT focus on real trading yet.

---

# PROJECT STRUCTURE

## CORE FILES

### official_api_collector.py

Uses official public GraphQL API.

Collects:

- live token state
- price
- volume
- buys/sells
- start/end dates
- speed mode

Writes:

- token_snapshots
- official_api_token_state

Runs 24/7.

---

### fast_finished_collector.py

Browser collector using logged-in browser profile.

Purpose:

```text
collect finished trajectories
```

Only scans:

```text
expired + ready tokens
```

Writes:

- finished_tokens

Runs 24/7.

---

### auto_run.py

Runs:

```text
analyze.py
paper_sim.py
sweep.py
```

Use periodically.

---

### clusters.py

Groups token trajectories into similar behavior clusters.

Used later for:

```text
pattern matching
```

---

### telegram_bot.py

Monitoring only for now.

Later:

```text
live trading signals
```

---

# DATABASE TABLES

## token_snapshots

Live snapshots over time.

One token can create many rows.

```text
90k snapshots != 90k tokens
```

---

## official_api_token_state

Current latest state of each token.

One row per token.

---

## finished_tokens

Final completed trajectories.

This is the MOST IMPORTANT dataset.

---

## trajectory_features

Calculated statistics/features for trajectories.

---

## strategy_sweep

Historical strategy backtests.

---

# LOCAL VS RAILWAY

## RUN LOCALLY

These require browser session / Cloudflare bypass.

### local only

```text
fast_finished_collector.py
```

Reason:

```text
needs persistent logged-in browser profile
```

---

## RUN ON RAILWAY

Safe/stateless services.

### railway services

```text
official_api_collector.py
telegram_bot.py
```

Possible later:

```text
auto_run.py scheduler
```

---

# REQUIRED ENV

Copy:

```text
.env.example
→
.env
```

Important variables:

## required

```env
DATABASE_URL=
CATAPULT_API_KEY=
```

---

## browser collector

```env
LOCAL_PROFILE_DIR=browser_profile_fast2
FAST_FINISHED_READY_DELAY_SECONDS=90
FAST_FINISHED_USE_OFFICIAL_READY_QUEUE=true
FAST_FINISHED_ALLOW_SNAPSHOT_FALLBACK=false
```

---

# NORMAL OPERATION

## TERMINAL 1

```bat
python official_api_collector.py
```

---

## TERMINAL 2

```bat
python fast_finished_collector.py
```

---

## PERIODICALLY

```bat
python auto_run.py
python clusters.py
```

---

# HOW TO READ STATUS

## GOOD

```text
saved >> not_ready
```

Example:

```text
saved: 500
not_ready: 20
```

---

## BAD

```text
not_ready >> saved
```

Usually means:

```text
ready delay too low
```

Increase:

```env
FAST_FINISHED_READY_DELAY_SECONDS=120
```

---

# CURRENT SUCCESS CRITERIA

## GOOD DATASET

```text
10k finished trajectories
```

## VERY GOOD DATASET

```text
25k+
```

## PRODUCTION GRADE

```text
50k+
```

---

# FUTURE

Later the pipeline becomes:

```text
token appears
↓
live features
↓
historical pattern matching
↓
LONG / SHORT / SKIP
↓
risk + leverage suggestion
↓
telegram alert
```
