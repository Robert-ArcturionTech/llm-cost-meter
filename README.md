# llm-cost-meter

**What did last week's LLM usage cost, and which agent burned it?**

`llm-cost-meter` is a fleet-level LLM meter for multi-agent setups. It does not wrap or
instrument your calls. It reads the usage logs your agents already write (Claude Code
session logs, Codex sessions, a local-LLM router log, plain-text provider logs), loads them
into SQLite, and reports **cost, latency and tokens per agent and per model**, plus a small
set of **efficiency findings**. Pure Python standard library, no dependencies.

> Built by Robert Lingoes with AI coding agents (Claude Code / Codex); Robert owns the architecture, requirements and review.

## What it measures

| Metric | How | Honesty label |
|---|---|---|
| Tokens (input, output, cache read/write) | Counted from each source's own usage fields | Measured for Claude/Codex; provider-reported for text logs; **estimated** for the local router |
| Cost | Claude session logs only, via a prefix pricing table | **API-equivalent dollars**, not cash. Flat-rate plans do not bill per token, so this is leverage visibility |
| Latency | Per-call/turn latency where the source records it (avg and p95) | Claude session logs carry none, shown as `-` |
| Attribution | Agent = the folder after an `agents` marker in the call's working directory | Never inferred when evidence is missing; legacy rows stay `unknown` |

Codex, provider-log and local-router rows are deliberately **unpriced**: the meter reports their
tokens and latency but never invents a dollar figure.

### Efficiency findings

Read-only review signals, each `clear`, `attention` or `unavailable`. They never change routing,
block work, or send anything:

- **pipeline_freshness** - has an ingest completed recently?
- **router_attribution** - share of local-router calls with no explicit owner field
- **router_call_failures** - failed-call rate for the local router
- **model_policy_observation** - provider-log models outside your `allowed_models` list
- **context_pressure** - share of context-window events at or above 80% used
- **retry_waste** - repeated explicit correlation IDs (review evidence, not a waste claim)
- **automation_dispatch_attribution** - dispatch receipts carrying run/correlation evidence

`unavailable` means the telemetry needed to judge was never emitted. Nothing is guessed.

## Quickstart

Requires Python 3.9+ (developed and tested on 3.14).

```bash
git clone <this repo> && cd llm-cost-meter

# Ingest the bundled synthetic logs, then report. --as-of pins the window to the fixtures' dates.
python3 llm_cost_meter.py ingest --source examples/sample_logs --db .demo/meter.db
python3 llm_cost_meter.py report --db .demo/meter.db --as-of 2026-09-05T23:59:00Z
```

Against your own machine, with the defaults (`~/.claude/projects`, `~/.codex/sessions`):

```bash
python3 llm_cost_meter.py ingest     # incremental and idempotent; safe to run hourly
python3 llm_cost_meter.py report     # last 7 days
python3 llm_cost_meter.py report --days 30 --json
```

### Commands

| Command | Does |
|---|---|
| `ingest` | Incremental ingest of every configured source (byte offsets + dedupe keys; re-runs add nothing) |
| `report [--days N] [--json]` | Totals, per-agent and per-model tables, findings |
| `rollup` | Rebuild the `rollup_daily` table for the trailing window |
| `digest` | Write a one-line weekly digest to `digest.txt` |
| `efficiency` | Write `efficiency.json` and `efficiency.md` |
| `status` | Row counts, source paths, last ingest, per-source errors |
| `cron` | `ingest` + `rollup` + `digest` + `efficiency` (what a scheduler runs) |

Global flags (before or after the command): `--config FILE`, `--db FILE`, `--source DIR`, `--as-of ISO`.

## Configuration

Precedence: **defaults < config file < `LLM_METER_*` environment variables < CLI flags.**
The config file is `--config FILE`, `$LLM_METER_CONFIG`, or `~/.llm-cost-meter/config.json`
if present. Relative paths in it resolve against the file's folder. See
[`examples/config.example.json`](examples/config.example.json).

| Config key | Env var | Default |
|---|---|---|
| `db_path` | `LLM_METER_DB` (or `LLM_METER_HOME`/meter.db) | `~/.llm-cost-meter/meter.db` |
| `out_dir` | `LLM_METER_OUT_DIR` | the DB's folder |
| `sources.claude_projects` | `LLM_METER_CLAUDE_PROJECTS` | `~/.claude/projects` |
| `sources.codex_sessions` | `LLM_METER_CODEX_SESSIONS` | `~/.codex/sessions` |
| `sources.router_log` | `LLM_METER_ROUTER_LOG` | not set (skipped) |
| `sources.dispatch_log` | `LLM_METER_DISPATCH_LOG` | not set (skipped) |
| `sources.context_log` | `LLM_METER_CONTEXT_LOG` | not set (skipped) |
| `sources.provider_logs` (glob) | `LLM_METER_PROVIDER_LOGS` | not set (skipped) |
| `provider_log_timezone` | `LLM_METER_PROVIDER_TZ` | `UTC` (timezone of those logs' wall-clock stamps) |
| `agent_markers` | `LLM_METER_AGENT_MARKERS` (comma list) | `["agents"]` |
| `roster_agents` | - | `[]` (accept any well-formed id; set to restrict explicit labels) |
| `allowed_models` | `LLM_METER_ALLOWED_MODELS` (comma list) | `[]` (model-policy finding stays `unavailable`) |
| `codex_config` | `LLM_METER_CODEX_CONFIG` | Codex's own TOML config (read for the default model only) |
| `pricing` | - | built-in table, merged; `{"claude-sonnet": [in, out]}` USD per 1M tokens |
| `efficiency_policy` | - | override any threshold, e.g. `context_pressure_percent`, `minimum_sample_size` |

`--source DIR` points every source at one folder laid out like `examples/sample_logs`.

### Agent attribution

An agent is the folder directly after a marker in the working directory:
`/work/agents/planner/src` -> `planner`. With no marker present the last path component is used.
Change the markers with `agent_markers` if your agents live under, say, `fleet/` or `bots/`.

### Pricing

The built-in table is a **cached snapshot**; prices change. Verify against your provider and
override per prefix in config (longest prefix wins). Cache reads are priced at 0.1x input,
5-minute cache writes at 1.25x, 1-hour writes at 2x.

### Log formats

| Source | Format |
|---|---|
| `claude_projects` | Directory of `<project>/*.jsonl` Claude Code session logs; `assistant` records with `message.usage` |
| `codex_sessions` | Recursive `*.jsonl` Codex sessions; cumulative per-turn token records are upserted once per turn |
| `router_log` | JSONL, one call per line: `ts, provider, model, agent, tokens_in_est, tokens_out_est, latency_ms, ok`, optional `roster_agent, run_id, correlation_id` |
| `dispatch_log` | JSONL receipts: `ts, msg_id, actions`, optional `roster_agent, run_id, correlation_id` |
| `context_log` | JSONL: `ts, agent, model, session_id, used_percentage, compaction, ...` |
| `provider_logs` | Text lines `... INFO [session] ...: API call #N: model=M provider=P in=I out=O total=T latency=Ls [cache=C/I (P%)]`, one agent per folder |

Session-style sources accept `*.jsonl` and `*.ndjson`. The bundled fixtures use `.ndjson`/`.txt`.
Malformed lines are skipped; one failing source never blocks the others, and a failed ingest does
not refresh the freshness stamp.

## Sample report

From the command above, on the synthetic fixtures (all names and numbers are made up):

```text
llm-cost-meter report: 2026-08-29T23:59:00Z -> 2026-09-05T23:59:00Z (7d)

Fleet priced $0.63 (API-equivalent) over the last 7 days - top spender CODER at $0.40. 11 priced calls, 0.16M priced tokens. Codex: 2 measured turns, 0.02M tokens (subscription price not inferred). Provider logs: 6 reported calls, 0.06M tokens (price not inferred).

Totals: 31 calls | in 24,226 | out 27,576 | cache 191,720 | $0.6325 API-equivalent

By agent                        calls      input    output      cache   cost USD   avg ms   p95 ms
--------------------------------------------------------------------------------------------------
coder                               6      9,400    17,700     86,500     0.3971    7,000    9,800
planner                             5      3,110     5,010     40,500     0.2067    2,100    2,100
reviewer                            3      2,300     1,950     13,500     0.0287        -        -
researcher                          8      7,496     2,383     49,520     0.0000    4,211   16,100
ops                                 6      1,455       460      1,700     0.0000    1,660    3,300
unknown                             3        465        73          0     0.0000    1,003    1,350

By model                        calls      input    output      cache   cost USD   avg ms   p95 ms
--------------------------------------------------------------------------------------------------
claude-opus-4-8                     4      3,450     9,550     51,500     0.3577        -        -
claude-sonnet-4-6                   5      5,400    11,300     68,800     0.2677        -        -
claude-haiku-4-5                    2      1,200     1,050      6,700     0.0071        -        -
gpt-5.1                             5      4,587     2,195     51,220     0.0000    6,180   16,100
gpt-5.1-codex                       2      4,500     2,700     13,500     0.0000    7,000    9,800
kimi-k2                             1      3,000       400          0     0.0000    5,200    5,200
local/qwen-7b                       7      1,174       171          0     0.0000    1,000    1,350
local/qwen-14b                      5        915       210          0     0.0000    1,132    2,100

Findings (review signals, never actions):
  [attention] Local-router owner attribution: Have the router emit an explicit roster_agent field; historical rows without it stay unattributed.
  [attention] Local-router call failures: Inspect failed calls alongside correlation evidence; failed calls alone are not retry waste.
  [unavailable] Model-policy observation: Set `allowed_models` in the config to evaluate model-policy conformance.
  [attention] Context-window pressure: A review signal, not waste: inspect context composition before changing a window or compaction policy.

Cost is API-equivalent for Claude session logs only; codex, provider and router rows are unpriced (tokens/latency only).
```

## Scheduling

Run `cron` hourly. A macOS LaunchAgent template with placeholder paths is in
[`examples/launchd/`](examples/launchd/com.example.llm-cost-meter.plist); on Linux use cron:

```cron
0 * * * *  python3 /path/to/llm_cost_meter.py --config /path/to/config.json cron
```

## Privacy

The meter stores counts, token totals, timings, model names and agent names. It does not store
prompts, responses or tool arguments, and it makes no network calls.

## Tests

```bash
python3 -m unittest discover -s tests     # or: python3 -m pytest
```

The end-to-end test ingests the shipped sample logs and asserts exact totals.

## Layout

```text
llm_cost_meter.py        CLI, config, ingesters, rollup, report
llm_cost_efficiency.py   read-only efficiency scorecard
examples/sample_logs/    synthetic fixtures for every source
examples/config.example.json
examples/launchd/        optional LaunchAgent template
tests/
```

## License

MIT. See [LICENSE](LICENSE).
