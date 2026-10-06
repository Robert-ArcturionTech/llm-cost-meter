#!/usr/bin/env python3
"""llm-cost-meter: fleet LLM cost / latency / token telemetry from logs you already write.

Ingest-only design: nothing here instruments a live call path. It reads the usage
logs a multi-agent setup already produces and loads them into SQLite.

Sources (each optional; configure only what you have):
  sessions   Claude Code session logs (~/.claude/projects/*/*.jsonl). Real token
             usage per assistant message. Cost is API-EQUIVALENT dollars from the
             pricing table below, not cash (subscription plans are flat-rate).
  codex      Codex session logs (~/.codex/sessions/**/*.jsonl). Measured cumulative
             tokens, model, effort and latency per turn. Never priced.
  router     Local-LLM router JSONL (one JSON object per call). $0, latency+volume;
             tokens are the router's estimates.
  provider   Plain-text provider-call logs (one agent per folder). Provider-reported
             tokens and latency. Never priced.
  dispatch   Orchestrator dispatch receipts (JSONL), for attribution evidence.
  context    Context-window telemetry (JSONL), for pressure findings.

Commands:
  ingest      incremental, idempotent ingest of every configured source into SQLite
  rollup      rebuild daily aggregates for the trailing window
  report      weekly cost / latency / tokens per agent and per model + findings
  digest      write a one-line weekly digest
  efficiency  write the read-only efficiency scorecard (JSON + Markdown)
  status      print row counts, source health and last ingest
  cron        ingest + rollup + digest + efficiency (what a scheduler should run)

Configuration precedence: defaults < config file < LLM_METER_* env vars < CLI flags.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from llm_cost_efficiency import (
    POLICY as EFFICIENCY_POLICY,
    build_scorecard,
    load_codex_policy,
    provider_policy_from_models,
    write_scorecard,
)

# Layout of a `--source ROOT` directory (see examples/sample_logs).
SOURCE_LAYOUT = {
    "claude_projects": "claude_projects",
    "codex_sessions": "codex_sessions",
    "router_log": "router.ndjson",
    "dispatch_log": "dispatch.ndjson",
    "context_log": "context.ndjson",
    "provider_logs": "provider_logs/*/agent.txt",
}
# Session logs are matched by these extensions (.ndjson is plain newline-delimited JSON).
JSON_LINES_GLOBS = ("*.jsonl", "*.ndjson")

# Provider-call summary lines, e.g.
#   2026-09-04 09:01:57,485 INFO [sess_1] agent.loop: API call #5: model=m provider=p
#   in=44607 out=765 total=45372 latency=16.1s cache=43520/44607 (98%)
# Cache is reported separately so fresh input + cache + output still equals the total.
PROVIDER_CALL_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+INFO\s+"
    r"\[(?P<session>[^\]]+)\].*?API call #(?P<call>\d+):\s+"
    r"model=(?P<model>\S+)\s+provider=(?P<provider>\S+)\s+"
    r"in=(?P<input>\d+)\s+out=(?P<output>\d+)\s+total=(?P<total>\d+)\s+"
    r"latency=(?P<latency>[\d.]+)s"
    r"(?:\s+cache=(?P<cache_read>\d+)/\d+\s+\(\d+%\))?"
)

# Pricing (USD per 1M tokens). A cached snapshot: verify against your provider's
# current price list and override per prefix via `pricing` in the config file.
# cache_read = 0.1x input; cache_write 5m = 1.25x input, 1h = 2x input.
DEFAULT_PRICING = {  # prefix match on model id -> (input, output); longest prefix wins
    "claude-fable-5": (10.00, 50.00),
    "claude-opus-4-1": (15.00, 75.00),
    "claude-opus-4-2": (15.00, 75.00),
    "claude-opus-4-0": (15.00, 75.00),
    "claude-opus-4-20": (15.00, 75.00),  # e.g. claude-opus-4-20250514
    "claude-opus": (5.00, 25.00),
    "claude-sonnet": (3.00, 15.00),
    "claude-haiku-4": (1.00, 5.00),
    "claude-3-5-haiku": (0.80, 4.00),
    "claude-3-haiku": (0.25, 1.25),
    "claude": (5.00, 25.00),  # unknown claude model -> opus rates
}

TELEMETRY_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS llm_calls (
  id            INTEGER PRIMARY KEY,
  ts            TEXT NOT NULL,          -- ISO8601 UTC
  agent         TEXT NOT NULL,
  source        TEXT NOT NULL,          -- sessions | codex | provider | router
  model         TEXT NOT NULL,
  input_tokens  INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read    INTEGER NOT NULL DEFAULT 0,
  cache_write   INTEGER NOT NULL DEFAULT 0,
  cost_usd      REAL NOT NULL DEFAULT 0,
  latency_ms    REAL,
  task          TEXT,
  roster_agent  TEXT,                  -- explicit only; historical rows stay NULL
  run_id        TEXT,                  -- explicit normalized execution identity
  correlation_id TEXT,                 -- explicit retry/causality identity
  ok            INTEGER NOT NULL DEFAULT 1,
  in_totals     INTEGER NOT NULL DEFAULT 1,  -- 0 = cross-check row, excluded from totals
  dedupe_key    TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_calls_ts ON llm_calls(ts);
CREATE INDEX IF NOT EXISTS idx_calls_agent_ts ON llm_calls(agent, ts);
CREATE INDEX IF NOT EXISTS idx_calls_correlation ON llm_calls(correlation_id);

CREATE TABLE IF NOT EXISTS dispatch_events (
  id             INTEGER PRIMARY KEY,
  ts             TEXT NOT NULL,
  roster_agent   TEXT,
  run_id         TEXT,
  correlation_id TEXT,
  msg_id         TEXT,
  actions_json   TEXT,
  dedupe_key     TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_dispatch_events_ts ON dispatch_events(ts);
CREATE INDEX IF NOT EXISTS idx_dispatch_events_correlation ON dispatch_events(correlation_id);

-- Codex behavior metadata is kept apart from token accounting. It stores counts
-- and timings only: no prompts, responses, tool arguments, or secrets.
CREATE TABLE IF NOT EXISTS codex_turns (
  session_id TEXT NOT NULL,
  turn_id TEXT NOT NULL,
  agent TEXT NOT NULL,
  model TEXT NOT NULL,
  reasoning_effort TEXT,
  duration_ms REAL,
  time_to_first_token_ms REAL,
  tool_calls INTEGER NOT NULL DEFAULT 0,
  assistant_messages INTEGER NOT NULL DEFAULT 0,
  subagents_spawned INTEGER NOT NULL DEFAULT 0,
  completed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (session_id, turn_id)
);
CREATE INDEX IF NOT EXISTS idx_codex_turns_agent ON codex_turns(agent);

CREATE TABLE IF NOT EXISTS file_state (
  path   TEXT PRIMARY KEY,
  offset INTEGER NOT NULL DEFAULT 0,
  mtime  REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rollup_daily (
  day    TEXT NOT NULL,
  agent  TEXT NOT NULL,
  model  TEXT NOT NULL,
  source TEXT NOT NULL,
  calls  INTEGER NOT NULL,
  input_tokens  INTEGER NOT NULL,
  output_tokens INTEGER NOT NULL,
  cache_read    INTEGER NOT NULL,
  cache_write   INTEGER NOT NULL,
  cost_usd      REAL NOT NULL,
  avg_latency_ms REAL,
  PRIMARY KEY (day, agent, model, source)
);

CREATE TABLE IF NOT EXISTS context_events (
  id                         INTEGER PRIMARY KEY,
  ts                         TEXT NOT NULL,
  agent                      TEXT NOT NULL,
  harness                    TEXT NOT NULL,
  model                      TEXT NOT NULL,
  session_id                 TEXT NOT NULL,
  event                      TEXT NOT NULL,
  effective_window_tokens    INTEGER,
  protected_identity_tokens  INTEGER,
  working_context_tokens     INTEGER,
  used_percentage            REAL,
  band                       TEXT,
  brain_injection_tokens     INTEGER,
  tool_output_tokens         INTEGER,
  checkpoint_status          TEXT,
  compaction                 INTEGER NOT NULL DEFAULT 0,
  rehydration_status         TEXT,
  reduced_working_memory     INTEGER NOT NULL DEFAULT 0,
  mode                       TEXT NOT NULL,
  dedupe_key                 TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_context_events_ts ON context_events(ts);
CREATE INDEX IF NOT EXISTS idx_context_events_agent_ts ON context_events(agent, ts);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


# -- Configuration -----------------------------------------------------------

def _path(value, base: Path | None = None) -> Path | None:
    if value in (None, ""):
        return None
    p = Path(os.path.expanduser(str(value)))
    if not p.is_absolute() and base is not None:
        p = base / p
    return p


@dataclass
class Config:
    db_path: Path
    out_dir: Path | None = None          # default: the DB's folder
    claude_projects: Path | None = None
    codex_sessions: Path | None = None
    router_log: Path | None = None
    dispatch_log: Path | None = None
    context_log: Path | None = None
    provider_logs: str | None = None     # glob, one agent per folder
    provider_log_timezone: str = "UTC"
    agent_markers: tuple = ("agents",)
    roster_agents: frozenset = frozenset()   # empty = accept any well-formed id
    pricing: dict = field(default_factory=lambda: dict(DEFAULT_PRICING))
    allowed_models: tuple = ()
    codex_config: Path | None = None
    efficiency_policy: dict = field(default_factory=dict)

    @property
    def output_dir(self) -> Path:
        return self.out_dir or self.db_path.parent

    @property
    def digest_path(self) -> Path:
        return self.output_dir / "digest.txt"

    @property
    def scorecard_json(self) -> Path:
        return self.output_dir / "efficiency.json"

    @property
    def scorecard_md(self) -> Path:
        return self.output_dir / "efficiency.md"


DEFAULT_HOME = Path.home() / ".llm-cost-meter"


def load_config(config_file: str | None = None, db: str | None = None,
                source_root: str | None = None, env: dict | None = None) -> Config:
    """Resolve configuration: defaults < config file < env vars < CLI flags."""
    env = os.environ if env is None else env
    cfg = Config(
        db_path=_path(env.get("LLM_METER_HOME") or DEFAULT_HOME) / "meter.db",
        claude_projects=Path.home() / ".claude" / "projects",
        codex_sessions=Path.home() / ".codex" / "sessions",
        codex_config=Path.home() / ".codex" / "config.toml",
    )

    cfg_path = _path(config_file or env.get("LLM_METER_CONFIG"))
    if cfg_path is None and (DEFAULT_HOME / "config.json").is_file():
        cfg_path = DEFAULT_HOME / "config.json"
    if cfg_path is not None:
        raw = json.loads(cfg_path.read_text())
        base = cfg_path.resolve().parent
        src = raw.get("sources") or {}
        if "db_path" in raw:
            cfg.db_path = _path(raw["db_path"], base)
        if "out_dir" in raw:
            cfg.out_dir = _path(raw["out_dir"], base)
        for key in ("claude_projects", "codex_sessions", "router_log",
                    "dispatch_log", "context_log"):
            if key in src:
                setattr(cfg, key, _path(src[key], base))
        if "provider_logs" in src:
            pl = src["provider_logs"]
            cfg.provider_logs = str(_path(pl, base)) if pl else None
        if "codex_config" in raw:
            cfg.codex_config = _path(raw["codex_config"], base)
        cfg.provider_log_timezone = raw.get("provider_log_timezone", cfg.provider_log_timezone)
        cfg.agent_markers = tuple(raw.get("agent_markers", cfg.agent_markers))
        cfg.roster_agents = frozenset(a.lower() for a in raw.get("roster_agents", ()))
        cfg.allowed_models = tuple(raw.get("allowed_models", ()))
        cfg.pricing = {**cfg.pricing, **{k: tuple(v) for k, v in (raw.get("pricing") or {}).items()}}
        cfg.efficiency_policy = dict(raw.get("efficiency_policy") or {})

    env_map = {
        "LLM_METER_DB": "db_path", "LLM_METER_OUT_DIR": "out_dir",
        "LLM_METER_CLAUDE_PROJECTS": "claude_projects",
        "LLM_METER_CODEX_SESSIONS": "codex_sessions",
        "LLM_METER_ROUTER_LOG": "router_log", "LLM_METER_DISPATCH_LOG": "dispatch_log",
        "LLM_METER_CONTEXT_LOG": "context_log", "LLM_METER_CODEX_CONFIG": "codex_config",
    }
    for var, attr in env_map.items():
        if env.get(var):
            setattr(cfg, attr, _path(env[var]))
    if env.get("LLM_METER_PROVIDER_LOGS"):
        cfg.provider_logs = os.path.expanduser(env["LLM_METER_PROVIDER_LOGS"])
    if env.get("LLM_METER_PROVIDER_TZ"):
        cfg.provider_log_timezone = env["LLM_METER_PROVIDER_TZ"]
    if env.get("LLM_METER_AGENT_MARKERS"):
        cfg.agent_markers = tuple(m for m in env["LLM_METER_AGENT_MARKERS"].split(",") if m)
    if env.get("LLM_METER_ALLOWED_MODELS"):
        cfg.allowed_models = tuple(m for m in env["LLM_METER_ALLOWED_MODELS"].split(",") if m)

    if source_root:
        root = _path(source_root)
        for attr, rel in SOURCE_LAYOUT.items():
            value = root / rel
            setattr(cfg, attr, str(value) if attr == "provider_logs" else value)
    if db:
        cfg.db_path = _path(db)
    return cfg


# -- Core helpers ------------------------------------------------------------

def connect(cfg: Config) -> sqlite3.Connection:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cfg.db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


def price_for(model: str, pricing: dict | None = None):
    pricing = pricing or DEFAULT_PRICING
    m = (model or "").lower()
    for prefix in sorted(pricing, key=len, reverse=True):  # longest prefix wins
        if m.startswith(prefix):
            return pricing[prefix]
    return (0.0, 0.0)  # non-claude / local -> no API-equivalent cost


def claude_cost(model: str, inp: int, out: int, cr: int, cw_5m: int, cw_1h: int,
                pricing: dict | None = None) -> float:
    p_in, p_out = price_for(model, pricing)
    return (
        inp * p_in + out * p_out + cr * 0.1 * p_in
        + cw_5m * 1.25 * p_in + cw_1h * 2.0 * p_in
    ) / 1_000_000


def agent_from_cwd(cwd: str, markers=("agents",)) -> str:
    """Attribute a call to an agent from the working directory.

    The agent is the folder directly after the first marker folder
    (``/work/agents/planner/src`` -> ``planner``). Without a marker, the last
    path component is used; an empty cwd is ``unknown``.
    """
    parts = Path(cwd or "").parts
    for i, part in enumerate(parts):
        if part in markers and i + 1 < len(parts):
            return parts[i + 1].lower()
    if parts and parts[-1] != "/":
        return parts[-1].lower()
    return "unknown"


def _iso_utc(ts: str) -> str:
    """Normalize any ISO timestamp to UTC ISO8601 (seconds precision)."""
    if not ts:
        return ""
    try:
        s = ts.strip().replace("Z", "+00:00")
        # Some loggers emit RFC-822 offsets (-0400); older fromisoformat rejects them.
        if re.search(r"[+-]\d{4}$", s):
            s = s[:-2] + ":" + s[-2:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return ""


def _explicit_roster_agent(value, roster: frozenset = frozenset()) -> str | None:
    """Accept an explicit roster label; never derive one from legacy data."""
    candidate = str(value or "").strip().lower()
    if not TELEMETRY_ID_RE.fullmatch(candidate):
        return None
    if roster and candidate not in roster:
        return None
    return candidate


def _explicit_telemetry_id(value) -> str | None:
    """Accept only an emitted normalized identity; missing stays unavailable."""
    candidate = str(value or "").strip().lower()
    return candidate if TELEMETRY_ID_RE.fullmatch(candidate) else None


def _local_iso_utc(ts: str, tz_name: str) -> str:
    """Normalize a local-wall-time log timestamp using the configured timezone."""
    try:
        local = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S,%f")
        return local.replace(tzinfo=ZoneInfo(tz_name)).astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except (TypeError, ValueError):
        return ""


# -- Incremental file reader -------------------------------------------------

def _stored_offset(conn, path: Path) -> int:
    row = conn.execute("SELECT offset FROM file_state WHERE path=?", (str(path),)).fetchone()
    return row[0] if row else 0


def _new_lines(conn, path: Path | None):
    """Eagerly read complete lines appended since the stored offset.

    Returns (lines, consumed_offset, mtime); (..., None, None) means nothing new.
    The caller MUST call _commit_offset() after processing succeeds.
    """
    if path is None:
        return [], None, None
    try:
        st = path.stat()
    except OSError:
        return [], None, None
    offset = _stored_offset(conn, path)
    if st.st_size < offset:  # truncated/rotated
        offset = 0
    if st.st_size == offset:
        return [], None, None
    with path.open("rb") as f:
        f.seek(offset)
        data = f.read()
    end = data.rfind(b"\n")
    if end == -1:  # only a partial trailing line so far
        return [], None, None
    consumed = offset + end + 1
    lines = [raw.decode("utf-8", errors="ignore").strip() for raw in data[: end + 1].splitlines()]
    return [ln for ln in lines if ln], consumed, st.st_mtime


def _commit_offset(conn, path: Path, consumed: int, mtime: float) -> None:
    conn.execute(
        "INSERT INTO file_state(path, offset, mtime) VALUES(?,?,?) "
        "ON CONFLICT(path) DO UPDATE SET offset=excluded.offset, mtime=excluded.mtime",
        (str(path), consumed, mtime),
    )


# -- Ingesters ---------------------------------------------------------------

def ingest_sessions(conn, cfg: Config) -> int:
    """Claude Code session logs: ground truth for subscription traffic."""
    n = 0
    root = cfg.claude_projects
    if root is None or not root.exists():
        return 0
    for proj in sorted(p for p in root.iterdir() if p.is_dir()):
        for fn in sorted(f for pat in JSON_LINES_GLOBS for f in proj.glob(pat)):
            lines, consumed, mtime = _new_lines(conn, fn)
            for line in lines:
                if '"usage"' not in line:
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("type") != "assistant":
                        continue
                    msg = rec.get("message") or {}
                    usage = msg.get("usage") or {}
                    if not usage:
                        continue
                    model = msg.get("model") or "unknown"
                    if model == "<synthetic>":
                        continue
                    inp = int(usage.get("input_tokens") or 0)
                    out = int(usage.get("output_tokens") or 0)
                    cr = int(usage.get("cache_read_input_tokens") or 0)
                    cw = int(usage.get("cache_creation_input_tokens") or 0)
                    cc = usage.get("cache_creation") or {}
                    cw_1h = int(cc.get("ephemeral_1h_input_tokens") or 0)
                    cw_5m = int(cc.get("ephemeral_5m_input_tokens") or 0)
                    if cw_1h + cw_5m == 0:
                        cw_5m = cw  # older records: no breakdown -> assume 5m
                    key = "sess:" + (rec.get("requestId") or rec.get("uuid") or "")
                    if key == "sess:":
                        continue
                    ts = _iso_utc(rec.get("timestamp") or "")
                    if not ts:
                        continue
                    cost = claude_cost(model, inp, out, cr, cw_5m, cw_1h, cfg.pricing)
                    conn.execute(
                        "INSERT INTO llm_calls(ts, agent, source, model, input_tokens,"
                        " output_tokens, cache_read, cache_write, cost_usd, latency_ms,"
                        " task, ok, in_totals, dedupe_key)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (ts, agent_from_cwd(rec.get("cwd") or "", cfg.agent_markers),
                         "sessions", model, inp, out, cr, cw_1h + cw_5m, cost, None,
                         rec.get("entrypoint") or "", 1, 1, key),
                    )
                    n += 1
                except sqlite3.IntegrityError:
                    pass  # duplicate requestId (retries, streamed partials)
                except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
                    continue  # malformed record: skip, never abort the ingest
            if consumed is not None:
                _commit_offset(conn, fn, consumed, mtime)
    return n


def _codex_file_context(path: Path) -> tuple[dict, dict, dict, dict]:
    """Read the small routing index for one Codex session without retaining prose.

    Token records are append-only and cumulative per turn. The index lets a later
    token record inherit the turn's model and effort even when its turn_context
    record was ingested in an earlier run.
    """
    session, turns, completions, behavior = {}, {}, {}, {}
    try:
        lines = path.read_text(errors="ignore").splitlines()
    except OSError:
        return session, turns, completions, behavior
    current_turn = None
    for line in lines:
        try:
            rec = json.loads(line)
            payload = rec.get("payload") or {}
            kind = rec.get("type")
            if kind == "session_meta":
                session = payload
            elif kind == "turn_context" and payload.get("turn_id"):
                current_turn = str(payload["turn_id"])
                turns[current_turn] = payload
                behavior.setdefault(current_turn, {"tool_calls": 0, "assistant_messages": 0,
                                                   "subagents_spawned": 0})
            elif kind == "event_msg" and payload.get("turn_id") and payload.get("type") == "task_complete":
                completions[str(payload["turn_id"])] = payload
            elif kind == "response_item" and current_turn:
                item_type = payload.get("type")
                if item_type in {"function_call", "custom_tool_call", "web_search_call"}:
                    behavior[current_turn]["tool_calls"] += 1
                elif item_type == "message" and payload.get("role") == "assistant":
                    behavior[current_turn]["assistant_messages"] += 1
            elif kind == "inter_agent_communication_metadata" and current_turn:
                behavior[current_turn]["subagents_spawned"] += 1
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
    return session, turns, completions, behavior


def ingest_codex_sessions(conn, cfg: Config) -> int:
    """Codex sessions: real cumulative token usage, never estimates.

    A turn emits several cumulative usage records while it runs. One row is kept
    per session/turn and only moves forward when the measured total grows, so
    streamed progress records are never counted twice.
    """
    n = 0
    root = cfg.codex_sessions
    if root is None or not root.exists():
        return 0
    for fn in sorted(f for pat in JSON_LINES_GLOBS for f in root.rglob(pat)):
        lines, consumed, mtime = _new_lines(conn, fn)
        if not lines:
            continue
        session, turns, completions, behavior = _codex_file_context(fn)
        session_id = str(session.get("session_id") or session.get("id") or fn.stem)
        for line in lines:
            try:
                rec = json.loads(line)
                if rec.get("type") != "token_usage_record":
                    continue
                payload = rec.get("payload") or {}
                turn_id = str(payload.get("turn_id") or "")
                usage = payload.get("turn_token_usage") or payload.get("usage") or {}
                total = int(usage.get("total_tokens") or 0)
                if not turn_id or total <= 0:
                    continue
                context = turns.get(turn_id) or {}
                complete = completions.get(turn_id) or {}
                counts = behavior.get(turn_id) or {}
                cached = int(usage.get("cached_input_tokens") or 0)
                cache_write = int(usage.get("cache_write_input_tokens") or 0)
                raw_input = int(usage.get("input_tokens") or 0)
                # Codex's input total includes cached input. Keep the invariant:
                # fresh input + cache columns = total input tokens.
                fresh_input = max(raw_input - cached - cache_write, 0)
                ts = _iso_utc(rec.get("timestamp") or complete.get("completed_at") or "")
                if not ts:
                    continue
                task = f"effort={context.get('effort') or 'unknown'}"
                if complete.get("time_to_first_token_ms") is not None:
                    task += f";ttft_ms={complete['time_to_first_token_ms']}"
                key = f"codex:{session_id}:{turn_id}"
                agent = agent_from_cwd(context.get("cwd") or session.get("cwd") or "", cfg.agent_markers)
                conn.execute(
                    "INSERT INTO llm_calls(ts, agent, source, model, input_tokens,"
                    " output_tokens, cache_read, cache_write, cost_usd, latency_ms,"
                    " task, ok, in_totals, dedupe_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(dedupe_key) DO UPDATE SET"
                    " ts=excluded.ts, input_tokens=excluded.input_tokens,"
                    " output_tokens=excluded.output_tokens, cache_read=excluded.cache_read,"
                    " cache_write=excluded.cache_write, latency_ms=excluded.latency_ms,"
                    " task=excluded.task, model=excluded.model"
                    " WHERE (excluded.input_tokens + excluded.output_tokens + excluded.cache_read + excluded.cache_write)"
                    " >= (llm_calls.input_tokens + llm_calls.output_tokens + llm_calls.cache_read + llm_calls.cache_write)",
                    (ts, agent, "codex", context.get("model") or "unknown", fresh_input,
                     int(usage.get("output_tokens") or 0), cached, cache_write,
                     0.0, complete.get("duration_ms"), task, 1, 1, key),
                )
                conn.execute(
                    "INSERT INTO codex_turns(session_id, turn_id, agent, model, reasoning_effort,"
                    " duration_ms, time_to_first_token_ms, tool_calls, assistant_messages,"
                    " subagents_spawned, completed) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(session_id, turn_id) DO UPDATE SET"
                    " agent=excluded.agent, model=excluded.model, reasoning_effort=excluded.reasoning_effort,"
                    " duration_ms=excluded.duration_ms, time_to_first_token_ms=excluded.time_to_first_token_ms,"
                    " tool_calls=excluded.tool_calls, assistant_messages=excluded.assistant_messages,"
                    " subagents_spawned=excluded.subagents_spawned, completed=excluded.completed",
                    (session_id, turn_id, agent, context.get("model") or "unknown",
                     context.get("effort") or "unknown", complete.get("duration_ms"),
                     complete.get("time_to_first_token_ms"),
                     int(counts.get("tool_calls") or 0), int(counts.get("assistant_messages") or 0),
                     int(counts.get("subagents_spawned") or 0), 1 if complete else 0),
                )
                n += 1
            except (sqlite3.IntegrityError, json.JSONDecodeError, ValueError, TypeError, AttributeError):
                continue
        if consumed is not None:
            _commit_offset(conn, fn, consumed, mtime)
    return n


def _provider_logs(cfg: Config) -> list[tuple[str, Path, Path]]:
    """Resolve (agent, file, canonical_file) for every provider log, incl. rotated copies."""
    if not cfg.provider_logs:
        return []
    out = []
    for active in sorted(glob.glob(cfg.provider_logs)):
        fn = Path(active)
        if not fn.is_file():
            continue
        folder = fn.parent.parent if fn.parent.name == "logs" else fn.parent
        agent = folder.name.lower()
        # Recover retained rotated logs after a stalled collector. Dedupe against
        # the canonical active path so a rotated copy never counts twice.
        for part in [*sorted(fn.parent.glob(fn.name + ".[0-9]*")), fn]:
            out.append((agent, part, fn))
    return out


def ingest_provider_logs(conn, cfg: Config) -> int:
    """Successful provider-call summaries from plain-text agent logs.

    Provider-reported totals, not independently billed usage; unpriced.
    """
    n = 0
    for agent, fn, canonical in _provider_logs(cfg):
        lines, consumed, mtime = _new_lines(conn, fn)
        for line in lines:
            match = PROVIDER_CALL_RE.match(line)
            if not match:
                continue
            fields = match.groupdict()
            ts = _local_iso_utc(fields["ts"], cfg.provider_log_timezone)
            if not ts:
                continue
            try:
                total = int(fields["total"])
                raw_input = int(fields["input"])
                output = int(fields["output"])
                cache_read = int(fields.get("cache_read") or 0)
                # A valid record always reports total = input + output. Reject
                # malformed/partial lines instead of overstating usage.
                if total != raw_input + output or cache_read > raw_input:
                    continue
                dedupe = "prov:" + hashlib.sha1(
                    (str(canonical) + "\n" + line).encode("utf-8")
                ).hexdigest()
                conn.execute(
                    "INSERT INTO llm_calls(ts, agent, source, model, input_tokens,"
                    " output_tokens, cache_read, cache_write, cost_usd, latency_ms,"
                    " task, ok, in_totals, dedupe_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ts, agent, "provider", fields["model"], raw_input - cache_read, output,
                     cache_read, 0, 0.0, round(float(fields["latency"]) * 1000, 3),
                     f"provider={fields['provider']};session={fields['session']}",
                     1, 1, dedupe),
                )
                n += 1
            except (sqlite3.IntegrityError, TypeError, ValueError):
                continue
        if consumed is not None:
            _commit_offset(conn, fn, consumed, mtime)
    return n


def ingest_router(conn, cfg: Config) -> int:
    """Local-LLM router telemetry: local models, $0, but latency + volume."""
    n = 0
    lines, consumed, mtime = _new_lines(conn, cfg.router_log)
    for line in lines:
        try:
            rec = json.loads(line)
            ts = _iso_utc(rec.get("ts") or "")
            if not ts:
                continue
            key = "rt:" + hashlib.sha1(line.encode()).hexdigest()
            provider = rec.get("provider") or "local"
            model = rec.get("model") or "unknown"
            conn.execute(
                "INSERT INTO llm_calls(ts, agent, source, model, input_tokens,"
                " output_tokens, cache_read, cache_write, cost_usd, latency_ms,"
                " task, roster_agent, run_id, correlation_id, ok, in_totals, dedupe_key)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ts, (rec.get("agent") or "unknown").lower(), "router",
                 f"{provider}/{model}",
                 int(rec.get("tokens_in_est") or 0), int(rec.get("tokens_out_est") or 0),
                 0, 0, 0.0, rec.get("latency_ms"),
                 rec.get("task") or "",
                 _explicit_roster_agent(rec.get("roster_agent"), cfg.roster_agents),
                 _explicit_telemetry_id(rec.get("run_id")),
                 _explicit_telemetry_id(rec.get("correlation_id")),
                 1 if rec.get("ok") else 0, 1, key),
            )
            n += 1
        except sqlite3.IntegrityError:
            pass
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            continue
    if consumed is not None:
        _commit_offset(conn, cfg.router_log, consumed, mtime)
    return n


def ingest_dispatch(conn, cfg: Config) -> int:
    """Passive dispatch receipts for automation attribution.

    Only fields present on the receipt are kept; older records are stored with
    NULL attribution evidence rather than guessed.
    """
    n = 0
    lines, consumed, mtime = _new_lines(conn, cfg.dispatch_log)
    for line in lines:
        try:
            rec = json.loads(line)
            ts = _iso_utc(rec.get("ts") or "")
            if not ts:
                continue
            key = "dispatch:" + hashlib.sha1(line.encode()).hexdigest()
            conn.execute(
                "INSERT INTO dispatch_events(ts,roster_agent,run_id,correlation_id,msg_id,actions_json,dedupe_key) "
                "VALUES(?,?,?,?,?,?,?)",
                (ts, _explicit_roster_agent(rec.get("roster_agent"), cfg.roster_agents),
                 _explicit_telemetry_id(rec.get("run_id")),
                 _explicit_telemetry_id(rec.get("correlation_id")),
                 rec.get("msg_id"), json.dumps(rec.get("actions") or []), key),
            )
            n += 1
        except sqlite3.IntegrityError:
            pass
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            continue
    if consumed is not None:
        _commit_offset(conn, cfg.dispatch_log, consumed, mtime)
    return n


def ingest_context(conn, cfg: Config) -> int:
    """Context-window telemetry. Counts and states only, never prompts."""
    n = 0
    lines, consumed, mtime = _new_lines(conn, cfg.context_log)
    for line in lines:
        try:
            rec = json.loads(line)
            ts = _iso_utc(rec.get("ts") or "")
            if not ts:
                continue
            key = "ctx:" + hashlib.sha1(line.encode()).hexdigest()
            conn.execute(
                "INSERT INTO context_events(ts,agent,harness,model,session_id,event,"
                "effective_window_tokens,protected_identity_tokens,working_context_tokens,"
                "used_percentage,band,brain_injection_tokens,tool_output_tokens,"
                "checkpoint_status,compaction,rehydration_status,reduced_working_memory,"
                "mode,dedupe_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ts, (rec.get("agent") or "unknown").lower(),
                 rec.get("harness") or "unknown", rec.get("model") or "unknown",
                 str(rec.get("session_id") or "unknown"), rec.get("event") or "unknown",
                 rec.get("effective_window_tokens"), rec.get("protected_identity_tokens"),
                 rec.get("working_context_tokens"), rec.get("used_percentage"),
                 rec.get("band"), rec.get("brain_injection_tokens"),
                 rec.get("tool_output_tokens"), rec.get("checkpoint_status"),
                 1 if rec.get("compaction") else 0, rec.get("rehydration_status"),
                 1 if rec.get("reduced_working_memory") else 0,
                 rec.get("mode") or "shadow", key),
            )
            n += 1
        except sqlite3.IntegrityError:
            pass
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            continue
    if consumed is not None:
        _commit_offset(conn, cfg.context_log, consumed, mtime)
    return n


INGESTERS = (
    ("sessions", ingest_sessions),
    ("codex", ingest_codex_sessions),
    ("provider", ingest_provider_logs),
    ("router", ingest_router),
    ("dispatch", ingest_dispatch),
    ("context", ingest_context),
)


def ingest(conn, cfg: Config) -> dict:
    t0 = time.time()
    counts: dict = {}
    # Commit per source so one failing source never rolls back the others.
    for name, fn in INGESTERS:
        try:
            counts[name] = fn(conn, cfg)
            conn.commit()
        except Exception as e:  # noqa: BLE001 - isolate per-source failures
            conn.rollback()
            counts[name] = f"error: {type(e).__name__}: {e}"

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    errors = {name: value for name, value in counts.items() if isinstance(value, str)}
    conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('last_ingest_attempt',?)", (stamp,))
    conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('last_ingest_errors',?)", (json.dumps(errors),))
    if not errors:
        conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('last_ingest',?)", (stamp,))
    counts["ok"] = not errors
    conn.commit()
    counts["secs"] = round(time.time() - t0, 1)
    return counts


# -- Rollup, digest, report --------------------------------------------------

def _now(as_of: str | None) -> datetime:
    if as_of:
        stamp = _iso_utc(as_of)
        if not stamp:
            raise SystemExit(f"--as-of: cannot parse {as_of!r} as an ISO timestamp")
        return datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    return datetime.now(timezone.utc)


def rollup(conn, days: int = 60, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    conn.execute("DELETE FROM rollup_daily WHERE day >= ?", (since,))
    cur = conn.execute(
        "INSERT INTO rollup_daily"
        " SELECT substr(ts,1,10) AS day, agent, model, source, COUNT(*),"
        "  SUM(input_tokens), SUM(output_tokens), SUM(cache_read), SUM(cache_write),"
        "  SUM(cost_usd), AVG(latency_ms)"
        " FROM llm_calls WHERE in_totals=1 AND substr(ts,1,10) >= ?"
        " GROUP BY day, agent, model, source",
        (since,),
    )
    conn.commit()
    return cur.rowcount


def _window(now: datetime, days: int) -> tuple[str, str]:
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return (now - timedelta(days=days)).strftime(fmt), now.strftime(fmt)


def _tokens(row_cols: str = "input_tokens+output_tokens+cache_read+cache_write") -> str:
    return f"COALESCE(SUM({row_cols}),0)"


def digest_line(conn, now: datetime, days: int = 7) -> str:
    since, until = _window(now, days)
    # Only Claude transcript rows have an API-equivalent price basis. Codex,
    # provider logs and local-router estimates are reported separately instead of
    # inflating "priced" calls.
    priced = conn.execute(
        f"SELECT COALESCE(SUM(cost_usd),0), {_tokens()}, COUNT(*) FROM llm_calls"
        " WHERE in_totals=1 AND source='sessions' AND ts>=? AND ts<=?", (since, until)).fetchone()
    codex = conn.execute(
        f"SELECT {_tokens()}, COUNT(*) FROM llm_calls WHERE source='codex' AND ts>=? AND ts<=?",
        (since, until)).fetchone()
    provider = conn.execute(
        f"SELECT {_tokens()}, COUNT(*) FROM llm_calls WHERE source='provider' AND ts>=? AND ts<=?",
        (since, until)).fetchone()
    top = conn.execute(
        "SELECT agent, SUM(cost_usd) c FROM llm_calls WHERE in_totals=1 AND ts>=? AND ts<=?"
        " GROUP BY agent ORDER BY c DESC, agent LIMIT 1", (since, until)).fetchone()
    top_agent, top_cost = top or ("n/a", 0.0)
    return (
        f"Fleet priced ${priced[0]:,.2f} (API-equivalent) over the last {days} days - "
        f"top spender {str(top_agent).upper()} at ${top_cost:,.2f}. "
        f"{priced[2]:,} priced calls, {priced[1]/1e6:,.2f}M priced tokens. "
        f"Codex: {codex[1]:,} measured turns, {codex[0]/1e6:,.2f}M tokens "
        f"(subscription price not inferred). Provider logs: {provider[1]:,} "
        f"reported calls, {provider[0]/1e6:,.2f}M tokens (price not inferred)."
    )


def write_digest(conn, cfg: Config, now: datetime, days: int = 7) -> str:
    line = digest_line(conn, now, days)
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    cfg.digest_path.write_text(f"[{now.strftime('%Y-%m-%d %H:%M UTC')}] {line}\n")
    return line


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[k]


def aggregate(conn, group_col: str, since: str, until: str) -> list[dict]:
    """Per-group totals: calls, tokens, API-equivalent cost, latency avg and p95."""
    groups: dict[str, dict] = {}
    for key, inp, out, cr, cw, cost, lat in conn.execute(
        f"SELECT {group_col}, input_tokens, output_tokens, cache_read, cache_write,"
        " cost_usd, latency_ms FROM llm_calls WHERE in_totals=1 AND ts>=? AND ts<=?",
        (since, until),
    ):
        g = groups.setdefault(key, {"name": key, "calls": 0, "input_tokens": 0,
                                    "output_tokens": 0, "cache_tokens": 0,
                                    "cost_usd": 0.0, "_lat": []})
        g["calls"] += 1
        g["input_tokens"] += inp
        g["output_tokens"] += out
        g["cache_tokens"] += cr + cw
        g["cost_usd"] += cost
        if lat is not None:
            g["_lat"].append(lat)
    rows = []
    for g in groups.values():
        lat = g.pop("_lat")
        g["avg_latency_ms"] = round(sum(lat) / len(lat), 1) if lat else None
        g["p95_latency_ms"] = round(_percentile(lat, 0.95), 1) if lat else None
        g["cost_usd"] = round(g["cost_usd"], 6)
        rows.append(g)
    return sorted(rows, key=lambda r: (-r["cost_usd"], -(r["input_tokens"] + r["output_tokens"]
                                                         + r["cache_tokens"]), r["name"]))


def build_report(conn, cfg: Config, now: datetime, days: int = 7) -> dict:
    since, until = _window(now, days)
    by_agent = aggregate(conn, "agent", since, until)
    by_model = aggregate(conn, "model", since, until)
    totals = {
        "calls": sum(r["calls"] for r in by_agent),
        "input_tokens": sum(r["input_tokens"] for r in by_agent),
        "output_tokens": sum(r["output_tokens"] for r in by_agent),
        "cache_tokens": sum(r["cache_tokens"] for r in by_agent),
        "cost_usd": round(sum(r["cost_usd"] for r in by_agent), 6),
    }
    scorecard = build_scorecard(
        conn, now=now, codex_policy=load_codex_policy(cfg.codex_config),
        provider_policy=provider_policy_from_models(cfg.allowed_models),
        policy={**cfg.efficiency_policy, "window_days": days},
    )
    findings = [
        {"id": s["id"], "status": s["status"], "title": s["title"],
         "recommended_action": s["recommended_action"]}
        for s in scorecard["signals"] if s["status"] != "clear"
    ]
    return {"window": {"since": since, "until": until, "days": days}, "totals": totals,
            "by_agent": by_agent, "by_model": by_model,
            "digest": digest_line(conn, now, days), "findings": findings}


def _fmt_ms(v) -> str:
    return "-" if v is None else f"{v:,.0f}"


def _table(title: str, rows: list[dict]) -> list[str]:
    head = f"{title:<30} {'calls':>6} {'input':>10} {'output':>9} {'cache':>10} {'cost USD':>10} {'avg ms':>8} {'p95 ms':>8}"
    out = [head, "-" * len(head)]
    for r in rows:
        out.append(
            f"{r['name'][:30]:<30} {r['calls']:>6,} {r['input_tokens']:>10,} "
            f"{r['output_tokens']:>9,} {r['cache_tokens']:>10,} {r['cost_usd']:>10.4f} "
            f"{_fmt_ms(r['avg_latency_ms']):>8} {_fmt_ms(r['p95_latency_ms']):>8}"
        )
    return out


def format_report(rep: dict) -> str:
    w, t = rep["window"], rep["totals"]
    lines = [
        f"llm-cost-meter report: {w['since']} -> {w['until']} ({w['days']}d)",
        "",
        rep["digest"],
        "",
        f"Totals: {t['calls']:,} calls | in {t['input_tokens']:,} | out {t['output_tokens']:,} | "
        f"cache {t['cache_tokens']:,} | ${t['cost_usd']:.4f} API-equivalent",
        "",
        *_table("By agent", rep["by_agent"]),
        "",
        *_table("By model", rep["by_model"]),
        "",
        "Findings (review signals, never actions):",
    ]
    if rep["findings"]:
        for f in rep["findings"]:
            lines.append(f"  [{f['status']}] {f['title']}: {f['recommended_action']}")
    else:
        lines.append("  none - all signals clear")
    lines += ["", "Cost is API-equivalent for Claude session logs only; codex, provider and "
              "router rows are unpriced (tokens/latency only)."]
    return "\n".join(lines)


def status(conn, cfg: Config, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    out: dict = {}
    for src in ("sessions", "codex", "provider", "router"):
        out[src] = conn.execute("SELECT COUNT(*) FROM llm_calls WHERE source=?", (src,)).fetchone()[0]
    out["dispatch_events"] = conn.execute("SELECT COUNT(*) FROM dispatch_events").fetchone()[0]
    out["context_events"] = conn.execute("SELECT COUNT(*) FROM context_events").fetchone()[0]
    out["codex_measured_tokens"] = conn.execute(
        f"SELECT {_tokens()} FROM llm_calls WHERE source='codex'").fetchone()[0]
    out["provider_reported_tokens"] = conn.execute(
        f"SELECT {_tokens()} FROM llm_calls WHERE source='provider'").fetchone()[0]
    completed, avg_tools, subagents = conn.execute(
        "SELECT COUNT(*), COALESCE(AVG(tool_calls),0), COALESCE(SUM(subagents_spawned),0)"
        " FROM codex_turns WHERE completed=1").fetchone()
    out["codex_completed_turns"] = completed
    out["codex_avg_tools_per_completed_turn"] = round(avg_tools, 2)
    out["codex_subagent_signals"] = subagents
    row = conn.execute("SELECT v FROM meta WHERE k='last_ingest'").fetchone()
    out["last_ingest"] = row[0] if row else None
    try:
        age = (now - datetime.fromisoformat(out["last_ingest"].replace("Z", "+00:00"))).total_seconds()
    except (AttributeError, TypeError, ValueError):
        age = None
    out["ingest_age_seconds"] = age
    out["fresh"] = age is not None and 0 <= age <= EFFICIENCY_POLICY["pipeline_stale_hours"] * 3600
    error_row = conn.execute("SELECT v FROM meta WHERE k='last_ingest_errors'").fetchone()
    out["last_ingest_errors"] = json.loads(error_row[0]) if error_row else {}
    out["sources_configured"] = {
        "claude_projects": str(cfg.claude_projects) if cfg.claude_projects else None,
        "codex_sessions": str(cfg.codex_sessions) if cfg.codex_sessions else None,
        "router_log": str(cfg.router_log) if cfg.router_log else None,
        "dispatch_log": str(cfg.dispatch_log) if cfg.dispatch_log else None,
        "context_log": str(cfg.context_log) if cfg.context_log else None,
        "provider_logs": cfg.provider_logs,
    }
    return out


def run_efficiency(conn, cfg: Config, now: datetime) -> dict:
    return write_scorecard(
        conn, cfg.scorecard_json, cfg.scorecard_md, now=now,
        codex_policy=load_codex_policy(cfg.codex_config),
        provider_policy=provider_policy_from_models(cfg.allowed_models),
        policy=cfg.efficiency_policy,
    )


# -- CLI ---------------------------------------------------------------------

def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", help="JSON config file (or set LLM_METER_CONFIG)")
    p.add_argument("--db", help="SQLite path (or LLM_METER_DB / LLM_METER_HOME)")
    p.add_argument("--source", help="ingest everything under one folder laid out like "
                   "examples/sample_logs (overrides configured source paths)")
    p.add_argument("--as-of", help="evaluate windows as of this ISO timestamp (default: now)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="llm_cost_meter", description=__doc__.split("\n")[0])
    _add_common(p)
    # The shared flags work before or after the subcommand; SUPPRESS keeps a
    # subcommand's unset flag from clobbering one given before it.
    sub_common = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
    _add_common(sub_common)
    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("ingest", parents=[sub_common], argument_default=argparse.SUPPRESS,
                   help="incremental ingest of every configured source")
    r = sub.add_parser("rollup", parents=[sub_common], argument_default=argparse.SUPPRESS,
                       help="rebuild daily aggregates")
    r.add_argument("--days", type=int, default=60)
    rp = sub.add_parser("report", parents=[sub_common], argument_default=argparse.SUPPRESS,
                        help="weekly cost/latency/tokens per agent and model")
    rp.add_argument("--days", type=int, default=7)
    rp.add_argument("--json", action="store_true", help="machine-readable output")
    for name, text in (("digest", "write the one-line weekly digest"),
                       ("efficiency", "write the efficiency scorecard"),
                       ("status", "row counts and source health"),
                       ("cron", "ingest + rollup + digest + efficiency")):
        sub.add_parser(name, parents=[sub_common], argument_default=argparse.SUPPRESS, help=text)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cmd:
        parser.print_help()
        return 2
    cfg = load_config(getattr(args, "config", None), getattr(args, "db", None),
                      getattr(args, "source", None))
    now = _now(getattr(args, "as_of", None))
    conn = connect(cfg)
    try:
        if args.cmd == "ingest":
            out = ingest(conn, cfg)
            print(json.dumps(out))
            return 0 if out["ok"] else 1
        if args.cmd == "rollup":
            print(json.dumps({"rollup_rows": rollup(conn, getattr(args, 'days', 60), now)}))
        elif args.cmd == "report":
            rep = build_report(conn, cfg, now, getattr(args, "days", 7))
            print(json.dumps(rep, indent=2) if getattr(args, "json", False) else format_report(rep))
        elif args.cmd == "digest":
            print(write_digest(conn, cfg, now))
        elif args.cmd == "efficiency":
            print(json.dumps(run_efficiency(conn, cfg, now), indent=2))
        elif args.cmd == "status":
            print(json.dumps(status(conn, cfg), indent=2))
        elif args.cmd == "cron":
            out = ingest(conn, cfg)
            out["rollup_rows"] = rollup(conn, 60, now)
            out["digest"] = write_digest(conn, cfg, now)
            card = run_efficiency(conn, cfg, now)
            out["efficiency"] = {"status": card["summary"]["status"],
                                 "attention_signals": card["summary"]["attention_signals"],
                                 "mode": card["mode"], "alert_delivery": card["alert_delivery"]}
            print(json.dumps(out))
            return 0 if out["ok"] else 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
