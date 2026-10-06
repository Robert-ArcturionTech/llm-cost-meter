#!/usr/bin/env python3
"""Efficiency scorecard: passive, evidence-labelled findings over the meter store.

This module only reads the SQLite store written by ``llm_cost_meter``. It never
changes routing, blocks a request, or sends a message. Every signal carries a
status of ``clear``, ``attention`` or ``unavailable``; "unavailable" means the
telemetry needed to judge it was not emitted, and nothing is inferred.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:  # Python 3.11+; only needed to read a Codex config.toml default model
    import tomllib
except ImportError:  # pragma: no cover
    tomllib = None

SCHEMA_ID = "llm-cost-meter.efficiency.v0"

# Guardrails flag telemetry quality and pipeline health; they never make a
# model or task decision. Override any key via ``efficiency_policy`` in config.
POLICY = {
    "version": "v0",
    "window_days": 7,
    "pipeline_stale_hours": 3,
    "router_unknown_attention_rate": 0.10,
    "router_failure_attention_rate": 0.05,
    "context_pressure_percent": 80,
    "context_pressure_attention_rate": 0.10,
    "minimum_sample_size": 10,
}

SOURCE_QUALITY = {
    "sessions": "measured transcript usage; price is API-equivalent, not subscription cash",
    "codex": "measured cumulative usage; subscription price is not inferred",
    "provider": "provider-log-reported usage; not independently invoice-verified",
    "router": "local-router token estimates",
    "dispatch": "dispatch receipts; attribution is explicit only when all evidence fields are present",
}


def _utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _age_minutes(value: str | None, now: datetime) -> int | None:
    stamp = _utc(value)
    if not stamp:
        return None
    return max(0, round((now - stamp).total_seconds() / 60))


def _model_rows(conn: sqlite3.Connection, source: str, since: str) -> list[dict]:
    return [
        {"model": model, "calls": calls}
        for model, calls in conn.execute(
            "SELECT model, COUNT(*) FROM llm_calls WHERE source=? AND ts>=? "
            "GROUP BY model ORDER BY COUNT(*) DESC, model",
            (source, since),
        )
    ]


def load_codex_policy(path: Path | None) -> dict:
    """Read the active Codex default model, if a config.toml is available."""
    if path is None or tomllib is None:
        return {"available": False, "path": str(path) if path else None}
    try:
        raw = tomllib.loads(Path(path).read_text())
        return {
            "available": True,
            "default_model": raw.get("model"),
            "default_reasoning_effort": raw.get("model_reasoning_effort"),
            "path": str(path),
        }
    except (OSError, tomllib.TOMLDecodeError):
        return {"available": False, "path": str(path)}


def provider_policy_from_models(allowed_models) -> dict:
    """Policy for provider-log traffic: the list of models you allow."""
    allowed = sorted({m for m in (allowed_models or []) if m})
    return {"available": bool(allowed), "allowed_models": allowed}


def _signal(signal_id: str, status: str, title: str, observed: dict,
            threshold: str | None, action: str) -> dict:
    return {
        "id": signal_id,
        "status": status,
        "title": title,
        "observed": observed,
        "threshold": threshold,
        "recommended_action": action,
    }


def _evidence_counts(conn: sqlite3.Connection, table: str, since: str,
                     source: str | None = None) -> dict:
    """Measure only explicit evidence fields; NULL historical values stay NULL."""
    where = "ts>=?"
    params: list[str] = [since]
    if source:
        where += " AND source=?"
        params.append(source)
    total, rostered, runs, correlations, complete = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(roster_agent IS NOT NULL),0), "
        f"COALESCE(SUM(run_id IS NOT NULL),0), COALESCE(SUM(correlation_id IS NOT NULL),0), "
        f"COALESCE(SUM(roster_agent IS NOT NULL AND run_id IS NOT NULL "
        f"AND correlation_id IS NOT NULL),0) FROM {table} WHERE {where}",
        params,
    ).fetchone()
    return {
        "records": total,
        "roster_agent_records": rostered,
        "run_id_records": runs,
        "correlation_id_records": correlations,
        "complete_evidence_records": complete,
        "complete_evidence_rate": round(complete / total, 4) if total else 0.0,
    }


def _retry_evidence(conn: sqlite3.Connection, since: str) -> dict:
    """Count repeated correlations without labeling a retry as waste by itself."""
    rows = conn.execute(
        "SELECT correlation_id, COUNT(*) FROM llm_calls WHERE source='router' AND ts>=? "
        "AND roster_agent IS NOT NULL AND run_id IS NOT NULL AND correlation_id IS NOT NULL "
        "GROUP BY correlation_id HAVING COUNT(*)>1",
        (since,),
    ).fetchall()
    return {
        "retry_groups": len(rows),
        "repeat_calls_after_first": sum(count - 1 for _, count in rows),
        "basis": "repeated explicit router correlation IDs; review evidence, not a waste claim",
    }


def build_scorecard(
    conn: sqlite3.Connection,
    now: datetime | None = None,
    codex_policy: dict | None = None,
    provider_policy: dict | None = None,
    policy: dict | None = None,
) -> dict:
    """Build a scorecard from observed data without mutating the meter store."""
    pol = {**POLICY, **(policy or {})}
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    since = (now - timedelta(days=pol["window_days"])).strftime("%Y-%m-%dT%H:%M:%SZ")
    until = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    codex_policy = codex_policy if codex_policy is not None else {"available": False}
    provider_policy = provider_policy if provider_policy is not None else provider_policy_from_models([])

    signals: list[dict] = []
    last = conn.execute("SELECT v FROM meta WHERE k='last_ingest'").fetchone()
    last_ingest = last[0] if last else None
    ingest_age = _age_minutes(last_ingest, now)
    stale_limit = pol["pipeline_stale_hours"] * 60
    signals.append(_signal(
        "pipeline_freshness",
        "clear" if ingest_age is not None and ingest_age <= stale_limit else "attention",
        "Meter pipeline freshness",
        {"last_ingest": last_ingest, "age_minutes": ingest_age},
        f"attention after {pol['pipeline_stale_hours']} hours without a completed ingest",
        "Check the scheduler that runs the meter and the per-source error counts in `status`.",
    ))

    router_calls, router_unknown, router_failed = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(roster_agent IS NULL),0), COALESCE(SUM(ok=0),0) "
        "FROM llm_calls WHERE source='router' AND ts>=? AND ts<=?",
        (since, until),
    ).fetchone()
    unknown_rate = router_unknown / router_calls if router_calls else 0.0
    failed_rate = router_failed / router_calls if router_calls else 0.0
    enough = router_calls >= pol["minimum_sample_size"]
    signals.append(_signal(
        "router_attribution",
        "attention" if enough and unknown_rate > pol["router_unknown_attention_rate"] else "clear",
        "Local-router owner attribution",
        {
            "calls": router_calls,
            "missing_explicit_roster_calls": router_unknown,
            "unknown_rate": round(unknown_rate, 4),
            "token_basis": "estimate",
        },
        f"attention above {pol['router_unknown_attention_rate']:.0%} after {pol['minimum_sample_size']} calls",
        "Have the router emit an explicit roster_agent field; historical rows without it stay unattributed.",
    ))
    signals.append(_signal(
        "router_call_failures",
        "attention" if enough and failed_rate > pol["router_failure_attention_rate"] else "clear",
        "Local-router call failures",
        {"calls": router_calls, "failed_calls": router_failed, "failure_rate": round(failed_rate, 4)},
        f"attention above {pol['router_failure_attention_rate']:.0%} after {pol['minimum_sample_size']} calls",
        "Inspect failed calls alongside correlation evidence; failed calls alone are not retry waste.",
    ))

    codex_models = _model_rows(conn, "codex", since)
    provider_models = _model_rows(conn, "provider", since)
    allowed = set(provider_policy.get("allowed_models") or [])
    outside = [row for row in provider_models if row["model"] not in allowed]
    if not provider_policy.get("available"):
        model_status = "unavailable"
        model_action = "Set `allowed_models` in the config to evaluate model-policy conformance."
    elif outside:
        model_status = "attention"
        model_action = "Review the listed models against your policy; this scorecard never changes a model."
    else:
        model_status = "clear"
        model_action = "Continue recording observed models."
    signals.append(_signal(
        "model_policy_observation",
        model_status,
        "Model-policy observation",
        {
            "codex_default": codex_policy,
            "codex_observed_models": codex_models,
            "provider_policy": provider_policy,
            "provider_observed_models": provider_models,
            "provider_models_outside_declared_policy": outside,
        },
        "Provider-log models must appear in `allowed_models`",
        model_action,
    ))

    context_events, high_pressure, compactions, reduced_memory = conn.execute(
        "SELECT COUNT(*), "
        "COALESCE(SUM(used_percentage>=?),0), COALESCE(SUM(compaction),0), "
        "COALESCE(SUM(reduced_working_memory),0) "
        "FROM context_events WHERE ts>=? AND ts<=?",
        (pol["context_pressure_percent"], since, until),
    ).fetchone()
    pressure_rate = high_pressure / context_events if context_events else 0.0
    pressure_attention = (
        context_events >= pol["minimum_sample_size"]
        and pressure_rate > pol["context_pressure_attention_rate"]
    )
    signals.append(_signal(
        "context_pressure",
        "attention" if pressure_attention else "clear",
        "Context-window pressure",
        {
            "events": context_events,
            "high_pressure_events": high_pressure,
            "high_pressure_rate": round(pressure_rate, 4),
            "compactions": compactions,
            "reduced_working_memory_events": reduced_memory,
        },
        f"attention when {pol['context_pressure_percent']}%+ pressure exceeds "
        f"{pol['context_pressure_attention_rate']:.0%} of {pol['minimum_sample_size']}+ events",
        "A review signal, not waste: inspect context composition before changing a window or compaction policy.",
    ))
    router_evidence = _evidence_counts(conn, "llm_calls", since, source="router")
    dispatch_evidence = _evidence_counts(conn, "dispatch_events", since)
    retry_evidence = _retry_evidence(conn, since)
    signals.append(_signal(
        "retry_waste",
        "clear" if router_evidence["complete_evidence_records"] else "unavailable",
        "Retry evidence",
        {"available": bool(router_evidence["complete_evidence_records"]),
         **router_evidence, **retry_evidence},
        None,
        "Repeated correlations are review evidence only; add explicit correlation IDs where coverage is missing.",
    ))
    signals.append(_signal(
        "automation_dispatch_attribution",
        "clear" if dispatch_evidence["complete_evidence_records"] else "unavailable",
        "Automation and dispatch attribution",
        {"available": bool(dispatch_evidence["complete_evidence_records"]), **dispatch_evidence},
        None,
        "Only dispatch records carrying all three explicit fields are measured; older ones stay unavailable.",
    ))

    source_activity = []
    for source, quality in SOURCE_QUALITY.items():
        if source == "dispatch":
            row = conn.execute("SELECT MAX(ts), COUNT(*) FROM dispatch_events").fetchone()
        else:
            row = conn.execute(
                "SELECT MAX(ts), COUNT(*) FROM llm_calls WHERE source=?", (source,)
            ).fetchone()
        source_activity.append({
            "source": source,
            "last_observed": row[0],
            "age_minutes": _age_minutes(row[0], now),
            "records": row[1],
            "quality": quality,
        })

    attention = [s for s in signals if s["status"] == "attention"]
    unavailable = [s for s in signals if s["status"] == "unavailable"]
    return {
        "schema": SCHEMA_ID,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mode": "read_only",
        "alert_delivery": "preview_only",
        "policy": pol,
        "summary": {
            "status": "attention" if attention else "clear",
            "attention_signals": len(attention),
            "unavailable_signals": len(unavailable),
            "meaning": "Attention requests review only; it never changes routing or sends a notification.",
        },
        "alert_preview": [
            {"signal": s["id"], "title": s["title"], "recommended_action": s["recommended_action"]}
            for s in attention
        ],
        "signals": signals,
        "source_activity": source_activity,
    }


def markdown(scorecard: dict) -> str:
    lines = [
        "# Efficiency scorecard",
        "",
        f"Generated: {scorecard['generated_at']} - mode: read-only - delivery: preview only",
        "",
        "| Signal | Status | Observed |",
        "|---|---|---|",
    ]
    for signal in scorecard["signals"]:
        observed = json.dumps(signal["observed"], sort_keys=True, separators=(",", ":"))
        lines.append(f"| {signal['title']} | {signal['status']} | `{observed}` |")
    lines.extend([
        "",
        "## Source labels",
        "",
        "| Source | Records | Last observed | Quality |",
        "|---|---:|---|---|",
    ])
    for source in scorecard["source_activity"]:
        lines.append(
            f"| {source['source']} | {source['records']:,} | "
            f"{source['last_observed'] or 'none'} | {source['quality']} |"
        )
    return "\n".join(lines) + "\n"


def write_scorecard(conn: sqlite3.Connection, json_path: Path, report_path: Path,
                    now: datetime | None = None, codex_policy: dict | None = None,
                    provider_policy: dict | None = None, policy: dict | None = None) -> dict:
    scorecard = build_scorecard(conn, now=now, codex_policy=codex_policy,
                                provider_policy=provider_policy, policy=policy)
    Path(json_path).parent.mkdir(parents=True, exist_ok=True)
    Path(json_path).write_text(json.dumps(scorecard, indent=2, sort_keys=True) + "\n")
    Path(report_path).write_text(markdown(scorecard))
    return scorecard
