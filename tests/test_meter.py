import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import llm_cost_meter as meter  # noqa: E402

SAMPLE = ROOT / "examples" / "sample_logs"
AS_OF = datetime(2026, 9, 5, 23, 59, tzinfo=timezone.utc)


def make_cfg(root: Path, **kw) -> meter.Config:
    return meter.Config(db_path=root / "meter.db", **kw)


class PricingAndAttributionTests(unittest.TestCase):
    def test_longest_prefix_wins_and_unknown_models_are_free(self):
        self.assertEqual((15.0, 75.0), meter.price_for("claude-opus-4-20250514"))
        self.assertEqual((5.0, 25.0), meter.price_for("claude-opus-4-8"))
        self.assertEqual((0.0, 0.0), meter.price_for("local/qwen-7b"))

    def test_cost_formula_includes_cache_multipliers(self):
        # 5000 + 12500 + 1000 + 2500 + 1000 = 22,000 / 1e6
        cost = meter.claude_cost("claude-opus-4-8", 1000, 500, 2000, 400, 100)
        self.assertAlmostEqual(0.022, cost, places=9)

    def test_pricing_override_from_config(self):
        pricing = {**meter.DEFAULT_PRICING, "claude-sonnet": (1.0, 2.0)}
        self.assertAlmostEqual(0.003, meter.claude_cost("claude-sonnet-4-6", 1000, 1000, 0, 0, 0, pricing))

    def test_agent_from_cwd(self):
        self.assertEqual("coder", meter.agent_from_cwd("/home/dev/agents/coder/src"))
        self.assertEqual("coder", meter.agent_from_cwd("/srv/fleet/Coder", ("agents",)))
        self.assertEqual("worker", meter.agent_from_cwd("/srv/team/worker/app", ("team",)))
        self.assertEqual("unknown", meter.agent_from_cwd(""))


class SessionHarvestTests(unittest.TestCase):
    def test_sessions_dedupe_skip_synthetic_and_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp), claude_projects=SAMPLE / "claude_projects")
            conn = meter.connect(cfg)
            self.assertEqual(11, meter.ingest_sessions(conn, cfg))  # dup + synthetic skipped
            self.assertEqual(0, meter.ingest_sessions(conn, cfg))   # offsets make re-runs no-ops
            row = conn.execute(
                "SELECT agent, model, input_tokens, output_tokens, cache_read, cache_write, cost_usd "
                "FROM llm_calls WHERE dedupe_key='sess:req_p01'").fetchone()
            self.assertEqual(("planner", "claude-opus-4-8", 1200, 850, 0, 9000), row[:6])
            self.assertAlmostEqual(0.0835, row[6], places=6)
            agents = {r[0] for r in conn.execute("SELECT DISTINCT agent FROM llm_calls")}
            self.assertEqual({"planner", "coder", "reviewer"}, agents)  # coder/src -> coder
            conn.close()


class CodexHarvestTests(unittest.TestCase):
    def test_cumulative_codex_usage_is_upserted_once_per_turn(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "sessions" / "2026" / "09" / "05" / "rollout.jsonl"
            target.parent.mkdir(parents=True)
            cwd = "/home/dev/agents/coder"
            records = [
                {"timestamp": "2026-09-05T10:00:00Z", "type": "session_meta", "payload": {"session_id": "s1", "cwd": cwd}},
                {"timestamp": "2026-09-05T10:00:01Z", "type": "turn_context", "payload": {"turn_id": "t1", "model": "gpt-5.1-codex", "effort": "medium", "cwd": cwd}},
                {"timestamp": "2026-09-05T10:00:02Z", "type": "event_msg", "payload": {"type": "task_complete", "turn_id": "t1", "duration_ms": 1200, "time_to_first_token_ms": 80}},
                {"timestamp": "2026-09-05T10:00:03Z", "type": "token_usage_record", "payload": {"turn_id": "t1", "turn_token_usage": {"input_tokens": 100, "cached_input_tokens": 30, "cache_write_input_tokens": 10, "output_tokens": 20, "total_tokens": 120}}},
                {"timestamp": "2026-09-05T10:00:04Z", "type": "token_usage_record", "payload": {"turn_id": "t1", "turn_token_usage": {"input_tokens": 140, "cached_input_tokens": 50, "cache_write_input_tokens": 10, "output_tokens": 30, "total_tokens": 170}}},
            ]
            target.write_text("\n".join(json.dumps(r) for r in records) + "\n")
            cfg = make_cfg(root, codex_sessions=root / "sessions")
            conn = meter.connect(cfg)
            self.assertEqual(2, meter.ingest_codex_sessions(conn, cfg))
            rows = conn.execute("SELECT agent, model, input_tokens, cache_read, cache_write, "
                                "output_tokens, latency_ms, task FROM llm_calls").fetchall()
            self.assertEqual([("coder", "gpt-5.1-codex", 80, 50, 10, 30, 1200.0, "effort=medium;ttft_ms=80")], rows)
            behavior = conn.execute("SELECT reasoning_effort, tool_calls, assistant_messages, completed "
                                    "FROM codex_turns").fetchone()
            self.assertEqual(("medium", 0, 0, 1), behavior)
            self.assertEqual(0, meter.ingest_codex_sessions(conn, cfg))
            conn.close()

    def test_sample_codex_counts_tools_and_subagents(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp), codex_sessions=SAMPLE / "codex_sessions")
            conn = meter.connect(cfg)
            self.assertEqual(3, meter.ingest_codex_sessions(conn, cfg))
            turns = conn.execute("SELECT turn_id, tool_calls, assistant_messages, subagents_spawned "
                                 "FROM codex_turns ORDER BY turn_id").fetchall()
            self.assertEqual([("t1", 2, 1, 0), ("t2", 1, 0, 1)], turns)
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM llm_calls").fetchone()[0])
            conn.close()


class ProviderLogTests(unittest.TestCase):
    LINE = ("2026-09-05 08:02:00,000 INFO [probe_1] agent.loop: API call #6: model=gpt-5.1 "
            "provider=openai in=500 out=20 total=520 latency=2.0s\n")

    def test_call_summaries_preserve_cache_and_are_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "providers" / "researcher" / "agent.log"
            target.parent.mkdir(parents=True)
            target.write_text(
                "2026-09-05 08:01:57,485 INFO [cron_1] agent.loop: API call #5: model=gpt-5.1 "
                "provider=openai in=44607 out=765 total=45372 latency=16.1s cache=43520/44607 (98%)\n"
                + self.LINE)
            cfg = make_cfg(root, provider_logs=str(root / "providers" / "*" / "agent.log"))
            conn = meter.connect(cfg)
            self.assertEqual(2, meter.ingest_provider_logs(conn, cfg))
            rows = conn.execute("SELECT agent, source, model, input_tokens, cache_read, output_tokens, "
                                "latency_ms, cost_usd FROM llm_calls ORDER BY ts").fetchall()
            self.assertEqual([
                ("researcher", "provider", "gpt-5.1", 1087, 43520, 765, 16100.0, 0.0),
                ("researcher", "provider", "gpt-5.1", 500, 0, 20, 2000.0, 0.0),
            ], rows)
            self.assertEqual(0, meter.ingest_provider_logs(conn, cfg))
            conn.close()

    def test_timezone_is_configurable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "p" / "ops" / "agent.log"
            target.parent.mkdir(parents=True)
            target.write_text(self.LINE)
            cfg = make_cfg(root, provider_logs=str(root / "p" / "*" / "agent.log"),
                           provider_log_timezone="America/New_York")
            conn = meter.connect(cfg)
            meter.ingest_provider_logs(conn, cfg)
            self.assertEqual("2026-09-05T12:02:00Z", conn.execute("SELECT ts FROM llm_calls").fetchone()[0])
            conn.close()

    def test_rotated_replay_has_no_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "ops" / "agent.log"
            target.parent.mkdir()
            target.write_text(self.LINE)
            target.with_name("agent.log.1").write_text(self.LINE)
            cfg = make_cfg(root, provider_logs=str(root / "*" / "agent.log"))
            conn = meter.connect(cfg)
            self.assertEqual(1, meter.ingest_provider_logs(conn, cfg))
            self.assertEqual(0, meter.ingest_provider_logs(conn, cfg))
            self.assertEqual(1, conn.execute("SELECT count(*) FROM llm_calls").fetchone()[0])
            conn.close()

    def test_inconsistent_totals_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "ops" / "agent.log"
            target.parent.mkdir()
            target.write_text(self.LINE.replace("total=520", "total=999"))
            cfg = make_cfg(root, provider_logs=str(root / "*" / "agent.log"))
            conn = meter.connect(cfg)
            self.assertEqual(0, meter.ingest_provider_logs(conn, cfg))
            conn.close()


class RouterAndDispatchTests(unittest.TestCase):
    def test_rfc822_timezone_offsets_are_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / "router.jsonl"
            log.write_text(json.dumps({
                "ts": "2026-09-05T08:29:05-0400", "provider": "local", "model": "qwen-7b",
                "task": "triage", "agent": "ops", "tokens_in_est": 169, "tokens_out_est": 21,
                "latency_ms": 1091, "ok": True}) + "\n")
            cfg = make_cfg(root, router_log=log)
            conn = meter.connect(cfg)
            self.assertEqual(1, meter.ingest_router(conn, cfg))
            row = conn.execute("SELECT ts, agent, source, model, input_tokens, output_tokens, "
                               "latency_ms, roster_agent, run_id, correlation_id FROM llm_calls").fetchone()
            self.assertEqual(("2026-09-05T12:29:05Z", "ops", "router", "local/qwen-7b", 169, 21,
                              1091.0, None, None, None), row)
            self.assertEqual(0, meter.ingest_router(conn, cfg))
            conn.close()

    def test_explicit_evidence_is_kept_and_legacy_rows_are_not_backfilled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / "router.jsonl"
            log.write_text("\n".join(json.dumps(r) for r in [
                {"ts": "2026-09-05T12:00:00Z", "provider": "local", "model": "qwen-7b", "agent": "ops",
                 "roster_agent": "ops", "run_id": "dispatch-1", "correlation_id": "dispatch-1",
                 "tokens_in_est": 10, "tokens_out_est": 2, "latency_ms": 1, "ok": True},
                {"ts": "2026-09-05T12:01:00Z", "provider": "local", "model": "qwen-7b", "agent": "unknown",
                 "tokens_in_est": 10, "tokens_out_est": 2, "latency_ms": 1, "ok": True},
            ]) + "\n")
            cfg = make_cfg(root, router_log=log)
            conn = meter.connect(cfg)
            self.assertEqual(2, meter.ingest_router(conn, cfg))
            rows = conn.execute("SELECT roster_agent, run_id, correlation_id FROM llm_calls ORDER BY ts").fetchall()
            self.assertEqual([("ops", "dispatch-1", "dispatch-1"), (None, None, None)], rows)
            conn.close()

    def test_roster_allowlist_rejects_unknown_labels(self):
        self.assertEqual("ops", meter._explicit_roster_agent("OPS", frozenset({"ops"})))
        self.assertIsNone(meter._explicit_roster_agent("intruder", frozenset({"ops"})))
        self.assertEqual("anyone", meter._explicit_roster_agent("anyone"))
        self.assertIsNone(meter._explicit_roster_agent("bad id!"))

    def test_dispatch_evidence_requires_explicit_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / "dispatch.jsonl"
            log.write_text("\n".join(json.dumps(r) for r in [
                {"ts": "2026-09-05T12:00:00Z", "msg_id": "legacy", "to": "ops", "actions": []},
                {"ts": "2026-09-05T12:01:00Z", "msg_id": "current", "roster_agent": "ops",
                 "run_id": "dispatch-current", "correlation_id": "dispatch-current", "actions": ["inbox"]},
            ]) + "\n")
            cfg = make_cfg(root, dispatch_log=log)
            conn = meter.connect(cfg)
            self.assertEqual(2, meter.ingest_dispatch(conn, cfg))
            rows = conn.execute("SELECT roster_agent, run_id, correlation_id FROM dispatch_events ORDER BY ts").fetchall()
            self.assertEqual([(None, None, None), ("ops", "dispatch-current", "dispatch-current")], rows)
            conn.close()


class IngestRecoveryTests(unittest.TestCase):
    def test_source_failure_does_not_refresh_success_stamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))
            conn = meter.connect(cfg)
            stamp = "2026-01-01T00:00:00Z"
            conn.execute("INSERT INTO meta(k,v) VALUES('last_ingest',?)", (stamp,))
            with ExitStack() as stack:
                patched = [(name, mock.Mock(return_value=0)) for name, _ in meter.INGESTERS]
                patched = [(n, mock.Mock(side_effect=RuntimeError("fixture")) if n == "provider" else m)
                           for n, m in patched]
                stack.enter_context(mock.patch.object(meter, "INGESTERS", tuple(patched)))
                result = meter.ingest(conn, cfg)
            self.assertFalse(result["ok"])
            self.assertTrue(result["provider"].startswith("error:"))
            self.assertEqual(stamp, conn.execute("SELECT v FROM meta WHERE k='last_ingest'").fetchone()[0])
            self.assertFalse(meter.status(conn, cfg)["fresh"])
            conn.close()

    def test_truncated_file_is_re_read_from_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / "router.jsonl"
            rec = {"ts": "2026-09-05T12:00:00Z", "model": "m", "agent": "ops", "ok": True}
            log.write_text(json.dumps({**rec, "task": "a-long-task-name-to-grow-the-file"}) + "\n")
            cfg = make_cfg(root, router_log=log)
            conn = meter.connect(cfg)
            self.assertEqual(1, meter.ingest_router(conn, cfg))
            log.write_text(json.dumps({**rec, "ts": "2026-09-05T13:00:00Z"}) + "\n")  # shorter file
            self.assertEqual(1, meter.ingest_router(conn, cfg))
            conn.close()


class ConfigTests(unittest.TestCase):
    def test_defaults_point_at_standard_locations(self):
        cfg = meter.load_config(env={})
        self.assertEqual(Path.home() / ".claude" / "projects", cfg.claude_projects)
        self.assertEqual(Path.home() / ".codex" / "sessions", cfg.codex_sessions)
        self.assertIsNone(cfg.router_log)

    def test_file_then_env_then_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            conf = root / "conf.json"
            conf.write_text(json.dumps({
                "db_path": "data/meter.db", "sources": {"router_log": "logs/router.jsonl"},
                "agent_markers": ["fleet"], "allowed_models": ["m1"],
                "pricing": {"claude-sonnet": [1, 2]}}))
            cfg = meter.load_config(str(conf), env={})
            self.assertEqual(root.resolve() / "data" / "meter.db", cfg.db_path)  # relative to the file
            self.assertEqual(root.resolve() / "logs" / "router.jsonl", cfg.router_log)
            self.assertEqual(("fleet",), cfg.agent_markers)
            self.assertEqual((1, 2), cfg.pricing["claude-sonnet"])
            self.assertIn("claude-opus", cfg.pricing)  # defaults are merged, not replaced
            cfg = meter.load_config(str(conf), env={"LLM_METER_DB": "/tmp/env.db",
                                                    "LLM_METER_ROUTER_LOG": "/tmp/r.jsonl"})
            self.assertEqual(Path("/tmp/env.db"), cfg.db_path)
            self.assertEqual(Path("/tmp/r.jsonl"), cfg.router_log)
            cfg = meter.load_config(str(conf), db="/tmp/flag.db", env={"LLM_METER_DB": "/tmp/env.db"})
            self.assertEqual(Path("/tmp/flag.db"), cfg.db_path)

    def test_source_root_overrides_every_source(self):
        cfg = meter.load_config(source_root=str(SAMPLE), env={})
        self.assertEqual(SAMPLE / "claude_projects", cfg.claude_projects)
        self.assertEqual(SAMPLE / "router.ndjson", cfg.router_log)
        self.assertEqual(str(SAMPLE / "provider_logs/*/agent.txt"), cfg.provider_logs)


class EndToEndSampleTests(unittest.TestCase):
    """The shipped sample logs must always ingest and report cleanly."""

    def run_pipeline(self, tmp, allowed=()):
        cfg = meter.load_config(db=str(Path(tmp) / "demo.db"), source_root=str(SAMPLE), env={})
        cfg.allowed_models = tuple(allowed)
        conn = meter.connect(cfg)
        counts = meter.ingest(conn, cfg)
        return cfg, conn, counts

    def test_ingest_counts_and_idempotence(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, conn, counts = self.run_pipeline(tmp)
            self.assertTrue(counts["ok"])
            self.assertEqual({"sessions": 11, "codex": 3, "provider": 6, "router": 12,
                              "dispatch": 3, "context": 12},
                             {k: counts[k] for k in ("sessions", "codex", "provider", "router", "dispatch", "context")})
            again = meter.ingest(conn, cfg)
            self.assertTrue(all(again[k] == 0 for k in ("sessions", "codex", "provider", "router", "dispatch", "context")))
            conn.close()

    def test_report_totals_and_ordering(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, conn, _ = self.run_pipeline(tmp)
            rep = meter.build_report(conn, cfg, AS_OF)
            self.assertEqual(31, rep["totals"]["calls"])
            self.assertAlmostEqual(0.6325, rep["totals"]["cost_usd"], places=4)
            self.assertEqual("coder", rep["by_agent"][0]["name"])  # top spender first
            self.assertEqual({"coder", "planner", "reviewer", "researcher", "ops", "unknown"},
                             {r["name"] for r in rep["by_agent"]})
            self.assertEqual(0.0, next(r for r in rep["by_agent"] if r["name"] == "ops")["cost_usd"])
            self.assertIn("claude-opus-4-8", [r["name"] for r in rep["by_model"]])
            text = meter.format_report(rep)
            self.assertIn("By agent", text)
            self.assertIn("By model", text)
            conn.close()

    def test_window_excludes_old_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, conn, _ = self.run_pipeline(tmp)
            rep = meter.build_report(conn, cfg, datetime(2026, 9, 3, 0, 0, tzinfo=timezone.utc), days=1)
            self.assertLess(rep["totals"]["calls"], 31)
            conn.close()

    def test_findings_reflect_policy_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, conn, _ = self.run_pipeline(tmp)
            ids = {f["id"]: f["status"] for f in meter.build_report(conn, cfg, AS_OF)["findings"]}
            self.assertEqual("unavailable", ids["model_policy_observation"])
            self.assertEqual("attention", ids["router_attribution"])
            self.assertEqual("attention", ids["router_call_failures"])
            self.assertEqual("attention", ids["context_pressure"])
            cfg.allowed_models = ("gpt-5.1",)
            ids = {f["id"]: f["status"] for f in meter.build_report(conn, cfg, AS_OF)["findings"]}
            self.assertEqual("attention", ids["model_policy_observation"])  # kimi-k2 is outside policy
            conn.close()

    def test_cron_style_artifacts_are_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, conn, _ = self.run_pipeline(tmp)
            self.assertGreater(meter.rollup(conn, 60, AS_OF), 0)
            self.assertIn("Fleet priced", meter.write_digest(conn, cfg, AS_OF))
            card = meter.run_efficiency(conn, cfg, AS_OF)
            self.assertEqual("read_only", card["mode"])
            self.assertTrue(cfg.digest_path.exists() and cfg.scorecard_json.exists() and cfg.scorecard_md.exists())
            conn.close()

    def test_cli_report_json(self):
        import io
        from contextlib import redirect_stdout
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cli.db")
            base = ["--db", db, "--source", str(SAMPLE)]
            with redirect_stdout(io.StringIO()):
                self.assertEqual(0, meter.main([*base, "ingest"]))
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertEqual(0, meter.main([*base, "--as-of", "2026-09-05T23:59:00Z", "report", "--json"]))
            rep = json.loads(buf.getvalue())
            self.assertEqual(31, rep["totals"]["calls"])


if __name__ == "__main__":
    unittest.main()
