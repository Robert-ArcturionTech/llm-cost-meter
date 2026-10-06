import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import llm_cost_efficiency as efficiency  # noqa: E402

NOW = datetime(2026, 9, 5, 14, 0, tzinfo=timezone.utc)

DDL = """
    CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE llm_calls (
        ts TEXT, agent TEXT, source TEXT, model TEXT,
        input_tokens INTEGER, output_tokens INTEGER, cache_read INTEGER,
        cache_write INTEGER, cost_usd REAL, latency_ms REAL, task TEXT,
        roster_agent TEXT, run_id TEXT, correlation_id TEXT,
        ok INTEGER, in_totals INTEGER, dedupe_key TEXT UNIQUE
    );
    CREATE TABLE dispatch_events (ts TEXT, roster_agent TEXT, run_id TEXT,
        correlation_id TEXT, msg_id TEXT, actions_json TEXT, dedupe_key TEXT UNIQUE);
    CREATE TABLE context_events (
        ts TEXT, agent TEXT, harness TEXT, model TEXT, session_id TEXT,
        event TEXT, effective_window_tokens INTEGER, protected_identity_tokens INTEGER,
        working_context_tokens INTEGER, used_percentage REAL, band TEXT,
        brain_injection_tokens INTEGER, tool_output_tokens INTEGER,
        checkpoint_status TEXT, compaction INTEGER, rehydration_status TEXT,
        reduced_working_memory INTEGER, mode TEXT, dedupe_key TEXT UNIQUE
    );
"""


def new_conn(tmp: str, last_ingest="2026-09-05T13:30:00Z"):
    conn = sqlite3.connect(Path(tmp) / "meter.db")
    conn.executescript(DDL)
    conn.execute("INSERT INTO meta VALUES(?,?)", ("last_ingest", last_ingest))
    return conn


def add_call(conn, key, agent="ops", source="router", model="local/qwen", ok=1,
             roster=None, run=None, corr=None):
    conn.execute("INSERT INTO llm_calls VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("2026-09-05T13:00:00Z", agent, source, model, 1, 1, 0, 0, 0, 1, "",
                  roster, run, corr, ok, 1, key))


class EfficiencyScorecardTests(unittest.TestCase):
    def test_scorecard_labels_observed_attention_without_sending(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = new_conn(tmp)
            for i in range(10):
                add_call(conn, f"router-{i}", agent="unknown" if i < 3 else "ops", ok=0 if i < 2 else 1)
                conn.execute("INSERT INTO context_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             ("2026-09-05T13:00:00Z", "coder", "claude-code", "model-a", "s",
                              "checkpoint", 100, 1, 99, 90 if i < 2 else 40, "", 0, 0, "", 0, "", 0,
                              "active", f"ctx-{i}"))
            add_call(conn, "prov-kimi", agent="researcher", source="provider", model="kimi-k2")
            conn.commit()
            card = efficiency.build_scorecard(
                conn, now=NOW, codex_policy={"available": True, "default_model": "gpt-5.1-codex"},
                provider_policy=efficiency.provider_policy_from_models(["gpt-5.1"]))
            by_id = {s["id"]: s for s in card["signals"]}
            self.assertEqual("read_only", card["mode"])
            self.assertEqual("preview_only", card["alert_delivery"])
            self.assertEqual("attention", by_id["router_attribution"]["status"])
            self.assertEqual("attention", by_id["router_call_failures"]["status"])
            self.assertEqual("attention", by_id["model_policy_observation"]["status"])
            self.assertEqual("attention", by_id["context_pressure"]["status"])
            self.assertEqual("unavailable", by_id["retry_waste"]["status"])
            self.assertEqual("clear", by_id["pipeline_freshness"]["status"])
            self.assertEqual(4, card["summary"]["attention_signals"])
            conn.close()

    def test_model_policy_is_unavailable_until_configured(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = new_conn(tmp)
            add_call(conn, "p1", source="provider", model="gpt-5.1")
            card = efficiency.build_scorecard(conn, now=NOW)
            by_id = {s["id"]: s for s in card["signals"]}
            self.assertEqual("unavailable", by_id["model_policy_observation"]["status"])
            conn.close()

    def test_stale_pipeline_needs_attention(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = new_conn(tmp, last_ingest="2026-09-05T01:00:00Z")
            card = efficiency.build_scorecard(conn, now=NOW)
            by_id = {s["id"]: s for s in card["signals"]}
            self.assertEqual("attention", by_id["pipeline_freshness"]["status"])
            conn.close()

    def test_policy_override_changes_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = new_conn(tmp)
            for i in range(10):
                add_call(conn, f"r{i}", ok=0 if i == 0 else 1, roster="ops", run="run-a", corr=f"c-{i}")
            by_default = {s["id"]: s for s in efficiency.build_scorecard(conn, now=NOW)["signals"]}
            self.assertEqual("attention", by_default["router_call_failures"]["status"])  # 10% > 5%
            relaxed = efficiency.build_scorecard(conn, now=NOW, policy={"router_failure_attention_rate": 0.5})
            by_id = {s["id"]: s for s in relaxed["signals"]}
            self.assertEqual("clear", by_id["router_call_failures"]["status"])
            conn.close()

    def test_writer_emits_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            conn = new_conn(tmp, last_ingest="2026-09-05T13:59:00Z")
            card = efficiency.write_scorecard(conn, root / "out" / "scorecard.json",
                                              root / "out" / "scorecard.md", now=NOW)
            self.assertEqual("llm-cost-meter.efficiency.v0", card["schema"])
            self.assertIn('"mode": "read_only"', (root / "out" / "scorecard.json").read_text())
            self.assertIn("# Efficiency scorecard", (root / "out" / "scorecard.md").read_text())
            conn.close()

    def test_scorecard_measures_only_explicit_retry_and_dispatch_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = new_conn(tmp, last_ingest="2026-09-05T13:59:00Z")
            for i in range(2):
                add_call(conn, f"router-evidence-{i}", roster="ops", run="dispatch-a", corr="correlation-a")
            conn.execute("INSERT INTO dispatch_events VALUES(?,?,?,?,?,?,?)",
                         ("2026-09-05T13:00:00Z", "ops", "dispatch-a", "correlation-a", "msg-a", "[]", "d1"))
            card = efficiency.build_scorecard(conn, now=NOW, codex_policy={"available": True},
                                              provider_policy={"available": True, "allowed_models": []})
            by_id = {s["id"]: s for s in card["signals"]}
            self.assertEqual("clear", by_id["retry_waste"]["status"])
            self.assertEqual(1, by_id["retry_waste"]["observed"]["retry_groups"])
            self.assertEqual("clear", by_id["automation_dispatch_attribution"]["status"])
            self.assertEqual(1, by_id["automation_dispatch_attribution"]["observed"]["complete_evidence_records"])
            conn.close()


if __name__ == "__main__":
    unittest.main()
