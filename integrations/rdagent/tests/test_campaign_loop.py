"""实际RD调度的跨轮反馈和中断恢复，使用合成输入及离线模型。"""
import asyncio
import json
import sys

import pytest
from test_campaign import prepare, live_payload
from quantwitness_rdagent import campaign

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="RD-Agent运行于Linux环境")


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    from rdagent.core.conf import RD_AGENT_SETTINGS
    from rdagent.log.conf import LOG_SETTINGS
    from rdagent.log import rdagent_logger
    payload = prepare(tmp_path)
    root = campaign.Path(payload["session_root"])
    monkeypatch.setattr(RD_AGENT_SETTINGS, "artifact_signing_key_path", root / "rd-signing.key")
    monkeypatch.setattr(LOG_SETTINGS, "trace_path", str(root / "rd-logs"))
    rdagent_logger.set_storages_path(root / "rd-logs")
    return payload


def test_snapshot_resume_after_evaluation_does_not_recompute(runtime, monkeypatch):
    from quantwitness_rdagent.campaign_loop import ResearchCampaignLoop, run_campaign
    session = campaign.Campaign(runtime)
    loop = ResearchCampaignLoop(session)
    asyncio.run(loop.run(step_n=2, loop_n=2))
    assert not session.history()
    assert (session.root / "rd-logs/__session__/0/1_evaluate").exists()
    baseline = session.root / "rounds/0000/evaluation.json"
    before = baseline.read_bytes(), baseline.stat().st_mtime_ns
    original = campaign.evaluate_candidate
    evaluated = []
    def evaluate(rows, candidate, folds):
        assert candidate["id"] != "baseline", "恢复不可重评已完成基准"
        evaluated.append(candidate["id"])
        return original(rows, candidate, folds)
    monkeypatch.setattr(campaign, "evaluate_candidate", evaluate)
    outcome = run_campaign(runtime, resume=True)
    assert outcome["status"] == "completed"
    assert evaluated == ["half"]
    assert before == (baseline.read_bytes(), baseline.stat().st_mtime_ns)
    files = {p.relative_to(session.root): (p.read_bytes(), p.stat().st_mtime_ns) for p in session.root.rglob("*") if p.is_file()}
    assert run_campaign(runtime, resume=True) == outcome
    assert files == {p.relative_to(session.root): (p.read_bytes(), p.stat().st_mtime_ns) for p in session.root.rglob("*") if p.is_file()}


def test_actual_loop_live_proposer_receives_finished_feedback(runtime, monkeypatch):
    from quantwitness_rdagent import model_client
    from quantwitness_rdagent.campaign_loop import run_campaign
    payload = live_payload(runtime)
    calls = []
    monkeypatch.setattr(model_client, "public_config", lambda p: {"model": "fixture", "base_url": "https://example.invalid/v1"})
    def response(env, prompt, maximum, **kwargs):
        facts = json.loads(prompt)
        assert len(facts["history"]) == 1
        assert facts["history"][0]["status"] == "evaluated"
        assert facts["history"][0]["metrics"]["mse"] > 0
        assert "reason" not in facts["history"][0]
        calls.append(facts)
        return {"model": "fixture", "text": json.dumps({"action": "evaluate", "candidate_id": "half", "parent_id": "baseline", "reason": "检验幅度收缩"}), "usage": {"output_tokens": 10}}
    monkeypatch.setattr(model_client, "request_text", response)
    outcome = run_campaign(payload, model_env_file="fixture.env")
    assert len(calls) == 1 and outcome["selected_development_mse"] == 0
    assert run_campaign(payload, resume=True, model_env_file="fixture.env") == outcome
    assert len(calls) == 1
