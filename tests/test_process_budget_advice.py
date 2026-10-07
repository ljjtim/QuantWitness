"""进程建议只消费环境事实和失败载荷，不运行探针。"""

from __future__ import annotations

from copy import deepcopy
import importlib

import pytest


budget = importlib.import_module("research_pipeline.platform.process_budget_advice")


@pytest.mark.parametrize(
    "platform_name,venv,version,purpose,expected",
    [
        ("linux", False, (3, 10), "worker", 2),
        ("linux", True, (3, 10), "worker", 2),
        ("win32", False, (3, 10), "worker", 3),
        ("win32", True, (3, 10), "worker", 4),
        ("win32", True, (3, 11), "worker", 3),
        ("win32", False, (3, 11), "worker", 2),
        ("win32", True, (3, 10), "verifier", 3),
        ("win32", False, (3, 10), "verifier", 2),
        ("darwin", True, (3, 13), "verifier", 2),
    ],
)
def test_startup_advice_uses_documented_process_tree(
    monkeypatch, platform_name, venv, version, purpose, expected,
):
    import subprocess

    def reject_probe(*args, **kwargs):
        pytest.fail("预算建议不得启动进程探针")

    monkeypatch.setattr(subprocess, "Popen", reject_probe)
    monkeypatch.setattr(budget.sys, "platform", platform_name)
    monkeypatch.setattr(budget.sys, "prefix", "venv" if venv else "base")
    monkeypatch.setattr(budget.sys, "base_prefix", "base")
    monkeypatch.setattr(budget.sys, "version_info", version)
    advice = budget.process_budget_advice(1, purpose=purpose)
    assert advice["declared_slots"] == 1
    assert advice["startup_slots"] == expected
    assert advice["suggested_slots"] == expected
    assert advice["status"] == "configuration_insufficient"
    assert ("windows_python310_version_query" in advice["sources"]) == (
        platform_name == "win32" and version == (3, 10) and purpose == "worker"
    )
    assert "硬上限" in advice["message"]
    if purpose == "verifier":
        assert advice["parameter"] == "--verification-process-slots"


def test_larger_declared_budget_is_not_reduced():
    advice = budget.process_budget_advice(12, purpose="worker")
    assert advice["suggested_slots"] == 12
    assert advice["declared_slots"] == 12
    assert advice["status"] == "startup_budget_sufficient"


@pytest.mark.parametrize("slots", [0, -1, None, True, "3", 2.5])
def test_invalid_budget_returns_diagnostic_without_raising(slots):
    advice = budget.process_budget_advice(slots, purpose="worker")
    assert advice["status"] == "invalid_declared_slots"
    assert advice["suggested_slots"] is None


def test_invalid_purpose_returns_diagnostic():
    assert budget.process_budget_advice(2, purpose="other")["status"] == "invalid_purpose"


def test_measured_failure_retains_each_actual_limit_and_does_not_mutate():
    payload = {
        "code": "resource_exceeded",
        "exceeded": {
            "process_slots": {"actual": 5, "limit": 3},
            "memory_bytes": {"actual": 2048, "limit": 1024},
            "scratch_bytes": {"actual": 8192, "limit": 4096},
        },
        "resource_measurement_status": "available",
        "process_cleanup_status": "complete",
    }
    before = deepcopy(payload)
    advice = budget.resource_failure_advice(payload, purpose="worker")
    assert advice["status"] == "measured_exceeded"
    assert advice["exceeded"] == {
        "process_slots": {"actual": 5, "limit": 3, "suggested_limit": 5},
        "memory_bytes": {"actual": 2048, "limit": 1024, "suggested_limit": 2048},
        "scratch_bytes": {"actual": 8192, "limit": 4096, "suggested_limit": 8192},
    }
    assert payload == before


def test_measurement_failure_is_not_reported_as_budget_shortage():
    advice = budget.resource_failure_advice(
        {"resource_measurement_status": "measurement_unavailable"},
        error_code="project_worker_measurement_unavailable",
    )
    assert advice["status"] == "measurement_unavailable"
    assert advice["exceeded"] == {}


def test_process_failure_without_counts_does_not_invent_measurement():
    advice = budget.resource_failure_advice(
        {"process_cleanup_status": "complete"},
        error_code="project_verifier_process_slots_exceeded", purpose="verifier",
    )
    assert advice["status"] == "process_slots_exceeded"
    assert advice["exceeded"] == {}
    assert advice["parameter"] == "--verification-process-slots"


def test_unrelated_failure_has_no_budget_advice():
    assert budget.resource_failure_advice({}, error_code="value_error") is None
