"""生成无数据库、无模型调用的开发预测校准教学输入。"""
import argparse
from datetime import date, timedelta
import json
from pathlib import Path


def prepare(output):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    for number in range(8):
        day = date(2025, 1, 6) + timedelta(days=number)
        stamp = day.isoformat() + "T15:00:00+08:00"
        label_end = (day + timedelta(days=1)).isoformat() + "T15:00:00+08:00"
        for entity in ("synthetic_a", "synthetic_b"):
            actual = (number + 1) * (0.001 if entity == "synthetic_a" else -0.001)
            rows.append({"candidate_id": "synthetic_model", "fold_id": "development_1", "sample_id": entity + ":" + day.isoformat(),
                         "entity_id": entity, "observation_session": day.isoformat(), "prediction": 2 * actual, "actual": actual,
                         "label_available_time": label_end, "decision_time": stamp, "feature_available_time": stamp,
                         "label_start_time": stamp, "label_end_time": label_end, "stage": "validation", "horizon_sessions": 1,
                         "score_semantics": "raw_return_prediction"})
    fixture = {"fixture": "synthetic", "design": {"holdout_start": "2025-02-01T00:00:00+08:00"}, "rows": rows}
    request = {
        "campaign_id": "synthetic_prediction_calibration", "session_root": str(output / "session"),
        "source": {"kind": "synthetic", "path": str(output / "development-fixture.json")},
        "development": {"start": "2025-01-06", "end": "2025-01-13", "as_of": "2025-01-15T15:00:00+08:00", "fold_ids": ["development_1"], "horizon_sessions": 1},
        "candidates": [{"id": "baseline", "model_candidate_id": "synthetic_model", "shrinkage": 1.0},
                       {"id": "half", "model_candidate_id": "synthetic_model", "shrinkage": 0.5}],
        "baseline_id": "baseline", "budget": {"rounds": 2, "evaluations": 2, "model_calls": 0, "output_tokens": 0,
                                              "max_output_tokens_per_call": 0, "max_rows": 100, "memory_bytes": 8388608},
        "stop": {"target_mse": 0, "min_improvement": 0, "patience": 2}, "proposer": {"mode": "fixed_policy"},
    }
    for name, value in (("development-fixture.json", fixture), ("request.json", request)):
        (output / name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    return request


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    prepare(args.output)
    print(json.dumps({"status": "prepared", "output": args.output, "fixture": "synthetic"}))
