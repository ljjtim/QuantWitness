from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform.causal_time import (
    CausalTimeContractError,
    attach_core_time_facts_arrow,
    validate_label_time_facts,
)
from research_pipeline.runtime import ExternalArtifactStore, RuntimeIntegrityError
from research_pipeline.evidence.errors import EvidenceContractError


def _at(day: int, hour: int = 15) -> datetime:
    return datetime(2024, 1, day, hour, tzinfo=timezone.utc)


@pytest.mark.parametrize("change", [None, "duplicate", "missing", "type", "future", "partitions"])
def test_arrow_causal_join_preserves_exact_values_and_rejects_invalid_facts(change):
    declared = [_at(2, 16), _at(3, 16)]
    values = pa.table({
        "code": ["B", "A"],
        "value": pa.array(
            [Decimal("9007199254740993.11"), Decimal("9007199254740993.12")],
            type=pa.decimal128(20, 2),
        ),
    })
    facts = {
        "code": ["A", "B"],
        "decision_time": declared,
        "max_source_observation_time": declared,
        "max_source_available_time": declared,
        "source_partition_ids": [["first", "second"], ["third"]],
    }
    if change == "duplicate":
        values = values.set_column(0, "code", pa.array(["A", "A"]))
        facts["code"] = ["A", "A"]
    elif change == "missing":
        facts["code"] = ["A", "C"]
    elif change == "type":
        values = values.set_column(0, "code", pa.array([1, 2]))
        facts["code"] = ["01", "02"]
    elif change == "future":
        facts["max_source_available_time"] = [_at(2, 17), _at(3, 16)]
    elif change == "partitions":
        facts["source_partition_ids"] = [["second", "second"], ["third"]]
    if change:
        with pytest.raises(CausalTimeContractError):
            attach_core_time_facts_arrow(values, pa.table(facts), key_columns=("code",), is_feature=True)
    else:
        combined = attach_core_time_facts_arrow(
            values, pa.table(facts), key_columns=("code",), is_feature=True,
        )
        assert combined.select(["code", "value"]).equals(values)
        assert combined["max_source_available_time"].to_pylist() == declared[::-1]
        assert combined["source_partition_ids"].to_pylist() == [["third"], ["first", "second"]]



def test_label_requires_strict_actual_future_observations() -> None:
    row = pd.DataFrame(
        {
            "decision_time": [_at(2)],
            "first_actual_observation_time": [_at(3)],
            "last_actual_observation_time": [_at(4)],
            "available_time": [_at(4, 16)],
        }
    )
    validate_label_time_facts(row)

    invalid = row.copy()
    invalid.loc[0, "first_actual_observation_time"] = _at(2)
    with pytest.raises(CausalTimeContractError, match="实际首观测必须严格晚于决策"):
        validate_label_time_facts(invalid)


def test_label_rejects_inverted_actual_or_availability_time() -> None:
    row = pd.DataFrame(
        {
            "decision_time": [_at(2)],
            "first_actual_observation_time": [_at(3)],
            "last_actual_observation_time": [_at(4)],
            "available_time": [_at(4, 16)],
        }
    )
    inverted = row.copy()
    inverted.loc[0, "last_actual_observation_time"] = _at(2, 17)
    with pytest.raises(CausalTimeContractError, match="实际末观测不能早于实际首观测"):
        validate_label_time_facts(inverted)

    unavailable = row.copy()
    unavailable.loc[0, "available_time"] = _at(3, 17)
    with pytest.raises(CausalTimeContractError, match="可见时间不能早于实际末观测"):
        validate_label_time_facts(unavailable)


def _result_snapshot_for_causal_table(
    frame: pd.DataFrame,
    *,
    artifact_type: str,
    schema_id: str = "research.causal-time.fixture.v2",
    include_table: bool = True,
) -> SimpleNamespace:
    manifest = SimpleNamespace(
        artifact_type=artifact_type,
        schema_id=schema_id,
    )
    return SimpleNamespace(
        bundle=SimpleNamespace(tables=(manifest,)),
        tables={schema_id: pa.Table.from_pandas(frame, preserve_index=False)}
        if include_table
        else {},
    )










def test_project_formal_feature_commit_rejects_self_reported_time_facts(
    tmp_path: Path,
) -> None:
    store = ExternalArtifactStore(tmp_path / "external")
    staging = store.prepare()
    target = staging / "features" / "data.parquet"
    target.parent.mkdir()
    pq.write_table(
        pa.table(
            {
                "date": ["2024-01-02"],
                "code": ["000001.XSHE"],
                "feature_value": [1.0],
                "decision_time": [_at(2, 16)],
                "max_source_observation_time": [_at(2)],
                "max_source_available_time": [_at(2, 15)],
                "source_partition_ids": [["2024-01-02"]],
            }
        ),
        target,
    )

    with pytest.raises(RuntimeIntegrityError, match="项目扩展不得提交核心时间事实"):
        store.commit(
            staging,
            artifact_name="features",
            artifact_type="research.feature-set.v1",
            producer_scope="project",
        )


def test_project_formal_feature_commit_requires_core_time_context(
    tmp_path: Path,
) -> None:
    store = ExternalArtifactStore(tmp_path / "external")
    staging = store.prepare()
    target = staging / "features" / "data.parquet"
    target.parent.mkdir()
    pq.write_table(
        pa.table(
            {
                "date": ["2024-01-02"],
                "code": ["000001.XSHE"],
                "feature_value": [1.0],
            }
        ),
        target,
    )

    with pytest.raises(RuntimeIntegrityError, match="缺少核心伴随时间上下文"):
        store.commit(
            staging,
            artifact_name="features",
            artifact_type="research.feature-set.v1",
            producer_scope="project",
        )


def test_project_formal_feature_commit_attaches_core_time_context(
    tmp_path: Path,
) -> None:
    store = ExternalArtifactStore(tmp_path / "external")
    staging = store.prepare()
    target = staging / "features" / "data.parquet"
    target.parent.mkdir()
    pq.write_table(
        pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "date": ["2024-01-02", "2024-01-03"],
                    "code": ["000001.XSHE", "000001.XSHE"],
                    "feature_value": [1.0, 2.0],
                }
            ),
            preserve_index=False,
        ),
        target,
    )
    facts = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "code": ["000001.XSHE", "000001.XSHE"],
            "decision_time": [_at(2, 16), _at(3, 16)],
            "max_source_observation_time": [_at(2), _at(3)],
            "max_source_available_time": [_at(2, 15), _at(3, 15)],
            "source_partition_ids": [["2024-01-02"], ["2024-01-03"]],
        }
    )

    commit = store.commit(
        staging,
        artifact_name="features",
        artifact_type="research.feature-set.v1",
        producer_scope="project",
        core_time_facts=pa.Table.from_pandas(facts, preserve_index=False),
        causal_time_key_columns=("date", "code"),
    )

    committed = pq.read_table(
        store.objects_root / commit.semantic_hash / "features" / "data.parquet"
    ).to_pandas()
    assert list(committed.columns[-4:]) == [
        "decision_time",
        "max_source_observation_time",
        "max_source_available_time",
        "source_partition_ids",
    ]
