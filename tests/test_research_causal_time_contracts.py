from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research_pipeline.platform.causal_time import (
    CausalTimeContractError,
    attach_core_feature_time_facts,
    build_core_feature_time_facts,
    validate_label_time_facts,
)
from research_pipeline.runtime import ExternalArtifactStore, RuntimeIntegrityError
from research_pipeline.evidence.errors import EvidenceContractError
from research_pipeline.evidence.verification_result import (
    _verify_causal_time_result_tables,
)


def _at(day: int, hour: int = 15) -> datetime:
    return datetime(2024, 1, day, hour, tzinfo=timezone.utc)


def _feature_source() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "code": ["000001.XSHE", "000001.XSHE"],
            "decision_time": [_at(2, 16), _at(3, 16)],
            "observation_at": [_at(2), _at(3)],
            "available_at": [_at(2, 15), _at(3, 15)],
            "source_partition_id": ["2024-01-02", "2024-01-03"],
        }
    )


def test_feature_rejects_observation_after_actual_decision() -> None:
    source = _feature_source()
    source.loc[0, "observation_at"] = _at(2, 17)

    with pytest.raises(CausalTimeContractError, match="观测时间晚于决策时间"):
        build_core_feature_time_facts(
            source,
            key_columns=("date", "code"),
            decision_time_column="decision_time",
            observation_time_column="observation_at",
            available_time_column="available_at",
            source_partition_column="source_partition_id",
        )


def test_feature_rejects_source_available_after_actual_decision() -> None:
    source = _feature_source()
    source.loc[0, "available_at"] = _at(2, 17)

    with pytest.raises(CausalTimeContractError, match="可见时间晚于决策时间"):
        build_core_feature_time_facts(
            source,
            key_columns=("date", "code"),
            decision_time_column="decision_time",
            observation_time_column="observation_at",
            available_time_column="available_at",
            source_partition_column="source_partition_id",
        )


def test_feature_rejects_naive_actual_source_time() -> None:
    source = _feature_source()
    source["observation_at"] = source["observation_at"].astype(object)
    source.loc[0, "observation_at"] = datetime(2024, 1, 2, 15)

    with pytest.raises(CausalTimeContractError, match="必须是带时区时间"):
        build_core_feature_time_facts(
            source,
            key_columns=("date", "code"),
            decision_time_column="decision_time",
            observation_time_column="observation_at",
            available_time_column="available_at",
            source_partition_column="source_partition_id",
        )


@pytest.mark.parametrize(
    "protected_column",
    ("max_source_observation_time", "max_source_available_time"),
)
def test_project_values_cannot_override_core_feature_time_facts(
    protected_column: str,
) -> None:
    facts = build_core_feature_time_facts(
        _feature_source(),
        key_columns=("date", "code"),
        decision_time_column="decision_time",
        observation_time_column="observation_at",
        available_time_column="available_at",
        source_partition_column="source_partition_id",
    )
    project_values = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "code": ["000001.XSHE", "000001.XSHE"],
            "feature_value": [1.0, 2.0],
            protected_column: [_at(1), _at(1)],
        }
    )

    with pytest.raises(CausalTimeContractError, match="扩展不得填写核心时间事实"):
        attach_core_feature_time_facts(
            project_values,
            facts,
            key_columns=("date", "code"),
        )


def test_project_feature_values_must_match_core_fact_keys_exactly() -> None:
    facts = build_core_feature_time_facts(
        _feature_source(),
        key_columns=("date", "code"),
        decision_time_column="decision_time",
        observation_time_column="observation_at",
        available_time_column="available_at",
        source_partition_column="source_partition_id",
    )
    project_values = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04"],
            "code": ["000001.XSHE", "000001.XSHE"],
            "feature_value": [1.0, 2.0],
        }
    )

    with pytest.raises(CausalTimeContractError, match="行键不闭合"):
        attach_core_feature_time_facts(
            project_values,
            facts,
            key_columns=("date", "code"),
        )


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


def test_verifier_rejects_tampered_feature_and_label_row_times() -> None:
    feature = pd.DataFrame(
        {
            "date": ["2024-01-02"],
            "code": ["000001.XSHE"],
            "decision_time": [_at(2, 16)],
            "max_source_observation_time": [_at(2, 17)],
            "max_source_available_time": [_at(2, 15)],
            "source_partition_ids": [["part-1"]],
        }
    )
    with pytest.raises(EvidenceContractError, match="源观测时间晚于决策时间"):
        _verify_causal_time_result_tables(
            _result_snapshot_for_causal_table(
                feature,
                artifact_type="research.feature-set.v1",
            )
        )

    label = pd.DataFrame(
        {
            "observation_at": [_at(2)],
            "code": ["000001.XSHE"],
            "decision_time": [_at(2)],
            "first_actual_observation_time": [_at(2)],
            "last_actual_observation_time": [_at(3)],
            "available_time": [_at(2, 23)],
        }
    )
    with pytest.raises(EvidenceContractError, match="实际首观测必须严格晚于决策"):
        _verify_causal_time_result_tables(
            _result_snapshot_for_causal_table(
                label,
                artifact_type="research.label.v1",
            )
        )


def test_verifier_rejects_duplicate_keys_missing_table_and_missing_time_schema() -> None:
    row = {
        "date": "2024-01-02",
        "code": "000001.XSHE",
        "decision_time": _at(2, 16),
        "max_source_observation_time": _at(2),
        "max_source_available_time": _at(2, 15),
        "source_partition_ids": ["part-1"],
    }
    duplicated = pd.DataFrame([row, row])
    with pytest.raises(EvidenceContractError, match="行键不唯一"):
        _verify_causal_time_result_tables(
            _result_snapshot_for_causal_table(
                duplicated,
                artifact_type="research.feature-set.v1",
            )
        )

    with pytest.raises(EvidenceContractError, match="缺少正式 Feature/Label 逐行表"):
        _verify_causal_time_result_tables(
            _result_snapshot_for_causal_table(
                pd.DataFrame([row]),
                artifact_type="research.feature-set.v1",
                include_table=False,
            )
        )

    missing_field = pd.DataFrame([row]).drop(columns="source_partition_ids")
    with pytest.raises(EvidenceContractError, match="缺少逐行时间字段"):
        _verify_causal_time_result_tables(
            _result_snapshot_for_causal_table(
                missing_field,
                artifact_type="research.feature-set.v1",
            )
        )


def test_verifier_streams_causal_rows_and_detects_cross_batch_duplicate() -> None:
    schema_id = "research.causal-time.lazy-fixture.v2"
    manifest = SimpleNamespace(
        artifact_type="research.feature-set.v1",
        schema_id=schema_id,
    )
    base_row = {
        "date": "2024-01-02",
        "code": "000001.XSHE",
        "decision_time": _at(2, 16),
        "max_source_observation_time": _at(2),
        "max_source_available_time": _at(2, 15),
        "source_partition_ids": ["part-1"],
    }

    class LazySnapshot:
        tables = {}
        bundle = SimpleNamespace(tables=(manifest,))

        def __init__(self, table: pa.Table) -> None:
            self._table = table

        def table_schema(self, requested_schema_id: str):
            assert requested_schema_id == schema_id
            return self._table.schema

        def iter_table_batches(self, requested_schema_id: str, **kwargs):
            assert requested_schema_id == schema_id
            columns = kwargs.get("columns")
            selected = self._table if columns is None else self._table.select(columns)
            return iter(selected.to_batches(max_chunksize=1))

        def read_table(self, *_args, **_kwargs):
            raise AssertionError("因果时间复核不得整表读取")

    distinct = dict(base_row)
    distinct["code"] = "000002.XSHE"
    _verify_causal_time_result_tables(
        LazySnapshot(pa.Table.from_pylist([base_row, distinct]))
    )

    with pytest.raises(EvidenceContractError, match="行键不唯一"):
        _verify_causal_time_result_tables(
            LazySnapshot(pa.Table.from_pylist([base_row, base_row]))
        )


def test_event_window_is_not_treated_as_formal_label() -> None:
    event_window = pd.DataFrame(
        {
            "event_id": ["event-1"],
            "relative_session": [-1],
            "session": ["2024-01-02"],
            "abnormal_return": [0.01],
        }
    )
    _verify_causal_time_result_tables(
        _result_snapshot_for_causal_table(
            event_window,
            artifact_type="research.event-window.v1",
        )
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
    facts = build_core_feature_time_facts(
        _feature_source(),
        key_columns=("date", "code"),
        decision_time_column="decision_time",
        observation_time_column="observation_at",
        available_time_column="available_at",
        source_partition_column="source_partition_id",
    )

    commit = store.commit(
        staging,
        artifact_name="features",
        artifact_type="research.feature-set.v1",
        producer_scope="project",
        core_time_facts=facts,
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
