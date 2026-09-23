"""为四个公开示例生成一次性的合成 DuckDB 与 Catalog Lock。"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path

import duckdb
import yaml

from research_pipeline.catalog import (
    DuckDBSourceInspector,
    compile_catalog,
    load_declarative_catalog,
    validate_contract_set,
)

try:
    from .equity_cross_section.synthetic import rows as equity_rows
    from .etf_time_series.synthetic import rows as etf_rows
    from .event_study.synthetic import events as event_rows
    from .event_study.synthetic import prices as event_price_rows
    from .futures_term_structure.synthetic import contracts as futures_rows
except ImportError:
    from equity_cross_section.synthetic import rows as equity_rows
    from etf_time_series.synthetic import rows as etf_rows
    from event_study.synthetic import events as event_rows
    from event_study.synthetic import prices as event_price_rows
    from futures_term_structure.synthetic import contracts as futures_rows


SOURCE_PROFILE = "source"
ENVIRONMENT = "test"


def _date(value: object) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _time(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError("合成数据时间必须带时区")
    return parsed


def _write_database(path: Path) -> dict[str, int]:
    equity = equity_rows()
    etf = etf_rows()
    events = event_rows()
    prices = event_price_rows()
    futures = futures_rows()
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "CREATE TABLE synthetic_equity_daily("
            "session DATE NOT NULL, instrument VARCHAR NOT NULL, close DOUBLE NOT NULL, "
            "available_at TIMESTAMPTZ NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO synthetic_equity_daily VALUES (?, ?, ?, ?)",
            [
                (_date(row["session"]), row["instrument"], row["close"], _time(row["available_at"]))
                for row in equity
            ],
        )
        connection.execute(
            "CREATE TABLE synthetic_etf_daily("
            "session DATE NOT NULL, instrument VARCHAR NOT NULL, open DOUBLE NOT NULL, "
            "close DOUBLE NOT NULL, available_at TIMESTAMPTZ NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO synthetic_etf_daily VALUES (?, ?, ?, ?, ?)",
            [
                (
                    _date(row["session"]), row["instrument"], row["open"],
                    row["close"], _time(row["available_at"]),
                )
                for row in etf
            ],
        )
        connection.execute(
            "CREATE TABLE synthetic_event_records("
            "row_id BIGINT NOT NULL, event_time DATE NOT NULL, kind VARCHAR NOT NULL, "
            "event_id VARCHAR, instrument VARCHAR NOT NULL, event_day DATE, revision BIGINT, "
            "decision_at TIMESTAMPTZ, surprise DOUBLE, session DATE, close DOUBLE, "
            "available_at TIMESTAMPTZ NOT NULL)"
        )
        records = [
            (
                index, _date(row["event_day"]), "event", row["event_id"],
                row["instrument"], _date(row["event_day"]), row["revision"],
                _time(row["decision_at"]), row["surprise"], None, None,
                _time(row["available_at"]),
            )
            for index, row in enumerate(events, start=1)
        ]
        records.extend(
            (
                index, _date(row["session"]), "price", None, row["instrument"],
                None, None, None, None, _date(row["session"]), row["close"],
                _time(row["available_at"]),
            )
            for index, row in enumerate(prices, start=len(records) + 1)
        )
        connection.executemany(
            "INSERT INTO synthetic_event_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            records,
        )
        connection.execute(
            "CREATE TABLE synthetic_futures_curve("
            "observation_day DATE NOT NULL, contract VARCHAR NOT NULL, expiry DATE NOT NULL, "
            "settlement DOUBLE NOT NULL, volume BIGINT NOT NULL, "
            "available_at TIMESTAMPTZ NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO synthetic_futures_curve VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    date(2024, 1, 4), row["contract"], _date(row["expiry"]),
                    row["settlement"], row["volume"], _time(row["available_at"]),
                )
                for row in futures
            ],
        )
    return {
        "quantwitness.synthetic_equity_daily": len(equity),
        "quantwitness.synthetic_etf_daily": len(etf),
        "quantwitness.synthetic_event_records": len(records),
        "quantwitness.synthetic_futures_curve": len(futures),
    }


def _raw_datasets() -> tuple[dict[str, object], ...]:
    return (
        {
            "dataset_id": "quantwitness.synthetic_equity_daily",
            "market": "cn_stock", "instrument_type": "equity", "frequency": "daily",
            "object_name": "synthetic_equity_daily",
            "primary_key": ["fld_qw_eq_session", "fld_qw_eq_instrument"],
            "event_time_field": "fld_qw_eq_session",
            "available_time_field": "fld_qw_eq_available_at",
            "fields": (
                ("fld_qw_eq_session", "quantwitness.equity.session", "session", "date32", "date", "day", False),
                ("fld_qw_eq_instrument", "quantwitness.equity.instrument", "instrument", "string", "identifier", "dimensionless", False),
                ("fld_qw_eq_close", "quantwitness.equity.close", "close", "float64", "price", "CNY", False),
                ("fld_qw_eq_available_at", "quantwitness.equity.available_at", "available_at", "timestamp", "datetime", "microsecond", False),
            ),
        },
        {
            "dataset_id": "quantwitness.synthetic_etf_daily",
            "market": "cn_etf", "instrument_type": "etf", "frequency": "daily",
            "object_name": "synthetic_etf_daily",
            "primary_key": ["fld_qw_etf_session", "fld_qw_etf_instrument"],
            "event_time_field": "fld_qw_etf_session",
            "available_time_field": "fld_qw_etf_available_at",
            "fields": (
                ("fld_qw_etf_session", "quantwitness.etf.session", "session", "date32", "date", "day", False),
                ("fld_qw_etf_instrument", "quantwitness.etf.instrument", "instrument", "string", "identifier", "dimensionless", False),
                ("fld_qw_etf_open", "quantwitness.etf.open", "open", "float64", "price", "CNY", False),
                ("fld_qw_etf_close", "quantwitness.etf.close", "close", "float64", "price", "CNY", False),
                ("fld_qw_etf_available_at", "quantwitness.etf.available_at", "available_at", "timestamp", "datetime", "microsecond", False),
            ),
        },
        {
            "dataset_id": "quantwitness.synthetic_event_records",
            "market": "cn_stock", "instrument_type": "equity", "frequency": "event",
            "object_name": "synthetic_event_records",
            "primary_key": ["fld_qw_event_time", "fld_qw_event_kind", "fld_qw_event_instrument", "fld_qw_event_row_id"],
            "event_time_field": "fld_qw_event_time",
            "available_time_field": "fld_qw_event_available_at",
            "fields": (
                ("fld_qw_event_row_id", "quantwitness.event.row_id", "row_id", "int64", "identifier", "dimensionless", False),
                ("fld_qw_event_time", "quantwitness.event.time", "event_time", "date32", "date", "day", False),
                ("fld_qw_event_kind", "quantwitness.event.kind", "kind", "string", "category", "dimensionless", False),
                ("fld_qw_event_id", "quantwitness.event.event_id", "event_id", "string", "identifier", "dimensionless", True),
                ("fld_qw_event_instrument", "quantwitness.event.instrument", "instrument", "string", "identifier", "dimensionless", False),
                ("fld_qw_event_day", "quantwitness.event.event_day", "event_day", "date32", "date", "day", True),
                ("fld_qw_event_revision", "quantwitness.event.revision", "revision", "int64", "revision", "dimensionless", True),
                ("fld_qw_event_decision_at", "quantwitness.event.decision_at", "decision_at", "timestamp", "datetime", "microsecond", True),
                ("fld_qw_event_surprise", "quantwitness.event.surprise", "surprise", "float64", "signal", "decimal", True),
                ("fld_qw_event_session", "quantwitness.event.price_session", "session", "date32", "date", "day", True),
                ("fld_qw_event_close", "quantwitness.event.price_close", "close", "float64", "price", "CNY", True),
                ("fld_qw_event_available_at", "quantwitness.event.available_at", "available_at", "timestamp", "datetime", "microsecond", False),
            ),
        },
        {
            "dataset_id": "quantwitness.synthetic_futures_curve",
            "market": "cn_future", "instrument_type": "future_contract", "frequency": "daily",
            "object_name": "synthetic_futures_curve",
            "primary_key": ["fld_qw_fut_observation_day", "fld_qw_fut_contract"],
            "event_time_field": "fld_qw_fut_observation_day",
            "available_time_field": "fld_qw_fut_available_at",
            "fields": (
                ("fld_qw_fut_observation_day", "quantwitness.futures.observation_day", "observation_day", "date32", "date", "day", False),
                ("fld_qw_fut_contract", "quantwitness.futures.contract", "contract", "string", "identifier", "dimensionless", False),
                ("fld_qw_fut_expiry", "quantwitness.futures.expiry", "expiry", "date32", "date", "day", False),
                ("fld_qw_fut_settlement", "quantwitness.futures.settlement", "settlement", "float64", "price", "CNY", False),
                ("fld_qw_fut_volume", "quantwitness.futures.volume", "volume", "int64", "volume", "contract", False),
                ("fld_qw_fut_available_at", "quantwitness.futures.available_at", "available_at", "timestamp", "datetime", "microsecond", False),
            ),
        },
    )


def _catalog_definition(database: Path) -> dict[str, object]:
    inspector = DuckDBSourceInspector(
        database, source_profile=SOURCE_PROFILE, environment=ENVIRONMENT
    )
    policies: list[dict[str, object]] = [
        {
            "policy_id": "drift.quantwitness.synthetic.v1",
            "policy_type": "schema_drift",
            "rules": {"schema_changed": "reject", "unknown": "reject"},
        },
        {
            "policy_id": "revision.quantwitness.synthetic.none.v1",
            "policy_type": "revision",
            "rules": {"mode": "none"},
        },
    ]
    datasets = []
    for raw in _raw_datasets():
        policy_id = f"available.{raw['dataset_id']}.v1"
        available_time_field = str(raw["available_time_field"])
        policies.append(
            {
                "policy_id": policy_id,
                "policy_type": "availability",
                "rules": {
                    "available_after": "source_available_at",
                    "available_time_field": available_time_field,
                    "missing_available_time": "reject",
                    "timezone": "Asia/Shanghai",
                },
            }
        )
        fields = []
        for field_id, logical, column, dtype, semantic, unit, nullable in raw["fields"]:
            fields.append(
                {
                    "field_id": field_id,
                    "logical_name": logical,
                    "physical_column": column,
                    "data_type": dtype,
                    "semantic_type": semantic,
                    "unit": unit,
                    "nullable": nullable,
                    "availability_policy": policy_id,
                    "observation_model": "market_event",
                    "observation_keys": {
                        "event_time_field": raw["event_time_field"],
                        "available_time_field": available_time_field,
                    },
                }
            )
        dataset = {
            key: value
            for key, value in raw.items()
            if key not in {"available_time_field", "fields"}
        }
        dataset.update(
            {
                "sort_order": list(raw["primary_key"]),
                "source_profile": SOURCE_PROFILE,
                "environment": ENVIRONMENT,
                "expected_schema_revision": inspector.observe_current_schema(
                    str(raw["object_name"])
                ).schema_revision,
                "available_time_policy": policy_id,
                "drift_policy_id": "drift.quantwitness.synthetic.v1",
                "revision_policy_id": "revision.quantwitness.synthetic.none.v1",
                "result_cardinality": "one_or_more",
                "fields": fields,
            }
        )
        datasets.append(dataset)
    return {
        "bundle_version": "catalog-bundle-v1",
        "coverage_slots": [
            {
                "slot_id": f"quantwitness_synthetic_{index}",
                "decision": "approved",
                "target_ids": [dataset["dataset_id"]],
            }
            for index, dataset in enumerate(datasets, start=1)
        ],
        "policies": policies,
        "datasets": datasets,
    }


def prepare(output_root: Path) -> dict[str, object]:
    output = output_root.resolve()
    if output.exists():
        raise ValueError("合成环境输出目录必须不存在")
    output.mkdir(parents=True)
    database = output / "quantwitness-synthetic.duckdb"
    row_counts = _write_database(database)
    definition_path = output / "catalog-definition.yaml"
    definition_path.write_text(
        yaml.safe_dump(_catalog_definition(database), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    proposed = load_declarative_catalog(
        (definition_path,), allow_generated_approvals=True
    )
    validate_contract_set(proposed.contracts)
    approvals_path = output / "catalog-approvals.yaml"
    approvals_path.write_text(
        yaml.safe_dump(
            {
                "approval_version": "catalog-approvals-v1",
                "decisions": [item.to_dict() for item in proposed.decisions],
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    approved = load_declarative_catalog(
        (definition_path,), approval_path=approvals_path
    )
    catalog_lock = output / "catalog-lock"
    compiled = compile_catalog(
        baseline=approved.baseline,
        expected_baseline_hash=approved.baseline.content_hash,
        manifest=approved.manifest,
        contracts=approved.contracts,
        decisions=approved.decisions,
        release_root=catalog_lock,
    )
    return {
        "schema_version": "quantwitness-synthetic-environment-v1",
        "status": "pass",
        "database": str(database),
        "catalog_definition": str(definition_path),
        "catalog_approvals": str(approvals_path),
        "catalog_lock": str(catalog_lock),
        "catalog_hash": compiled.catalog_hash,
        "dataset_ids": sorted(row_counts),
        "row_counts": row_counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="生成 QuantWitness 公开示例的一次性合成数据环境"
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(Path(args.output)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
