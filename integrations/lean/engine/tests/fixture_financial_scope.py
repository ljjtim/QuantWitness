"""为真实 Launcher 场景构建冻结的交易日历、规则和结算来源。"""
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta

from research_pipeline.domain import SessionPolicyBundle, load_session_policy_bundle
from research_pipeline.domain.minute_rule_snapshots import (
    PROJECT_RULE_BUNDLE_VERSION, MinuteRuleSnapshotBundle, load_minute_rule_snapshot_bundle,
)
from research_pipeline.domain.minute_scope_input import write_minute_scope_input
from research_pipeline.domain.session_calendar import PROJECT_SESSION_BUNDLE_VERSION, PROJECT_SESSION_POLICY_VERSION
from research_pipeline.platform import typed_canonical_hash
from research_pipeline.runtime.lean_strategy_inputs import frozen_rule_files
from types import SimpleNamespace


def _seal(payload, cls):
    payload.pop("bundle_hash", None)
    payload["bundle_hash"] = typed_canonical_hash(payload)
    return cls.from_dict(payload)


def _calendar(spec, asset):
    days = list(spec["dates"])
    if asset == "cn_future":
        days.append((date.fromisoformat(days[-1]) + timedelta(days=1)).isoformat())
    visible = (date.fromisoformat(days[0]) - timedelta(days=2)).isoformat() + "T00:00:00+08:00"
    original = load_session_policy_bundle().to_dict()
    prototype = next(p for p in original["policies"] if p["instrument"]["asset_class"] == asset and p["revision"] == 1)
    policies = []
    for code in sorted(spec["symbols"]):
        p = deepcopy(prototype)
        source = typed_canonical_hash({"dates": days, "code": code})
        p.update(policy_id="session.synthetic.v1", revision=1, contract_version=PROJECT_SESSION_POLICY_VERSION,
                 previous_policy_hash=None, effective_from=visible[:10], effective_to=days[-1],
                 available_at=visible, trading_dates=days)
        p["instrument"].update(instrument_id=code, metadata_hash=source)
        p["evidence_hashes"] = [r for r in p["evidence_hashes"] if r["source_id"].startswith("minute.capability.")]
        p["evidence_hashes"].append({"source_id": "synthetic", "content_hash": source})
        if asset == "cn_future":
            session = next(s for s in spec["sessions"] if s["instrument_id"] == code)
            day = date.fromisoformat(session["trading_date"])
            p["segments"] = []
            for index, segment in enumerate(session["segments"]):
                start, end = (datetime.fromisoformat(segment[k]) for k in ("start_at", "end_at"))
                offset = (start.date() - day).days
                p["segments"].append(dict(segment_id=f"segment_{index}", start_day_offset=offset,
                    start_time=start.time().isoformat(), end_day_offset=(end.date() - day).days,
                    end_time=end.time().isoformat(), phase="night" if offset < 0 else "day",
                    bar_eligible=True, bucket_anchor_day_offset=offset, bucket_anchor_time=start.time().isoformat(),
                    contract_version="minute-session-segment-template-v1"))
        policies.append(p)
    original.update(bundle_id="synthetic.calendar", contract_version=PROJECT_SESSION_BUNDLE_VERSION, policies=policies)
    return _seal(original, SessionPolicyBundle), visible


def frozen_financial_day(output, settings, spec, samples, binding):
    asset = settings["declaration"]["asset_class"]
    future = asset == "cn_future"
    prefix = {"cn_future": "cn_futures", "cn_stock": "cn_stock", "cn_etf": "cn_fund"}[asset]
    calendar, visible = _calendar(spec, asset)
    rows = []
    revisions = Counter()
    def add(code, kind, params, first, last, at):
        revisions[code, kind] += 1
        rows.append(dict(rule_id=f"rule.{prefix}.{kind}.v1", instrument_id=code, revision=revisions[code, kind],
                         asset_class=asset, status="supported", effective_from=first, effective_to=last,
                         available_at=at, source_ids=["synthetic"], parameters=params, unsupported_reason=None,
                         contract_version="minute-rule-snapshot-v1"))
    for day, (header, _) in samples.items():
        for row in header["rules"]:
            code, at, params = row["instrument_id"], row["available_at"], row["parameters"]
            if future:
                c = spec["contracts"][code]
                groups = dict(contract_lifecycle=dict(product_code=c["product"], listed_date=c["listed_date"],
                                  last_trade_date=c["expiry_date"], exit_deadline=c["exit_deadline"]),
                              contract_multiplier=dict(contract_multiplier=int(c["multiplier"])),
                              price_tick=dict(price_scale=c["price_scale"], price_tick_units=1),
                              price_limit=dict(price_scale=c["price_scale"], low_limit_units=int(params["lower_limit"]),
                                  high_limit_units=int(params["upper_limit"]), reference_price_available_at=at),
                              margin={k:v for k,v in params.items() if k.endswith("margin_ppm")},
                              fee_schedule={k:v for k,v in params.items() if "_fee_" in k},
                              delivery_expiry=dict(close_priority=params["close_priority"],exit_deadline=c["exit_deadline"]),
                              actual_contract_mapping=dict(actual_contract_id=code))
            else:
                groups = {k: {} for k in ("instrument_lifecycle", "lot_size", "suspension", "settlement",
                                          "trading_fee", "price_limit", "adjustment_factor_snapshot")}
                for name, value in params.items():
                    if name == "paused":
                        kind = "suspension"
                    elif name == "settlement_days":
                        kind = "settlement"
                    elif name in ("stock_board", "listing_phase", "trading_status", "is_st"):
                        kind = "instrument_lifecycle"
                    elif name in ("commission_ppm", "min_commission_units", "sell_tax_ppm", "transfer_fee_ppm"):
                        kind = "trading_fee"
                    elif name.startswith(("buy_", "sell_")):
                        kind = "lot_size"
                    else:
                        kind = "price_limit"
                    groups[kind][name] = value
            groups["session"] = dict(session_policy_id="session.synthetic.v1",session_policy_revision=1)
            for kind, values in groups.items():
                add(code, kind, values, day, day, at)
        for settlement in header.get("settlements", ()):
            add(settlement["instrument_id"], "settlement", dict(price_scale=0,
                settlement_price_units=int(settlement["price"]),
                settlement_availability_semantics="next_session_open_after_1700"),
                day, day, settlement["available_at"])
    rules = load_minute_rule_snapshot_bundle().to_dict()
    rules.update(bundle_id="synthetic.rules", contract_version=PROJECT_RULE_BUNDLE_VERSION,
                 instruments=[p.instrument.to_dict() for p in calendar.policies if any(r["instrument_id"] == p.instrument.instrument_id for r in rows)],
                 sources=[dict(source_id=name, source_kind=kind, locator=locator, published_on=visible[:10],
                               claim_ceiling="research_observation", contract_version="minute-rule-source-v1")
                          for name,kind,locator in [("calendar","repository_snapshot","fixture#"+calendar.bundle_hash),
                                                   ("synthetic","repository_evidence","fixture")]],
                 rules=sorted(rows,key=lambda r:(r["instrument_id"],r["rule_id"],r["revision"])))
    scope = write_minute_scope_input(output, name="market", calendar=calendar, rules=_seal(rules, MinuteRuleSnapshotBundle))
    spec.update(asset_class=asset, source_kind="frozen_financial_day")
    settings["minute_scope"] = scope
    if not future:
        spec["rules_parquet"] = frozen_rule_files(SimpleNamespace(root=output, reference=scope))
    if spec["frequency"] == "1m" and not future:
        spec["source_kind"] = "frozen_minute_parquet"
        return
    market = None
    if spec["frequency"] == "1d":
        data = binding["direct"]
        market = dict(root=data["root"], reference=data["reference"], plan=data["plan"],
                      open_reference=data["opening"]["reference"], open_plan=data["opening"]["plan"], source_identity="fixture")
        if future:
            data["futures"] = dict(calendar=calendar.to_dict(), symbols=spec["symbols"])
    spec["financial_day"] = dict(scope_root=str(output),scope=scope,market=market,
                                 memory_bytes=64*1024**2,settlements=None,lifecycle=settings.get("lifecycle",{}))
