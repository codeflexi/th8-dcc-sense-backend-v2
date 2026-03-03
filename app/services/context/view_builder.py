from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.services.adapters.registry import AdapterRegistry
from app.services.context.models import (
    ContextDecisionView,
    ContextDecisionItemView,
    ContextSummaryView,
    ExposureInfo,
    PolicyRef,
    ReasonCodeCount,
)


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def _detect_domain(raw_item: Dict[str, Any]) -> str:
    if raw_item.get("domain"):
        return str(raw_item["domain"])
    rules = raw_item.get("rules") or []
    if isinstance(rules, list) and rules:
        d = rules[0].get("domain")
        if d:
            return str(d)
    return ""


def _severity_rank(v: str) -> int:
    m = {"CRITICAL": 4, "HIGH": 3, "MED": 2, "LOW": 1}
    return m.get(str(v or "").upper(), 0)


def _decision_rank(v: str) -> int:
    # worst-wins
    m = {"REJECT": 4, "ESCALATE": 3, "REVIEW": 2, "APPROVE": 1}
    return m.get(str(v or "").upper(), 0)


def build_decision_view(raw_bundle: Dict[str, Any]) -> ContextDecisionView:
    """
    Build canonical decision context (v2) from raw bundle.
    Deterministic aggregation, no domain-specific branching here.
    """
    case_id = str(raw_bundle.get("case_id") or "")
    run_id = raw_bundle.get("run_id")
    policy_id = str(raw_bundle.get("policy_id") or "")
    policy_version = str(raw_bundle.get("policy_version") or "")
    technique = str(raw_bundle.get("technique") or "")
    created_at = _parse_dt(raw_bundle.get("created_at"))

    results = raw_bundle.get("results") or []
    items: List[ContextDecisionItemView] = []
    
    

    if isinstance(results, list):
        for r in results:
            if not isinstance(r, dict):
                continue
            domain = _detect_domain(r)
            adapter = AdapterRegistry.get(domain)
            item = adapter.to_item_view(r)
            # preserve detected domain label (important for unknown domains)
            if domain and item.domain != domain:
                item.domain = domain
            items.append(item)

    summary = _build_summary_from_items(items, raw_summary=raw_bundle.get("summary"))

    if not created_at:
        created_at = _max_item_created_at(items)

    return ContextDecisionView(
        view_version="v1",
        case_id=case_id,
        run_id=run_id,
        policy=PolicyRef(policy_id=policy_id, policy_version=policy_version),
        technique=technique,
        created_at=created_at,
        summary=summary,
        items=items,
    )


def _max_item_created_at(items: List[ContextDecisionItemView]) -> Optional[datetime]:
    dts = [x.created_at for x in items if x.created_at]
    if not dts:
        return None
    try:
        return max(dts)
    except Exception:
        return None


def _build_summary_from_items(items: List[ContextDecisionItemView], raw_summary: Any = None) -> ContextSummaryView:
    """
    If raw_summary exists, normalize it to our contract (Option 3C exposure).
    Otherwise derive deterministically from items.
    """
    if isinstance(raw_summary, dict) and raw_summary:
        overall = str(raw_summary.get("overall_decision") or "REVIEW").upper()
        risk_level = str(raw_summary.get("risk_level") or raw_summary.get("risk") or "LOW").upper()
        confidence_avg = float(raw_summary.get("confidence_avg") or 0.0)
        item_count = int(raw_summary.get("item_count") or len(items))

        exp = raw_summary.get("exposure") or {}
        totals = {}
        metrics = {}

        # accept old format: {currency, unit_variance_sum}
        ccy = exp.get("currency")
        uvs = exp.get("unit_variance_sum")
        if ccy and uvs is not None:
            totals[str(ccy)] = float(uvs)
            metrics["unit_variance_sum"] = float(uvs)
        else:
            totals = exp.get("totals") or {}
            metrics = exp.get("metrics") or {}

        trc = raw_summary.get("top_reason_codes") or []
        top_reason_codes: List[ReasonCodeCount] = []
        if isinstance(trc, list):
            for x in trc:
                if isinstance(x, dict) and x.get("code"):
                    top_reason_codes.append(ReasonCodeCount(code=str(x["code"]), count=int(x.get("count") or 0)))
                elif isinstance(x, str):
                    top_reason_codes.append(ReasonCodeCount(code=x, count=1))

        return ContextSummaryView(
            overall_decision=overall,  # type: ignore
            risk_level=risk_level,     # type: ignore
            confidence_avg=confidence_avg,
            item_count=item_count,
            exposure=ExposureInfo(
                totals={k: float(v) for k, v in (totals or {}).items()},
                metrics=dict(metrics or {}),
            ),
            top_reason_codes=top_reason_codes,
        )

    # derive
    item_count = len(items)
    confidence_avg = (sum(float(i.status.confidence or 0.0) for i in items) / item_count) if item_count else 0.0

    overall = "REVIEW"
    risk = "LOW"
    for it in items:
        d = str(it.status.decision or "REVIEW").upper()
        rl = str(it.status.risk_level or "LOW").upper()
        if _decision_rank(d) > _decision_rank(overall):
            overall = d
        if _severity_rank(rl) > _severity_rank(risk):
            risk = rl

    totals: Dict[str, float] = {}
    unit_variance_sum = 0.0
    for it in items:
        ccy = it.price.currency or "THB"
        vabs = float(it.price.variance_abs or 0.0)
        totals[ccy] = float(totals.get(ccy, 0.0) + vabs)
        unit_variance_sum += vabs

    top_reason_codes = _derive_top_reason_codes(items)

    return ContextSummaryView(
        overall_decision=overall,  # type: ignore
        risk_level=risk,          # type: ignore
        confidence_avg=float(confidence_avg),
        item_count=item_count,
        exposure=ExposureInfo(totals=totals, metrics={"unit_variance_sum": float(unit_variance_sum)}),
        top_reason_codes=top_reason_codes,
    )


def _derive_top_reason_codes(items: List[ContextDecisionItemView], max_codes: int = 10) -> List[ReasonCodeCount]:
    # Count FAIL rule_id occurrences deterministically
    counter: Dict[str, int] = {}
    for it in items:
        for r in it.rules or []:
            if str(r.result).upper() == "FAIL":
                counter[r.rule_id] = counter.get(r.rule_id, 0) + 1

    pairs = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    return [ReasonCodeCount(code=k, count=v) for k, v in pairs[:max_codes]]