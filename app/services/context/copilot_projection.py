from __future__ import annotations

from typing import List

from app.services.context.models import (
    ArtifactFlags,
    CopilotContextLite,
    CopilotKeyNumbers,
    CopilotLiteItem,
    CopilotSignals,
    CopilotTopRule,
    ContextDecisionView,
)


def _severity_rank(v: str) -> int:
    m = {"CRITICAL": 4, "HIGH": 3, "MED": 2, "LOW": 1}
    return m.get(str(v or "").upper(), 0)


def project_copilot_lite(
    view: ContextDecisionView,
    *,
    max_items: int = 20,
    max_drivers_per_item: int = 3,
    max_top_rules_per_item: int = 5,
) -> CopilotContextLite:
    """
    Copilot Lite projection:
    - summary + signals
    - per-item key numbers
    - drivers limited
    - top FAIL rules limited (with calculation + fail_actions)
    Deterministic ordering.
    """
    items_out: List[CopilotLiteItem] = []

    blocking_rule_count = 0
    failing_item_count = 0

    # Worst-case readiness = AND across items
    readiness = ArtifactFlags(po=True, grn=True, invoice=True)
    for it in view.items:
        readiness.po = readiness.po and bool(it.artifacts.po)
        readiness.grn = readiness.grn and bool(it.artifacts.grn)
        readiness.invoice = readiness.invoice and bool(it.artifacts.invoice)

    for it in view.items[:max_items]:
        fail_rules = [r for r in it.rules if str(r.result).upper() == "FAIL"]
        fail_rules.sort(key=lambda r: (-_severity_rank(r.severity), r.rule_id))
        top_rules = fail_rules[:max_top_rules_per_item]

        blocking_rule_count += len(fail_rules)
        if fail_rules:
            failing_item_count += 1

        drivers = (it.drivers or [])[:max_drivers_per_item]

        key_numbers = CopilotKeyNumbers(
            quantity={
                "ordered": it.quantity.ordered,
                "received": it.quantity.received,
                "invoiced": it.quantity.invoiced,
                "over_gr_qty": it.quantity.over_gr_qty,
                "over_inv_qty": it.quantity.over_inv_qty,
                "flags": {
                    "gr_exceeds_po": it.quantity.flags.gr_exceeds_po,
                    "inv_exceeds_gr": it.quantity.flags.inv_exceeds_gr,
                    "inv_without_gr": it.quantity.flags.inv_without_gr,
                },
            },
            price={
                "context": it.price.context,
                "currency": it.price.currency,
                "po_unit": it.price.po_unit,
                "inv_unit": it.price.inv_unit,
                "baseline_unit": it.price.baseline_unit,
                "has_baseline": it.price.has_baseline,
                "variance_pct": it.price.variance_pct,
                "variance_abs": it.price.variance_abs,
                "within_tolerance": it.price.within_tolerance,
                "tolerance_abs": it.price.tolerance_abs,
            },
        )

        items_out.append(
            CopilotLiteItem(
                group_id=it.group_id,
                domain=it.domain,
                item=it.item,
                status=it.status,
                key_numbers=key_numbers,
                drivers=drivers,
                top_rules=[
                    CopilotTopRule(
                        rule_id=r.rule_id,
                        group=r.group,
                        domain=r.domain,
                        result=r.result,
                        severity=r.severity,
                        calculation=r.calculation,
                        fail_actions=r.fail_actions,
                    )
                    for r in top_rules
                ],
                artifacts=it.artifacts,
            )
        )

    signals = CopilotSignals(
        blocking_rule_count=blocking_rule_count,
        failing_item_count=failing_item_count,
        artifact_readiness=readiness,
    )

    return CopilotContextLite(
        view_version=view.view_version,
        case_id=view.case_id,
        run_id=view.run_id,
        policy=view.policy,
        created_at=view.created_at,
        summary=view.summary,
        signals=signals,
        items=items_out,
    )