from __future__ import annotations

from typing import Any, Dict, List

from app.services.context.models import (
    ArtifactFlags,
    ContextDecisionItemView,
    DriverInfo,
    FailAction,
    ItemIdentity,
    PriceInfo,
    QuantityFlags,
    QuantityInfo,
    RuleView,
    StatusInfo,
)


def _upper(v: Any) -> str:
    return str(v or "").upper()


class BaseAdapter:
    """
    Adapter responsibilities:
    - Normalize field names (risk -> risk_level, name -> item_name, po/gr/inv -> ordered/received/invoiced)
    - Normalize rule objects (ensure calculation dict, fail_actions list objects)
    - Derive drivers deterministically when missing
    - NO business logic recalculation (no tolerance, no variance recompute beyond basic aggregation)
    """

    domain: str = ""

    def detect_domain(self, raw: Dict[str, Any]) -> str:
        if raw.get("domain"):
            return str(raw["domain"])
        rules = raw.get("rules") or []
        if isinstance(rules, list) and rules:
            d = rules[0].get("domain")
            if d:
                return str(d)
        return self.domain or ""

    def to_item_view(self, raw: Dict[str, Any]) -> ContextDecisionItemView:
        raise NotImplementedError

    # ------------------------
    # Normalizers
    # ------------------------

    def norm_status(self, raw: Dict[str, Any]) -> StatusInfo:
        s = raw.get("status") or {}
        risk_level = s.get("risk_level") or s.get("risk") or "LOW"
        return StatusInfo(
            decision=_upper(s.get("decision") or "REVIEW"),  # type: ignore
            risk_level=_upper(risk_level),                   # type: ignore
            confidence=float(s.get("confidence") or 0.0),
        )

    def norm_item_identity(self, raw: Dict[str, Any]) -> ItemIdentity:
        it = raw.get("item") or {}
        return ItemIdentity(
            sku=str(it.get("sku") or ""),
            item_name=str(it.get("item_name") or it.get("name") or ""),
            uom=str(it.get("uom") or ""),
        )

    def norm_quantity(self, raw: Dict[str, Any]) -> QuantityInfo:
        q = raw.get("quantity") or {}
        flags = q.get("flags") or {}

        ordered = q.get("ordered", q.get("po", 0.0))
        received = q.get("received", q.get("gr", 0.0))
        invoiced = q.get("invoiced", q.get("inv", 0.0))

        return QuantityInfo(
            ordered=float(ordered or 0.0),
            received=float(received or 0.0),
            invoiced=float(invoiced or 0.0),
            over_gr_qty=float(q.get("over_gr_qty") or 0.0),
            over_inv_qty=float(q.get("over_inv_qty") or 0.0),
            flags=QuantityFlags(
                gr_exceeds_po=bool(flags.get("gr_exceeds_po", False)),
                inv_exceeds_gr=bool(flags.get("inv_exceeds_gr", False)),
                inv_without_gr=bool(flags.get("inv_without_gr", False)),
            ),
        )

    def norm_price(self, raw: Dict[str, Any]) -> PriceInfo:
        p = raw.get("price") or {}
        baseline_unit = p.get("baseline_unit", None)
        has_baseline = bool(p.get("has_baseline", baseline_unit is not None))

        within_tol = p.get("within_tolerance")
        if within_tol is None:
            within_tol = True

        return PriceInfo(
            context=str(p.get("context") or "UNKNOWN"),
            currency=str(p.get("currency") or "THB"),
            po_unit=float(p.get("po_unit") or 0.0),
            inv_unit=float(p.get("inv_unit") or 0.0),
            grn_unit=float(p.get("grn_unit") or 0.0),
            baseline_unit=(float(baseline_unit) if baseline_unit is not None else None),
            has_baseline=has_baseline,
            variance_pct=float(p.get("variance_pct") or 0.0),
            variance_abs=float(p.get("variance_abs") or 0.0),
            tolerance_abs=float(p.get("tolerance_abs") or 0.0),
            within_tolerance=bool(within_tol),
        )

    def norm_artifacts(self, raw: Dict[str, Any]) -> ArtifactFlags:
        a = raw.get("artifacts") or {}
        return ArtifactFlags(
            po=bool(a.get("po", False)),
            grn=bool(a.get("grn", False)),
            invoice=bool(a.get("invoice", False)),
        )

    def norm_rules(self, raw: Dict[str, Any], domain_fallback: str) -> List[RuleView]:
        rules = raw.get("rules") or []
        out: List[RuleView] = []
        if not isinstance(rules, list):
            return out

        for r in rules:
            if not isinstance(r, dict):
                continue

            calc = r.get("calculation") or {}
            if calc is None:
                calc = {}

            fa = r.get("fail_actions") or []
            fa_out: List[FailAction] = []
            if isinstance(fa, list):
                for x in fa:
                    if isinstance(x, dict) and x.get("type"):
                        fa_out.append(FailAction(type=str(x["type"]), meta=x.get("meta") or {}))
                    elif isinstance(x, str):
                        fa_out.append(FailAction(type=x, meta={}))

            out.append(
                RuleView(
                    rule_id=str(r.get("rule_id") or ""),
                    group=str(r.get("group") or ""),
                    domain=str(r.get("domain") or domain_fallback),
                    result=_upper(r.get("result") or "INFO"),     # type: ignore
                    severity=_upper(r.get("severity") or "LOW"),  # type: ignore
                    exec_message=str(r.get("exec_message") or ""),
                    audit_message=str(r.get("audit_message") or ""),
                    calculation=calc,
                    fail_actions=fa_out,
                    reason_codes=r.get("reason_codes") or [],
                    extra=r.get("extra") or {},
                )
            )
        return out

    # ------------------------
    # Drivers (deterministic)
    # ------------------------

    def _severity_rank(self, v: str) -> int:
        m = {"CRITICAL": 4, "HIGH": 3, "MED": 2, "LOW": 1}
        return m.get(str(v or "").upper(), 0)

    def enrich_drivers(self, drivers: List[DriverInfo], rules: List[RuleView]) -> List[DriverInfo]:
        by_id = {x.rule_id: x for x in rules if x.rule_id}
        out: List[DriverInfo] = []
        for d in drivers:
            if d.rule_id and d.rule_id in by_id:
                r = by_id[d.rule_id]
                if not d.group:
                    d.group = r.group
                if not d.severity:
                    d.severity = r.severity
                if not d.exec_message:
                    d.exec_message = r.exec_message
                if not d.audit_message:
                    d.audit_message = r.audit_message
                if not d.label:
                    d.label = r.exec_message or r.rule_id
            out.append(d)
        return out

    def derive_default_drivers(self, rules: List[RuleView], *, max_items: int = 3) -> List[DriverInfo]:
        fails = [r for r in rules if str(r.result).upper() == "FAIL"]
        # deterministic ordering: severity desc, then rule_id asc
        fails.sort(key=lambda r: (-self._severity_rank(r.severity), r.rule_id))
        top = fails[:max_items]
        return [
            DriverInfo(
                type="RULE",
                rule_id=r.rule_id,
                group=r.group,
                severity=r.severity,
                label=r.exec_message or r.rule_id,
                exec_message=r.exec_message,
                audit_message=r.audit_message,
            )
            for r in top
        ]

    def norm_drivers(self, raw: Dict[str, Any], rules: List[RuleView]) -> List[DriverInfo]:
        ds = raw.get("drivers") or []
        out: List[DriverInfo] = []
        if isinstance(ds, list):
            for d in ds:
                if not isinstance(d, dict):
                    continue
                out.append(
                    DriverInfo(
                        type=str(d.get("type") or "RULE"),
                        rule_id=d.get("rule_id"),
                        group=d.get("group"),
                        severity=d.get("severity"),
                        label=str(d.get("label") or ""),
                        exec_message=str(d.get("exec_message") or ""),
                        audit_message=str(d.get("audit_message") or ""),
                    )
                )
        if not out:
            out = self.derive_default_drivers(rules)
        return self.enrich_drivers(out, rules)