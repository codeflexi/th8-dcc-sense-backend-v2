"""
C.3.5 — Technical Selection Service (LOCKED)
----------------------------------
Contract:
- Facts are OWNED by group_id only
- NO case_id + fact_key lookup
- PO is OPTIONAL context, never anchor
- Group anchor = (anchor_type='PO_ITEM', anchor_id=dcc_case_line_items.item_id)

This service must be deterministic, auditable, and production-safe.

Enterprise rule:
- NO Repo() without sb injection
- Services own repositories via injected sb (single lifecycle)
"""

from typing import Dict, Any, List, Optional

from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.case_fact_repo import CaseFactRepository

from app.services.policy.registry import PolicyRegistry
from app.services.policy.resolver import resolve_domain_policy


class SelectionService:
    """
    Technical Selection (C3.5)
    - Deterministic selection of baseline technique per group_id
    - Consumes:
        - dcc_case_evidence_groups (groups for case)
        - dcc_case_facts (facts owned by group_id)
        - dcc_case_evidences (evidence attached to group_id)
        - dcc_case_line_items (optional PO context via anchor_id)
    """

    def __init__(self, *, sb):
        # IMPORTANT: enforce single sb lifecycle; never construct repos without sb
        self.sb = sb
        self.case_line_repo = CaseLineItemRepository(sb)
        self.group_repo = CaseEvidenceGroupRepository(sb)
        self.evidence_repo = CaseEvidenceRepository(sb)
        self.fact_repo = CaseFactRepository(sb)

    # =====================================================
    # Public API
    # =====================================================
    def select_for_case(self, case_id: str, domain_code: str) -> Dict[str, Any]:
        policy = PolicyRegistry.get_bundle()
        resolved_policy = resolve_domain_policy(policy, domain_code)

        # YAML you shared: meta.defaults.currency
        currency_default = (
            getattr(getattr(getattr(policy, "meta", None), "defaults", None), "currency", None)
            or (getattr(getattr(policy, "meta", None), "currency_default", None))  # backward compat
            or "THB"
        )

        # PO is optional context (never anchor)
        po_lines = self.case_line_repo.list_by_case(case_id)
        po_by_item_id = self._index_po_lines_by_item_id(po_lines)

        # groups are case-scoped (anchor lives in group)
        groups = self.group_repo.list_by_case(case_id)

        results: List[Dict[str, Any]] = []
        for group in groups:
            group_ctx = self._build_group_context(
                group=group,
                po_by_item_id=po_by_item_id,
                currency_default=currency_default,
                domain_code=domain_code,
            )
            selection = self._select_for_group(group_ctx, resolved_policy, domain_code)
            results.append(selection)

        return {"case_id": case_id, "domain": domain_code, "groups": results}

    # =====================================================
    # Core Selection
    # =====================================================
    def _select_for_group(
        self,
        group_ctx: Dict[str, Any],
        resolved_policy,
        domain_code: str,
    ) -> Dict[str, Any]:
        profile = getattr(resolved_policy, "profile", {}) or {}

        # -------- baseline_priority safe --------
        if isinstance(profile, dict):
            baseline_priority = profile.get("baseline_priority", []) or []
        else:
            baseline_priority = getattr(profile, "baseline_priority", []) or []

        # -------- techniques safe --------
        techniques = getattr(resolved_policy, "techniques", None)

        # fallback: policy.domains[domain_code].techniques
        if not techniques:
            bundle = PolicyRegistry.get_bundle()
            domains = getattr(bundle, "domains", None) or {}
            domain = domains.get(domain_code)
            if domain:
                techniques = getattr(domain, "techniques", None)

        if not isinstance(techniques, dict):
            techniques = {}

        trace: List[Dict[str, Any]] = []
        selected = None

        # Deterministic: UNGROUPED never attempts baselines
        if group_ctx.get("group_key") == "UNGROUPED":
            selected = self._fallback()
            trace.append(selected)
            return self._result(group_ctx, selected, trace)

        for tech_id in baseline_priority:
            tech = techniques.get(tech_id)
            if not tech:
                # keep deterministic: skip missing technique id
                continue

            r = self._evaluate_technique(group_ctx, tech)
            trace.append(r)
            if r.get("passed") is True:
                selected = r
                break

        if not selected:
            selected = self._fallback()
            trace.append(selected)

        return self._result(group_ctx, selected, trace)

    def _fallback(self) -> Dict[str, Any]:
        return {
            "technique_id": "T_NO_BASELINE_ESCALATE",
            "passed": True,
            "baseline": None,
            "baseline_source": None,
            "fail_reasons": [],
            "references": {},
        }

    def _result(self, ctx: Dict[str, Any], selected: Dict[str, Any], trace: List[Dict[str, Any]]) -> Dict[str, Any]:
        gk = ctx.get("group_key") or ""

        return {
            "group_id": ctx["group_id"],
            "group_key": {
                "sku": gk if isinstance(gk, str) and gk.startswith("SKU:") else None,
                "name": None,
            },
            "selected_technique": selected.get("technique_id"),
            "baseline": selected.get("baseline"),
            "baseline_source": selected.get("baseline_source"),
            "readiness_flags": {
                "baseline_available": selected.get("baseline") is not None,
                "evidence_present": len(ctx.get("evidences") or []) > 0,
                "currency_present": bool(ctx.get("currency")),
                # audit signal only
                "po_line_found": bool(ctx.get("po_line")),
            },
            "selection_trace": trace,
        }

    # =====================================================
    # Technique Evaluation
    # =====================================================
    def _evaluate_technique(self, ctx: Dict[str, Any], tech) -> Dict[str, Any]:
        tech_id = self._tech_id(tech)

        # required facts
        required_facts = self._tech_required_facts(tech)
        for ft in required_facts:
            if ft not in (ctx.get("facts") or {}):
                return self._fail(tech_id, [f"MISSING_FACT:{ft}"])

        # gates
        gates = self._tech_gates(tech)
        if gates:
            gate_err = self._check_gates(ctx, gates)
            if gate_err:
                return self._fail(tech_id, gate_err)

        # baseline derive
        category = self._tech_category(tech)
        if category == "BASELINE":
            return self._derive(ctx, tech)

        return {
            "technique_id": tech_id,
            "passed": True,
            "baseline": None,
            "baseline_source": None,
            "fail_reasons": [],
            "references": {},
        }

    # =====================================================
    # Gates / Derive
    # =====================================================
    def _check_gates(self, ctx: Dict[str, Any], gates: Dict[str, Any]) -> List[str]:
        errs: List[str] = []

        # currency gate MUST NOT depend on PO
        if gates.get("currency_match") is True:
            if not ctx.get("currency"):
                errs.append("CURRENCY_MISSING")

        # evidence confidence gate (optional, safe)
        if "min_confidence" in gates:
            cfg = gates.get("min_confidence") or {}
            et = cfg.get("evidence_type", "PRICE")
            threshold = float(cfg.get("threshold", 0) or 0)

            evids = ctx.get("evidences") or []
            typed = [e for e in evids if e.get("evidence_type") == et]

            if not typed:
                errs.append(f"MISSING_EVIDENCE:{et}")
            else:
                best = max(typed, key=lambda x: float(x.get("confidence", 0) or 0))
                if float(best.get("confidence", 0) or 0) < threshold:
                    errs.append(f"EVIDENCE_CONFIDENCE_BELOW_THRESHOLD:{best.get('confidence')}")

        return errs

    def _derive(self, ctx: Dict[str, Any], tech) -> Dict[str, Any]:
        tech_id = self._tech_id(tech)
        cfg = self._tech_derive(tech)
        ft = cfg.get("baseline_from")
        fact = (ctx.get("facts") or {}).get(ft)

        if not ft:
            return self._fail(tech_id, ["BASELINE_FROM_MISSING"])
        if not fact:
            return self._fail(tech_id, [f"MISSING_FACT:{ft}"])

        vj = fact.get("value_json") or {}
        value = vj.get("price")
        if value is None:
            return self._fail(tech_id, ["PRICE_VALUE_MISSING"])

        # currency precedence:
        currency = vj.get("currency") or ctx.get("currency")

        # method_required
        method_required = cfg.get("method_required")
        if method_required and vj.get("method") != method_required:
            return self._fail(tech_id, ["FACT_METHOD_MISMATCH"])

        return {
            "technique_id": tech_id,
            "passed": True,
            "baseline": {"value": value, "currency": currency},
            "baseline_source": {"fact_type": ft, "method": vj.get("method")},
            "fail_reasons": [],
            "references": {
                "fact_ids": [fact.get("fact_id")],
                "evidence_ids": fact.get("source_evidence_ids", []) or [],
            },
        }

    # =====================================================
    # Context (LOCKED)
    # =====================================================
    def _build_group_context(
        self,
        *,
        group: Dict[str, Any],
        po_by_item_id: Dict[str, Dict[str, Any]],
        currency_default: str,
        domain_code: str,
    ) -> Dict[str, Any]:
        group_id = group["group_id"]
        group_key = group.get("group_key")

        # anchor fields (enterprise-grade)
        anchor_type = group.get("anchor_type")
        anchor_id = group.get("anchor_id")

        # CRITICAL: evidences are owned by group_id (same as facts)
        evidences = self.evidence_repo.list_by_group_id(group_id)

        # CRITICAL: facts are OWNED by group_id only
        facts = self.fact_repo.list_by_group(group_id)
        fact_map = {f["fact_type"]: f for f in (facts or [])}

        # PO context: ONLY via anchor_id (item_id)
        po_line = None
        if anchor_type == "PO_ITEM" and anchor_id:
            po_line = po_by_item_id.get(anchor_id)

        # Currency resolution:
        currency = (
            (po_line or {}).get("currency")
            or self._fact_currency(fact_map)
            or currency_default
        )

        return {
            "domain": domain_code,
            "group_id": group_id,
            "group_key": group_key,
            "anchor_type": anchor_type,
            "anchor_id": anchor_id,
            "po_line": po_line,
            "evidences": evidences,
            "facts": fact_map,
            "currency": currency,
        }

    def _fact_currency(self, facts: Dict[str, dict]) -> Optional[str]:
        for f in (facts or {}).values():
            c = (f.get("value_json") or {}).get("currency")
            if c:
                return c
        return None

    def _index_po_lines_by_item_id(self, po_lines: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        PO index must be item_id-based only (anchor contract)
        """
        idx: Dict[str, Dict[str, Any]] = {}
        for l in po_lines or []:
            item_id = l.get("item_id")
            if item_id:
                idx[item_id] = l
        return idx

    # =====================================================
    # Technique access helpers (dict/object safe)
    # =====================================================
    def _tech_id(self, tech) -> str:
        if isinstance(tech, dict):
            return str(tech.get("id") or tech.get("technique_id") or "")
        return str(getattr(tech, "id", "") or getattr(tech, "technique_id", "") or "")

    def _tech_required_facts(self, tech) -> List[str]:
        if isinstance(tech, dict):
            return tech.get("required_facts", []) or []
        return getattr(tech, "required_facts", []) or []

    def _tech_gates(self, tech) -> Dict[str, Any]:
        if isinstance(tech, dict):
            return tech.get("gates", {}) or {}
        return getattr(tech, "gates", {}) or {}

    def _tech_category(self, tech) -> Optional[str]:
        if isinstance(tech, dict):
            return tech.get("category")
        return getattr(tech, "category", None)

    def _tech_derive(self, tech) -> Dict[str, Any]:
        if isinstance(tech, dict):
            return tech.get("derive", {}) or {}
        return getattr(tech, "derive", None) or {}

    # =====================================================
    # Helpers
    # =====================================================
    def _fail(self, tech_id: str, reasons: List[str]) -> Dict[str, Any]:
        return {
            "technique_id": tech_id,
            "passed": False,
            "baseline": None,
            "baseline_source": None,
            "fail_reasons": reasons,
            "references": {},
        }