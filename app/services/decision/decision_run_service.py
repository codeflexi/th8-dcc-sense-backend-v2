# app/services/decision/decision_run_service.py
"""
C4 — Decision Run Service (Enterprise-grade, production patched)
---------------------------------------------------------------
LOCKED constraints respected:
- No redesign / no reset
- Deterministic, evidence-first, audit-grade
- Anchors are group_id-based
- C4 consumes C3.5 selection output (NO baseline re-derivation)
- Decision values MUST comply with DB constraint:
    CHECK decision IN ('APPROVE','REVIEW','ESCALATE','REJECT')

PATCH (critical fix):
- Force calculation list derived from Enterprise policy v1 shape:
    policy.domains[domain].calculations
  (Ignore legacy required_calculations that causes empty calc list -> missing variance_pct)
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import yaml

# Optional imports (do NOT break runtime)
try:
    from uuid import UUID
except Exception:
    UUID = None  # type: ignore

# IMPORTANT: keep imports safe; but we will NOT rely on required_calculations for v1
try:
    from app.services.policy.calculation_requirements import required_calculations  # type: ignore
except Exception:
    required_calculations = None  # type: ignore

try:
    from app.services.decision.calculation_service import CalculationService  # type: ignore
except Exception:
    try:
        from app.services.decision.calculation_service import CalculationService  # type: ignore
    except Exception:
        CalculationService = None  # type: ignore


SEVERITY_ORDER = {"LOW": 1, "MED": 2, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

# Run-level decisions constrained by DB
RUN_DECISIONS = {"APPROVE", "REVIEW", "ESCALATE", "REJECT"}
# Group-level decisions are internal
GROUP_DECISIONS = {"PASS", "REVIEW", "REJECT"}


class DecisionRunService:
    def __init__(
        self,
        *,
        run_repo,
        result_repo,
        group_repo,
        case_line_repo,
        doc_link_repo=None,  # optional; used for artifact presence (DOCUMENT)
        audit_repo=None,
        policy_path: str,
    ):
        self.run_repo = run_repo
        self.result_repo = result_repo
        self.group_repo = group_repo
        self.case_line_repo = case_line_repo
        self.doc_link_repo = doc_link_repo
        self.audit_repo = audit_repo
        self.policy = self._load_policy(policy_path)

    # =====================================================
    # Public API
    # =====================================================
    def run_case(
        self,
        *,
        case_id: str,
        domain_code: str,
        selection: Dict[str, Any],
        created_by: str = "SYSTEM",
    ) -> Dict[str, Any]:
        meta = self.policy.get("meta") or {}
        policy_id = str(meta.get("policy_name") or meta.get("policy_id") or "UNKNOWN_POLICY")
        policy_version = str(meta.get("version") or "UNKNOWN_VERSION")

        input_hash = self._compute_input_hash(case_id, policy_id, policy_version, selection)

        inputs_snapshot = {
            "case_id": case_id,
            "domain": domain_code,
            "policy": {"policy_id": policy_id, "policy_version": policy_version},
            "selection_summary": self._selection_summary(selection),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        inputs_snapshot = self._json_safe(inputs_snapshot)

        run = self.run_repo.create_run(
            case_id=case_id,
            policy_id=policy_id,
            policy_version=policy_version,
            input_hash=input_hash,
            created_by=created_by,
            inputs_snapshot=inputs_snapshot,
        )
        
        run_id = str(run["run_id"])
        print("RUN_ID")
        print(run_id)
        
        print("DOMAIN =", domain_code)
        print("POLICY DOMAINS =", list(self.policy.get("domains", {}).keys()))
        print("CALCS KEYS =", list(self.policy.get("domains", {}).get(domain_code, {}).get("calculations", {}).keys()))


        self._audit_emit(
            case_id=case_id,
            event_type="DECISION_RUN_STARTED",
            actor=created_by,
            run_id=run_id,
            payload={
                "run_id": run_id,
                "domain": domain_code,
                "policy_id": policy_id,
                "policy_version": policy_version,
                "input_hash": input_hash,
            },
        )

        try:
            selection_by_group = self._index_selection_by_group(selection, case_id, domain_code)

            po_lines = self.case_line_repo.list_by_case(case_id) or []
            po_by_item_id = {str(l.get("item_id")): l for l in po_lines if l.get("item_id") is not None}

            artifacts_present = self._detect_artifacts_present(case_id)
            groups = self.group_repo.list_by_case(case_id) or []

            group_results: List[Dict[str, Any]] = []
            for g in groups:
                group_results.append(
                    self._evaluate_group(
                        run_id=run_id,
                        case_id=case_id,
                        domain_code=domain_code,
                        group=g,
                        selection_by_group=selection_by_group,
                        po_by_item_id=po_by_item_id,
                        artifacts_present=artifacts_present,
                        created_by=created_by,
                    )
                )

            agg = self._aggregate_case(group_results)
            run_decision = self._normalize_run_decision(agg["decision"])
            risk_level = self._normalize_risk_level(agg["risk_level"])
            confidence = float(agg["confidence"])
            summary = self._json_safe(agg["summary"])
            
            summary_result = {
                
                "decision" : run_decision,
                "risk_level": risk_level,
                "confidence": confidence
            }

            self.run_repo.complete_run(
                run_id=run_id,
                decision=run_decision,   # MUST match DB check constraint
                risk_level=risk_level,
                confidence=confidence,
                summary=summary,
            )
            
            
            self.result_repo.sync_after_success(
                case_id,
                run_id,
                summary_result
            )

            
            response = {
                "run_id": run_id,
                "case_id": case_id,
                "domain": domain_code,
                "decision": run_decision,
                "risk_level": risk_level,
                "confidence": confidence,
                "groups": group_results,
            }

            self._audit_emit(
                case_id=case_id,
                event_type="DECISION_RUN_DONE",
                actor=created_by,
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "domain": domain_code,
                    "decision": run_decision,
                    "risk_level": risk_level,
                    "confidence": confidence,
                    "summary": summary,
                },
            )

            return self._json_safe(response)

        except Exception as e:
            self.run_repo.fail_run(run_id=run_id, error=str(e))
            self._audit_emit(
                case_id=case_id,
                event_type="DECISION_RUN_FAILED",
                actor=created_by,
                run_id=run_id,
                payload={"run_id": run_id, "error": str(e)},
            )
            raise

    # =====================================================
    # Group evaluation
    # =====================================================
    def _evaluate_group(
        self,
        *,
        run_id: str,
        case_id: str,
        domain_code: str,
        group: Dict[str, Any],
        selection_by_group: Dict[str, Dict[str, Any]],
        po_by_item_id: Dict[str, Dict[str, Any]],
        artifacts_present: set[str],
        created_by: str,
    ) -> Dict[str, Any]:
        group_id = str(group.get("group_id"))
        anchor_type = group.get("anchor_type")
        anchor_id = group.get("anchor_id")

        self._audit_emit(
            case_id=case_id,
            event_type="GROUP_EVAL_STARTED",
            actor=created_by,
            run_id=run_id,
            payload={"run_id": run_id, "group_id": group_id, "anchor_type": anchor_type, "anchor_id": anchor_id},
        )

        # PO context
        po_line = None
        if anchor_type == "PO_ITEM" and anchor_id:
            po_line = po_by_item_id.get(str(anchor_id))

        # PO missing => deterministic REVIEW
        if not po_line:
            decision_status = "REVIEW"
            risk_level = "HIGH"
            confidence = 0.20

            trace = self._json_safe(
                {
                    "policy": self._policy_meta(),
                    "inputs": {
                        "group_id": group_id,
                        "anchor_type": anchor_type,
                        "anchor_id": str(anchor_id) if anchor_id else None,
                        "po_line_found": False,
                        "artifacts_present": sorted(list(artifacts_present)),
                    },
                    "selection": None,
                    "calculations": {},
                    "rules": [],
                    "notes": ["PO_LINE_MISSING_FOR_GROUP"],
                }
            )

            self.result_repo.upsert_result(
                run_id=run_id,
                group_id=group_id,
                decision_status=decision_status,
                risk_level=risk_level,
                confidence=confidence,
                reason_codes=["PO_LINE_MISSING_FOR_GROUP"],
                fail_actions=[{"type": "REVIEW"}],
                trace=trace,
                evidence_refs={"fact_ids": [], "evidence_ids": []},
                created_by=created_by,
            )

            self._audit_emit(
                case_id=case_id,
                event_type="GROUP_DECISION_FINALIZED",
                actor=created_by,
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "decision": decision_status,
                    "risk_level": risk_level,
                    "confidence": confidence,
                    "reason_codes": ["PO_LINE_MISSING_FOR_GROUP"],
                },
            )

            return {"group_id": group_id, "decision": decision_status, "risk_level": risk_level, "confidence": confidence}

        # Selection (C3.5)
        sel = selection_by_group.get(group_id)
        baseline_ctx = self._baseline_from_selection(sel)
        readiness = (sel or {}).get("readiness_flags") or {}

        if baseline_ctx.get("baseline_available"):
            self._audit_emit(
                case_id=case_id,
                event_type="BASELINE_SELECTED",
                actor=created_by,
                run_id=run_id,
                payload={
                    "run_id": run_id,
                    "group_id": group_id,
                    "baseline": baseline_ctx.get("baseline"),
                    "baseline_source": baseline_ctx.get("baseline_source"),
                    "technique": baseline_ctx.get("selected_technique"),
                },
            )

        # =====================================================
        # Calculations (enterprise)
        # =====================================================
        calculated: Dict[str, Any] = {}
        calc_trace: List[Dict[str, Any]] = []
        calc_error: Optional[str] = None

        try:
            calc_defs = self._get_required_calcs(domain_code)
            print("CALC_DEFS_RAW =", calc_defs)
            
            if calc_defs and CalculationService:
                defaults = (self.policy.get("meta") or {}).get("defaults") or {}
                rounding = defaults.get("rounding") or {}

                calc_context = {
                    "meta": self.policy.get("meta") or {},
                    "po": {"unit_price": self._normalize_money(po_line.get("unit_price"), fallback_currency=defaults.get("currency"))},
                    "selection": {"baseline": baseline_ctx.get("baseline")},
                    "ap": (sel or {}).get("ap_context") or {},
                }

                calc_engine = CalculationService()
                calc_result = calc_engine.compute_all(calcs=calc_defs, ctx=calc_context, rounding=rounding)

                calculated = self._json_safe(getattr(calc_result, "values", {}) or {})
                print("CALCULATED_VALUES =", calculated)
                calc_trace = getattr(calc_result, "trace", []) or []

        except Exception as e:
            calc_error = str(e)
            calculated = {}

        # =====================================================
        # Rule evaluation
        # =====================================================
        rule_traces: List[Dict[str, Any]] = []
        fail_actions: List[Dict[str, Any]] = []

        rule_ctx = {
                "meta": self.policy.get("meta") or {},
                "po": {"unit_price": self._normalize_money(po_line.get("unit_price"))},
                "selection": {
                    "baseline": baseline_ctx.get("baseline"),
                    "baseline_source": baseline_ctx.get("baseline_source"),
                    "baseline_layer": baseline_ctx.get("baseline_layer"),
                    "baseline_source_tag": baseline_ctx.get("baseline_source_tag"),
                    "baselines": baseline_ctx.get("baselines") or {},
                },
                "baselines": baseline_ctx.get("baselines") or {},
                "calculated": calculated,
                "ap": (sel or {}).get("ap_context") or {},
            }
        self._active_fmt_ctx = rule_ctx

        for rule in self._iter_rules(domain_code):
            rt = self._eval_rule(
                rule=rule,
                po_line=po_line,
                baseline_ctx=baseline_ctx,
                artifacts_present=artifacts_present,
                readiness=readiness,
                calculated=calculated,
                rule_ctx=rule_ctx,
            )
            if rt:
                rule_traces.append(rt)
                if rt.get("result") == "FAIL":
                    fail_actions.extend(rt.get("fail_actions") or [])

        failed_rules = [r for r in rule_traces if r.get("result") == "FAIL"]
        reason_codes = [r.get("rule_id") for r in failed_rules if r.get("rule_id")]

        max_severity: Optional[str] = None
        for r in failed_rules:
            max_severity = self._max_severity(max_severity, r.get("severity"))

        if not failed_rules:
            decision_status = "PASS"
            risk_level = "LOW"
        else:
            if max_severity == "CRITICAL":
                decision_status = "REJECT"
                risk_level = "CRITICAL"
            else:
                decision_status = "REVIEW"
                risk_level = max_severity or "MED"

        confidence = self._confidence(
            baseline_ctx,
            rule_traces,
            artifacts_present=artifacts_present,
            calculated=calculated,
        )

        trace = self._json_safe(
            {
                "policy": self._policy_meta(),
                "inputs": { 
                    "group_id": group_id,
                    "anchor_type": anchor_type,
                    "anchor_id": str(anchor_id) if anchor_id else None,
                    "po_item": self._safe_po(po_line),
                    "artifacts_present": sorted(list(artifacts_present)),
                },
                "selection": {
                "selected_technique": (sel or {}).get("selected_technique"),
                "baseline": baseline_ctx.get("baseline"),
                "baseline_source": baseline_ctx.get("baseline_source"),
                "baseline_layer": baseline_ctx.get("baseline_layer"),
                "baseline_source_tag": baseline_ctx.get("baseline_source_tag"),
                "baselines": baseline_ctx.get("baselines") or {},
                "readiness_flags": readiness,
                "selection_refs": self._refs_from_selection(sel),
            },
                "calculations": {"values": calculated, "trace": calc_trace, "error": calc_error},
                "explainability": self._build_explainability_pack(po_line=po_line, sel=sel, calculated=calculated, rule_ctx=rule_ctx),
                "rules": rule_traces,
            }
        )

        self.result_repo.upsert_result(
            run_id=run_id,
            group_id=group_id,
            decision_status=decision_status,
            risk_level=risk_level,
            confidence=confidence,
            reason_codes=reason_codes,
            fail_actions=self._dedup_actions(fail_actions),
            trace=trace,
            evidence_refs=self._json_safe(self._refs_from_selection(sel)),
            created_by=created_by,
        )

        self._audit_emit(
            case_id=case_id,
            event_type="GROUP_DECISION_FINALIZED",
            actor=created_by,
            run_id=run_id,
            payload={
                "run_id": run_id,
                "group_id": group_id,
                "decision": decision_status,
                "risk_level": risk_level,
                "confidence": confidence,
                "reason_codes": reason_codes,
            },
        )

        return {"group_id": group_id, "decision": decision_status, "risk_level": risk_level, "confidence": confidence}

    # =====================================================
    # Rule Evaluation (Policy YAML)
    # =====================================================
    def _eval_rule(
        self,
        *,
        rule: Dict[str, Any],
        po_line: Dict[str, Any],
        baseline_ctx: Dict[str, Any],
        artifacts_present: set[str],
        readiness: Dict[str, Any],
        calculated: Dict[str, Any],
        rule_ctx: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not self._preconditions_ok(rule.get("preconditions") or {}, baseline_ctx, artifacts_present, readiness):
            return None

        logic = rule.get("logic") or {}
        logic_type = str(logic.get("type") or "").strip()

        if logic_type == "compare":
            field = str(logic.get("field") or "")
            op = str(logic.get("operator") or "").strip()
            expected_raw = logic.get("value")

            actual = calculated.get(field)
            expected = self._resolve_expected(expected_raw, rule_ctx)

            if actual is None:
                return self._trace(
                    rule,
                    "FAIL",
                    {"note": f"missing calculated field: {field}"},
                    self._normalize_fail_actions(rule.get("fail_actions") or [{"type": "REVIEW"}]),
                    {"missing_field": field},
                )

            is_fail = self._compare(actual, op, expected)
            calculation = {"field": field, "actual": actual, "operator": op, "expected": expected}

            return self._trace(
                rule,
                "FAIL" if is_fail else "PASS",
                calculation,
                self._normalize_fail_actions(rule.get("fail_actions") or []) if is_fail else [],
                {},
            )

        if logic_type == "compare_all_true":
            fields = logic.get("fields") or []
            missing: List[str] = []
            failed: List[str] = []

            for f in fields:
                k = str(f)
                v = calculated.get(k)
                if v is None:
                    missing.append(k)
                elif v is not True:
                    failed.append(k)

            if missing:
                return self._trace(
                    rule,
                    "FAIL",
                    {"missing_fields": missing},
                    [{"type": "REVIEW"}],
                    {"reason": "missing_calculated_fields", "mismatches": missing},
                )

            ok = len(failed) == 0
            return self._trace(
                rule,
                "PASS" if ok else "FAIL",
                {"failed_fields": failed},
                [] if ok else self._normalize_fail_actions(rule.get("fail_actions") or []),
                {"mismatches": failed},
            )

        if logic_type == "compare_any_true":
            fields = logic.get("fields") or []
            missing: List[str] = []
            trues: List[str] = []

            for f in fields:
                k = str(f)
                v = calculated.get(k)
                if v is None:
                    missing.append(k)
                elif v is True:
                    trues.append(k)

            if missing:
                return self._trace(
                    rule,
                    "FAIL",
                    {"missing_fields": missing},
                    [{"type": "REVIEW"}],
                    {"reason": "missing_calculated_fields", "mismatches": missing},
                )

            ok = len(trues) > 0
            return self._trace(
                rule,
                "PASS" if ok else "FAIL",
                {"true_fields": trues},
                [] if ok else self._normalize_fail_actions(rule.get("fail_actions") or []),
                {"mismatches": [] if ok else fields},
            )

        if logic_type == "document_presence":
            required_docs = [str(x).upper() for x in (logic.get("required_docs") or [])]
            has_any_doc = ("DOCUMENT" in artifacts_present)

            missing = []
            if not has_any_doc:
                missing = required_docs[:]

            ok = (len(missing) == 0)
            calculation = {"required_docs": required_docs, "has_any_document": has_any_doc}
            extra = {"missing_docs": missing}
            return self._trace(
                rule,
                "PASS" if ok else "FAIL",
                calculation,
                [] if ok else self._normalize_fail_actions(rule.get("fail_actions") or []),
                extra,
            )

        if logic_type in ("three_way_match", "two_way_match", "duplicate_pattern"):
            calculation = {"note": "MVP placeholder – insufficient artifacts/data"}
            return self._trace(
                rule,
                "FAIL",
                calculation,
                self._normalize_fail_actions(rule.get("fail_actions") or []),
                {"reason": "placeholder"},
            )

        return self._trace(
            rule,
            "FAIL",
            {"note": f"unknown logic_type={logic_type}"},
            [{"type": "REVIEW"}],
            {"reason": "unknown_logic"},
        )

    # =====================================================
    # Consume C3.5 Selection
    # =====================================================
    def _index_selection_by_group(self, selection: Dict[str, Any], case_id: str, domain_code: str) -> Dict[str, Dict[str, Any]]:
        if not selection:
            raise ValueError("C4 requires C3.5 selection payload (selection is missing)")

        if str(selection.get("case_id")) != str(case_id):
            inner = selection.get("selection")
            if inner and str(inner.get("case_id")) == str(case_id):
                selection = inner
            else:
                raise ValueError("selection.case_id mismatch")

        if str(selection.get("domain")) != str(domain_code):
            raise ValueError(f"selection.domain mismatch expected={domain_code} got={selection.get('domain')}")

        groups = selection.get("groups") or []
        idx: Dict[str, Dict[str, Any]] = {}
        for g in groups:
            gid = str(g.get("group_id"))
            if gid:
                idx[gid] = g
        return idx

    def _baseline_from_selection(self, sel: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not sel:
            return {
                "baseline_available": False,
                "baseline": None,
                "baseline_source": None,
                "baseline_layer": None,
                "baseline_source_tag": None,
                "selected_technique": None,
                "baselines": {},
            }

        baseline = sel.get("baseline")
        baseline_source = sel.get("baseline_source")

        out = {
            "baseline_available": bool(baseline and baseline.get("value") is not None),
            "baseline": (
                {"value": baseline.get("value"), "currency": baseline.get("currency")}
                if baseline and baseline.get("value") is not None
                else None
            ),
            "baseline_source": baseline_source,
            "baseline_layer": sel.get("baseline_layer"),
            "baseline_source_tag": sel.get("baseline_source_tag"),
            "selected_technique": sel.get("selected_technique"),
            "baselines": sel.get("baselines") or {},
        }
        return out
    
    def _refs_from_selection(self, sel: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        out = {"fact_ids": [], "evidence_ids": []}
        if not sel:
            return out

        trace = sel.get("selection_trace") or []
        for step in trace:
            if step.get("passed") is True:
                refs = (step.get("references") or {})
                out["fact_ids"] = refs.get("fact_ids") or []
                out["evidence_ids"] = refs.get("evidence_ids") or []
                return out
        return out

    def _selection_summary(self, selection: Dict[str, Any]) -> Dict[str, Any]:
        inner = selection.get("selection") if selection.get("selection") else selection
        groups = inner.get("groups") or []
        return {
            "case_id": inner.get("case_id"),
            "domain": inner.get("domain"),
            "group_count": len(groups),
            "technique_counts": self._count([g.get("selected_technique") for g in groups]),
        }

    # =====================================================
    # Preconditions / Aggregation
    # =====================================================
   
    def _preconditions_ok(
        self,
        pre: Dict[str, Any],
        baseline_ctx: Dict[str, Any],
        artifacts_present: set[str],
        readiness: Dict[str, Any],
    ) -> bool:
        # ---------------------------------
        # baseline_available exact match
        # ---------------------------------
        if "baseline_available" in pre:
            expected_available = bool(pre.get("baseline_available"))
            actual_available = bool(
                baseline_ctx.get("baseline_available")
                and readiness.get("baseline_available", True)
            )
            if actual_available != expected_available:
                return False

        # ---------------------------------
        # legacy baseline_source exact match
        # ---------------------------------
        if "baseline_source" in pre:
            expected = str(pre.get("baseline_source") or "")
            actual = str((baseline_ctx.get("baseline_source") or {}).get("fact_type") or "")
            if expected and expected != actual:
                return False

        # ---------------------------------
        # NEW: baseline_layer_in
        # ---------------------------------
        if "baseline_layer_in" in pre:
            allowed = {str(x).strip() for x in (pre.get("baseline_layer_in") or []) if str(x).strip()}
            actual_layer = str(baseline_ctx.get("baseline_layer") or "").strip()
            if not actual_layer or actual_layer not in allowed:
                return False

        # ---------------------------------
        # NEW: baseline_source_tag_in
        # ---------------------------------
        if "baseline_source_tag_in" in pre:
            allowed = {str(x).strip() for x in (pre.get("baseline_source_tag_in") or []) if str(x).strip()}
            actual_tag = str(baseline_ctx.get("baseline_source_tag") or "").strip()
            if not actual_tag or actual_tag not in allowed:
                return False

        # ---------------------------------
        # artifacts_present subset
        # ---------------------------------
        if "artifacts_present" in pre:
            needed = {str(x).upper() for x in (pre.get("artifacts_present") or [])}
            if not needed.issubset(artifacts_present):
                return False

        # ---------------------------------
        # artifact_missing
        # ---------------------------------
        if "artifact_missing" in pre:
            missing = str(pre.get("artifact_missing") or "").upper()
            if missing and (missing in artifacts_present):
                return False

        return True
   
    def _aggregate_case(self, group_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        worst = None
        any_review = False
        any_reject = False

        for g in group_results or []:
            worst = self._max_severity(worst, g.get("risk_level"))
            if g.get("decision") == "REVIEW":
                any_review = True
            if g.get("decision") == "REJECT":
                any_reject = True

        decision = "PASS"
        if any_reject:
            decision = "REJECT"
        elif any_review:
            decision = "REVIEW"

        risk_level = worst or "LOW"

        return {
            "decision": decision,
            "risk_level": risk_level,
            "confidence": self._avg([g.get("confidence") for g in (group_results or [])]),
            "summary": {
                "groups": len(group_results or []),
                "review_count": sum(1 for g in (group_results or []) if g.get("decision") == "REVIEW"),
                "reject_count": sum(1 for g in (group_results or []) if g.get("decision") == "REJECT"),
            },
        }

    def _normalize_run_decision(self, internal_decision: str) -> str:
        x = (internal_decision or "").upper().strip()
        if x == "PASS":
            return "APPROVE"
        if x == "REVIEW":
            return "REVIEW"
        if x == "REJECT":
            return "REJECT"
        return "REVIEW"

    def _confidence(
        self,
        baseline_ctx: Dict[str, Any],
        rule_traces: List[Dict[str, Any]],
        artifacts_present: Optional[set] = None,
        calculated: Optional[Dict[str, Any]] = None,
    ) -> float:
        artifacts_present = artifacts_present or set()
        calculated = calculated or {}

        # ---- Evidence Score ----
        if baseline_ctx.get("baseline_available"):
            evidence_score = 1.0
        else:
            # finance_ap case
            required = {"PO", "GR", "INVOICE"}
            present = {a.upper() for a in artifacts_present}
            evidence_score = len(required & present) / len(required)

        # ---- Calculation Score ----
        total_calcs = len(calculated.keys()) or 1
        valid_calcs = sum(1 for v in calculated.values() if v is not None)
        calculation_score = valid_calcs / total_calcs

        # ---- Rule Clarity Score ----
        fails = [r for r in rule_traces if r.get("result") == "FAIL"]
        if not fails:
            rule_score = 1.0
        else:
            clear = sum(
                1 for r in fails
                if r.get("calculation") and r["calculation"].get("actual") is not None
            )
            rule_score = clear / len(fails)

        # ---- Artifact Score ----
        if not artifacts_present:
            artifact_score = 0.5
        else:
            artifact_score = min(1.0, len(artifacts_present) / 3)

        confidence = (
            evidence_score * 0.4 +
            calculation_score * 0.2 +
            rule_score * 0.2 +
            artifact_score * 0.2
        )

        return round(confidence, 2)

    # =====================================================
    # Artifacts
    # =====================================================
    def _detect_artifacts_present(self, case_id: str) -> set[str]:
        present = {"PO"}
        if not self.doc_link_repo:
            return present

        links = self.doc_link_repo.list_by_case(case_id) or []
        for l in links:
            if str(l.get("link_status") or "").upper() == "CONFIRMED":
                present.add("DOCUMENT")
                break
        return present

    # =====================================================
    # Calculation requirements (ENTERPRISE v1, policy-first)
    # =====================================================
    def _get_required_calcs(self, domain_code: str) -> List[Dict[str, Any]]:
        """
        CRITICAL FIX:
        - Always derive calcs from Enterprise policy v1 shape:
            policy.domains[domain].calculations (dict of calc objects)
        - Do NOT rely on legacy required_calculations(), which can return [] and break procurement variance_pct.
        """
        dom = (self.policy.get("domains") or {}).get(domain_code) or {}
        calcs = dom.get("calculations") or {}
        if isinstance(calcs, dict):
            return [c for c in calcs.values() if isinstance(c, dict)]
        return []

    # =====================================================
    # Trace helpers
    # ===========================================
    # =====================================================
    # Explainability helpers (Enterprise)
    # =====================================================
    def _format_explanation_obj(self, explanation: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(explanation, dict):
            return {}
        out: Dict[str, Any] = {}
        for k, v in explanation.items():
            if isinstance(v, str):
                out[k] = self._format_template(v, ctx)
            else:
                out[k] = v
        return out
    
    def _select_explanation_by_result(
        self,
        explanation: Dict[str, Any],
        result: str,
        ctx: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(explanation, dict):
            return {}

        result_norm = str(result or "").upper().strip()
        is_fail = result_norm == "FAIL"

        exec_key = "exec_fail" if is_fail else "exec_pass"
        audit_key = "audit_fail" if is_fail else "audit_pass"

        selected = {
            "exec": explanation.get(exec_key) or explanation.get("exec") or "",
            "audit": explanation.get(audit_key) or explanation.get("audit") or "",
        }

        return self._format_explanation_obj(selected, ctx)

    def _format_template(self, template: str, ctx: Dict[str, Any]) -> str:
        if not template or "{" not in template:
            return template or ""
        # Replace {path.like.this} tokens using ctx resolver.
        def repl(m):
            token = (m.group(1) or "").strip()
            if not token:
                return m.group(0)
            val = self._resolve_token(ctx, token)
            if val is None:
                return m.group(0)
            return str(val)
        return re.sub(r"\{([^{}]+)\}", repl, template)

    def _resolve_token(self, ctx: Dict[str, Any], token: str) -> Any:
        # Support both dot paths (ap.sku) and flat calculated fields (variance_pct)
        if "." in token:
            return self._resolve_path(ctx, "$" + token)
        # Flat field: try calculated first, then ctx root
        if isinstance(ctx, dict):
            calc = ctx.get("calculated") if isinstance(ctx.get("calculated"), dict) else {}
            if isinstance(calc, dict) and token in calc:
                return calc.get(token)
            if token in ctx:
                return ctx.get(token)
        return None

    def _trace(
        self,
        rule: Dict[str, Any],
        result: str,
        calculation: Dict[str, Any],
        fail_actions: List[Dict[str, Any]],
        extra: Optional[Dict[str, Any]] = None,
        fmt_ctx: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        resolved_ctx = fmt_ctx or getattr(self, "_active_fmt_ctx", {}) or {}
        selected_explanation = self._select_explanation_by_result(
            rule.get("explanation") or {},
            result,
            resolved_ctx,
        )

        out = {
            "rule_id": rule.get("rule_id"),
            "domain": rule.get("domain"),
            "group": rule.get("group"),
            "severity": rule.get("severity"),
            "result": result,
            "calculation": calculation,
            "fail_actions": fail_actions,
            "exec_message": selected_explanation.get("exec"),
            "audit_message": selected_explanation.get("audit"),
            "explanation": selected_explanation,
        }
        if extra:
            out["extra"] = extra
        return out
    
    def _build_explainability_pack(
        self,
        *,
        po_line: Optional[Dict[str, Any]],
        sel: Optional[Dict[str, Any]],
        calculated: Dict[str, Any],
        rule_ctx: Dict[str, Any],
    ) -> Dict[str, Any]:
        ap = (sel or {}).get("ap_context") or {}
        sku = ap.get("sku") or (po_line or {}).get("sku")
        qty_po = ap.get("qty_po")
        qty_gr = ap.get("qty_gr")
        qty_inv = ap.get("qty_inv")
        over_gr = ap.get("over_gr_qty")
        over_inv = ap.get("over_inv_qty")

        po_price = ap.get("po_unit_price")
        inv_price = ap.get("inv_unit_price")

        def dec(x):
            try:
                return Decimal(str(x))
            except Exception:
                return None

        po_p = dec(po_price)
        inv_p = dec(inv_price)
        price_diff_abs = None
        price_diff_pct = None
        if po_p is not None and inv_p is not None:
            price_diff_abs = inv_p - po_p
            if po_p != 0:
                price_diff_pct = (price_diff_abs / po_p) * Decimal("100")

        pack = {
            "sku": sku,
            "item": {
                "item_id": (po_line or {}).get("item_id"),
                "item_name": (po_line or {}).get("item_name") or (po_line or {}).get("name"),
                "description": (po_line or {}).get("description"),
                "uom": (po_line or {}).get("uom"),
            },
            "qty": {
                "po": qty_po,
                "gr": qty_gr,
                "inv": qty_inv,
                "over_gr_qty": over_gr,
                "over_inv_qty": over_inv,
            },
            "price": {
                "po_unit_price": po_price,
                "inv_unit_price": inv_price,
                "diff_abs": str(price_diff_abs) if price_diff_abs is not None else None,
                "diff_pct": str(price_diff_pct) if price_diff_pct is not None else None,
                "tolerance_abs": self._resolve_path(rule_ctx, "$meta.defaults.tolerances.price_abs"),
            },
            "flags": {
                "dup_invoice": ap.get("dup_flag"),
                "inv_without_gr": ap.get("inv_without_gr_flag"),
            },
            "calculated": calculated,
        }

        return self._json_safe(pack)


    # =====================================================
    # Policy shape helpers
    # =====================================================
    def _iter_rules(self, domain_code: str) -> List[Dict[str, Any]]:
        domains = self.policy.get("domains") or {}
        d = domains.get(domain_code)
        if isinstance(d, dict) and isinstance(d.get("rules"), list):
            out = []
            for r in d.get("rules") or []:
                if isinstance(r, dict):
                    rr = dict(r)
                    rr.setdefault("domain", domain_code)
                    out.append(rr)
            return out

        out = []
        for r in self.policy.get("rules") or []:
            if not isinstance(r, dict):
                continue
            if (r.get("domain") or "").strip() != domain_code:
                continue
            out.append(r)
        return out

    def _policy_meta(self) -> Dict[str, str]:
        meta = self.policy.get("meta") or {}
        return {
            "policy_id": str(meta.get("policy_id") or ""),
            "policy_version": str(meta.get("version") or ""),
        }

    # =====================================================
    # Expected resolver for rules
    # =====================================================
    def _resolve_expected(self, expected_raw: Any, ctx: Dict[str, Any]) -> Any:
        if isinstance(expected_raw, str) and expected_raw.strip().startswith("$"):
            return self._resolve_path(ctx, expected_raw.strip())
        return expected_raw

    def _resolve_path(self, ctx: Dict[str, Any], path: str) -> Any:
        p = path.strip()
        if not p.startswith("$"):
            return None
        keys = [k for k in p[1:].split(".") if k]
        cur: Any = ctx
        for k in keys:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return None
        return cur

    # =====================================================
    # Generic helpers
    # =====================================================
    def _compute_input_hash(self, case_id: str, policy_id: str, policy_version: str, selection: Dict[str, Any]) -> str:
        payload = {
            "case_id": case_id,
            "policy_id": policy_id,
            "policy_version": policy_version,
            "selection_summary": self._selection_summary(selection),
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _safe_po(self, po_line: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "item_id": po_line.get("item_id"),
            "sku": po_line.get("sku"),
            "item_name": po_line.get("item_name") or po_line.get("name"),
            "quantity": po_line.get("quantity"),
            "unit_price": po_line.get("unit_price"),
            "currency": po_line.get("currency"),
            "total_price": po_line.get("total_price"),
            "uom": po_line.get("uom"),
            "source_line_ref": po_line.get("source_line_ref"),
            "created_at": po_line.get("created_at"),
        }

    def _dec(self, v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _avg(self, xs: List[Any]) -> float:
        vals = [float(x) for x in xs if x is not None]
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def _max_severity(self, a: Optional[str], b: Optional[str]) -> Optional[str]:
        if not b:
            return a
        if not a:
            return b
        aa = str(a).upper()
        bb = str(b).upper()
        return aa if SEVERITY_ORDER.get(aa, 0) >= SEVERITY_ORDER.get(bb, 0) else bb

    @staticmethod
    def _normalize_risk_level(risk_level: str) -> str:
        m = (risk_level or "").upper().strip()
        mapping = {
            "LOW": "LOW",
            "L": "LOW",
            "MED": "MEDIUM",
            "MID": "MEDIUM",
            "M": "MEDIUM",
            "MEDIUM": "MEDIUM",
            "HIGH": "HIGH",
            "H": "HIGH",
            "CRITICAL": "CRITICAL",
            "C": "CRITICAL",
        }
        return mapping.get(m, m)

    def _normalize_fail_actions(self, fail_actions: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for a in fail_actions or []:
            if isinstance(a, str):
                out.append({"type": a})
            elif isinstance(a, dict):
                if "type" in a:
                    out.append({"type": a.get("type"), **{k: v for k, v in a.items() if k != "type"}})
                else:
                    for k, v in a.items():
                        out.append({"type": k, "value": v})
            else:
                out.append({"type": "unknown_action", "raw": str(a)})
        return out

    def _dedup_actions(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for a in actions or []:
            safe = self._json_safe(a)
            s = json.dumps(safe, sort_keys=True, ensure_ascii=False, default=str)
            if s in seen:
                continue
            seen.add(s)
            out.append(safe)
        return out

    def _count(self, xs: List[Any]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for x in xs or []:
            k = str(x) if x is not None else "null"
            out[k] = out.get(k, 0) + 1
        return out

    def _load_policy(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    # =====================================================
    # Compare helpers
    # =====================================================
    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        op = operator.strip()

        if op in (">", ">=", "<", "<="):
            a = self._dec(actual)
            e = self._dec(expected)
            if a is None or e is None:
                return True
            if op == ">":
                return a > e
            if op == ">=":
                return a >= e
            if op == "<":
                return a < e
            if op == "<=":
                return a <= e

        if op in ("==", "!="):
            if op == "==":
                return actual != expected
            return actual == expected

        return True

    def _normalize_money(self, v: Any, fallback_currency: Optional[str] = None) -> Dict[str, Any]:
        if isinstance(v, dict) and "value" in v:
            return {"value": v.get("value"), "currency": v.get("currency") or fallback_currency}
        if v is None:
            return {"value": None, "currency": fallback_currency}
        return {"value": v, "currency": fallback_currency}

    def _json_safe(self, v: Any) -> Any:
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, Decimal):
            return str(v)
        if UUID is not None and isinstance(v, UUID):
            return str(v)
        if isinstance(v, set):
            return [self._json_safe(x) for x in sorted(list(v), key=lambda z: str(z))]
        if isinstance(v, tuple):
            return [self._json_safe(x) for x in v]
        if isinstance(v, dict):
            return {str(k): self._json_safe(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self._json_safe(x) for x in v]
        return v

    # =====================================================
    # Audit helper
    # =====================================================
    def _audit_emit(self, *, case_id: str, event_type: str, actor: str, run_id: str, payload: Dict[str, Any]) -> None:
        if not self.audit_repo:
            return
        try:
            self.audit_repo.emit(
                case_id=case_id,
                event_type=event_type,
                actor=actor,
                run_id=run_id,
                payload=self._json_safe(payload),
            )
        except Exception:
            return
