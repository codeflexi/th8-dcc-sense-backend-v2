# app/context/audit_timeline_builder.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Helpers
# ============================================================

def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_iso_z(ts: Any) -> Optional[str]:
    """
    Normalize timestamps to ISO-8601 Z.
    Accepts:
      - ISO string (with Z or +00:00 or timezone)
      - datetime
      - None
    Returns ISO string with Z, or None.
    """
    if ts is None:
        return None

    if isinstance(ts, datetime):
        dt = ts
    else:
        s = str(ts)
        try:
            # support "2026-02-18 04:30:38.183603+00" style
            s = s.replace(" ", "T")
            # keep Z
            if s.endswith("Z"):
                return s
            # normalize +00:00
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            # last resort: return raw string
            return str(ts)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _upper_or_none(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s.upper() if s else None


def _lower_or_none(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s.lower() if s else None


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


# ============================================================
# Frozen Enums (v1)
# ============================================================

ALLOWED_DOMAINS = {"procurement", "finance_ap", "system", "discovery", "pipeline"}
ALLOWED_SEVERITY = {"INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
ALLOWED_STATUS = {"QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"}
ALLOWED_DECISIONS = {"APPROVE", "REVIEW", "REJECT", "ESCALATE"}
ALLOWED_RISK = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
ALLOWED_RUN_CATEGORY = {"DECISION", "PIPELINE", "DISCOVERY"}


# ============================================================
# Contract Models (Plain dict output)
# ============================================================

@dataclass
class _RunAgg:
    run_id: str
    run_category: str
    domain: str
    policy_id: Optional[str] = None
    policy_version: Optional[str] = None
    technique: Optional[str] = None

    status: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    decision: Optional[str] = None
    risk_level: Optional[str] = None
    confidence: Optional[float] = None

    groups_total: Optional[int] = None
    groups_finalized: Optional[int] = None
    fail_groups: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_category": self.run_category,
            "domain": self.domain,
            "policy": {"policy_id": self.policy_id, "policy_version": self.policy_version},
            "technique": self.technique,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "decision": self.decision,
            "risk_level": self.risk_level,
            "confidence": self.confidence,
            "counts": {
                "groups_total": self.groups_total,
                "groups_finalized": self.groups_finalized,
                "fail_groups": self.fail_groups,
            },
        }


# ============================================================
# AuditTimelineBuilder (Production)
# ============================================================

class AuditTimelineBuilderV1:
    """
    Builds enterprise-grade Timeline Contract v1 (FROZEN SPEC).
    Input: raw_events from dcc_audit_events table (list[dict])
    Output: dict that matches frozen contract schema
    """

    VIEW_VERSION = "v1"

    # -----------------------------
    # Public API
    # -----------------------------
    @staticmethod
    def build(
        *,
        case_id: str,
        raw_events: List[Dict[str, Any]],
        timezone_name: str = "UTC",
    ) -> Dict[str, Any]:
        # 1) Normalize + sort events
        normalized_events = AuditTimelineBuilderV1._normalize_events(raw_events)

        # 2) Assign deterministic sequence
        for idx, e in enumerate(normalized_events, start=1):
            e["sequence"] = idx

        # 3) Build runs aggregation
        runs = AuditTimelineBuilderV1._build_runs(normalized_events)

        # 4) Summary
        summary = AuditTimelineBuilderV1._build_summary(normalized_events, runs)

        return {
            "view_version": AuditTimelineBuilderV1.VIEW_VERSION,
            "case_id": case_id,
            "generated_at": _iso_utc_now(),
            "timezone": timezone_name,
            "summary": summary,
            "runs": runs,
            "events": normalized_events,
        }

    # -----------------------------
    # Normalization
    # -----------------------------
    @staticmethod
    def _normalize_events(raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tmp: List[Tuple[str, str, Dict[str, Any]]] = []

        for e in (raw_events or []):
            payload = e.get("payload") or {}
            event_type = str(e.get("event_type") or "UNKNOWN_EVENT").strip()

            ts = _to_iso_z(e.get("created_at") or payload.get("timestamp") or payload.get("created_at"))
            # stable tie-breakers for same timestamp
            audit_id = str(e.get("audit_id") or e.get("id") or "")
            tie = audit_id or event_type

            meta = e.get("payload") or e.get("meta") or {}
            run_id = e.get("run_id") or meta.get("run_id")
                        
            group_id = payload.get("group_id") or e.get("group_id")

            # domain normalization (NO "unknown")
            domain = AuditTimelineBuilderV1._normalize_domain(e.get("domain"), payload, event_type)

            # category + severity
            category = AuditTimelineBuilderV1._map_category(event_type)
            severity = AuditTimelineBuilderV1._map_severity(event_type, payload)

            # title + message (human-readable)
            title = AuditTimelineBuilderV1._map_title(event_type, payload)
            message = AuditTimelineBuilderV1._map_message(event_type, payload, domain)

            # tags, refs, actor, ui
            tags = AuditTimelineBuilderV1._build_tags(domain, category, severity, event_type, payload)
            refs = AuditTimelineBuilderV1._build_refs(payload)
            actor = AuditTimelineBuilderV1._build_actor(e.get("actor"), payload)
            ui = AuditTimelineBuilderV1._build_ui(event_type, severity, category, domain)

            normalized = {
                "id": audit_id,
                "timestamp": ts,
                "sequence": 0,  # assigned later
                "domain": domain,
                "run_id": str(run_id) if run_id is not None else None,
                "group_id": str(group_id) if group_id is not None else None,
                "type": event_type,
                "category": category,
                "severity": severity,
                "title": title,
                "message": message,
                "tags": tags,
                "actor": actor,
                "refs": refs,
                "ui": ui,
                "payload": payload,
            }

            tmp.append((ts or "", tie, normalized))

        tmp.sort(key=lambda x: (x[0], x[1]))
        return [x[2] for x in tmp]

    @staticmethod
    def _normalize_domain(event_domain: Any, payload: Dict[str, Any], event_type: str) -> str:
        # order:
        # 1) explicit event domain
        # 2) payload.domain
        # 3) infer from event_type
        # 4) default system
        d = _lower_or_none(event_domain) or _lower_or_none(payload.get("domain"))

        if not d:
            d = AuditTimelineBuilderV1._infer_domain_from_event_type(event_type)

        if not d:
            d = "system"

        if d not in ALLOWED_DOMAINS:
            # normalize common variants
            if d in {"procurement_flow", "procure"}:
                d = "procurement"
            elif d in {"finance", "ap", "finance-ap"}:
                d = "finance_ap"
            elif d in {"pipe"}:
                d = "pipeline"
            elif d in {"disc"}:
                d = "discovery"
            else:
                d = "system"

        return d

    @staticmethod
    def _infer_domain_from_event_type(event_type: str) -> Optional[str]:
        et = event_type.upper()
        if et.startswith("PIPELINE_"):
            return "pipeline"
        if et.startswith("DISCOVERY_"):
            return "discovery"
        # decision-run group events without domain -> keep system (payload usually has)
        return None

    # -----------------------------
    # Run aggregation
    # -----------------------------
    @staticmethod
    def _build_runs(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # aggregate by run_id; for events without run_id, ignore
        runs_by_id: Dict[str, _RunAgg] = {}

        for e in events:
            run_id = e.get("run_id")
            if not run_id:
                continue

            event_type = str(e.get("type") or "")
            payload = e.get("payload") or {}
            domain = e.get("domain") or "system"

            run_category = AuditTimelineBuilderV1._derive_run_category(event_type, payload, domain)
            # normalize
            if run_category not in ALLOWED_RUN_CATEGORY:
                run_category = "DECISION" if event_type.upper().startswith("DECISION_RUN_") else "PIPELINE"

            r = runs_by_id.get(run_id)
            if not r:
                r = _RunAgg(
                    run_id=run_id,
                    run_category=run_category,
                    domain=domain,
                )
                runs_by_id[run_id] = r

            # policy/technique
            pol_id = payload.get("policy_id") or payload.get("policy", {}).get("policy_id")
            pol_ver = payload.get("policy_version") or payload.get("policy", {}).get("policy_version")
            if pol_id:
                r.policy_id = str(pol_id)
            if pol_ver:
                r.policy_version = str(pol_ver)
            if payload.get("technique"):
                r.technique = str(payload.get("technique"))

            # timestamps
            ts = e.get("timestamp")
            if AuditTimelineBuilderV1._is_run_started(event_type):
                r.started_at = r.started_at or ts
                r.status = "RUNNING"
            if AuditTimelineBuilderV1._is_run_completed(event_type):
                r.completed_at = ts
                r.status = "SUCCEEDED"
            if AuditTimelineBuilderV1._is_run_failed(event_type):
                r.completed_at = ts
                r.status = "FAILED"

            # decision summary (for decision runs)
            dec = _upper_or_none(payload.get("decision"))
            risk = _upper_or_none(payload.get("risk_level"))
            conf = _safe_float(payload.get("confidence"))

            if dec in ALLOWED_DECISIONS:
                r.decision = dec
            if risk in ALLOWED_RISK:
                r.risk_level = risk
            if conf is not None:
                r.confidence = conf

            # counts
            # prefer payload.summary.groups (or compact)
            if isinstance(payload.get("summary"), dict):
                summ = payload["summary"]
                if r.groups_total is None:
                    r.groups_total = _safe_int(summ.get("groups") or summ.get("groups_total"))
            if event_type.upper() == "GROUP_DECISION_FINALIZED":
                r.groups_finalized = (r.groups_finalized or 0) + 1
                # fail group: if decision in payload indicates not pass
                g_dec = _upper_or_none(payload.get("decision"))
                if g_dec in {"REVIEW", "REJECT", "ESCALATE"}:
                    r.fail_groups = (r.fail_groups or 0) + 1

        # Finalize defaults: no UNKNOWN
        for r in runs_by_id.values():
            if r.status not in ALLOWED_STATUS:
                # derive: if has completed_at -> SUCCEEDED else RUNNING
                if r.completed_at:
                    r.status = "SUCCEEDED"
                elif r.started_at:
                    r.status = "RUNNING"
                else:
                    r.status = "QUEUED"

            # counts default
            if r.groups_finalized is None:
                r.groups_finalized = 0
            if r.fail_groups is None:
                r.fail_groups = 0

        # Sort runs by started_at then completed_at then run_id
        runs_list = list(runs_by_id.values())
        runs_list.sort(
            key=lambda x: (
                x.started_at or "",
                x.completed_at or "",
                x.run_id,
            )
        )
        return [r.to_dict() for r in runs_list]

    @staticmethod
    def _derive_run_category(event_type: str, payload: Dict[str, Any], domain: str) -> str:
        et = event_type.upper()
        if et.startswith("DECISION_RUN_") or et.startswith("GROUP_"):
            return "DECISION"
        if et.startswith("PIPELINE_"):
            return "PIPELINE"
        if et.startswith("DISCOVERY_"):
            return "DISCOVERY"
        # fallback: infer by domain
        if domain == "pipeline":
            return "PIPELINE"
        if domain == "discovery":
            return "DISCOVERY"
        return "DECISION"

    @staticmethod
    def _is_run_started(event_type: str) -> bool:
        et = event_type.upper()
        return et in {"DECISION_RUN_STARTED", "PIPELINE_STARTED", "DISCOVERY_STARTED"}

    @staticmethod
    def _is_run_completed(event_type: str) -> bool:
        et = event_type.upper()
        return et in {"DECISION_RUN_DONE", "PIPELINE_COMPLETED", "DISCOVERY_DONE"}

    @staticmethod
    def _is_run_failed(event_type: str) -> bool:
        return event_type.upper() in {"DECISION_RUN_FAILED", "PIPELINE_FAILED", "DISCOVERY_FAILED"}

    # -----------------------------
    # Summary
    # -----------------------------
    @staticmethod
    def _build_summary(events: List[Dict[str, Any]], runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        # latest decision run = last SUCCEEDED DECISION run
        latest_decision_run = None
        for r in reversed(runs):
            if r.get("run_category") == "DECISION" and r.get("status") == "SUCCEEDED":
                latest_decision_run = r
                break

        return {
            "event_count": len(events),
            "run_count": sum(1 for r in runs if r.get("run_category") == "DECISION"),
            "latest_run_id": (latest_decision_run or {}).get("run_id"),
            "latest_run_decision": (latest_decision_run or {}).get("decision"),
            "latest_run_risk_level": (latest_decision_run or {}).get("risk_level"),
        }

    # -----------------------------
    # Mapping: category / severity / title / message
    # -----------------------------
    @staticmethod
    def _map_category(event_type: str) -> str:
        et = event_type.upper()
        if et.startswith("DECISION_RUN_"):
            return "RUN"
        if et.startswith("GROUP_") or et in {"BASELINE_SELECTED"}:
            return "GROUP"
        if et.startswith("PIPELINE_"):
            return "PIPELINE"
        if et in {"CASE_CREATED_FROM_PO", "PROCUREMENT_TRANSACTION_SEEDED", "INVOICE_RECEIVED", "GRN_RECEIVED"}:
            return "SYSTEM"
        if et.startswith("DISCOVERY_") or "DISCOVERY" in et or "CONTRACT_" in et:
            return "SYSTEM"
        return "SYSTEM"

    @staticmethod
    def _map_severity(event_type: str, payload: Dict[str, Any]) -> str:
        et = event_type.upper()

        if et.endswith("_FAILED") or et == "DECISION_RUN_FAILED":
            return "ERROR"

        # group finalized: escalate by decision/risk
        if et == "GROUP_DECISION_FINALIZED":
            decision = _upper_or_none(payload.get("decision"))
            risk = _upper_or_none(payload.get("risk_level"))
            if decision in {"REJECT"} or risk in {"CRITICAL"}:
                return "CRITICAL"
            if decision in {"REVIEW", "ESCALATE"} or risk in {"HIGH"}:
                return "WARNING"
            return "INFO"

        if et == "DECISION_RUN_DONE":
            decision = _upper_or_none(payload.get("decision"))
            risk = _upper_or_none(payload.get("risk_level"))
            if decision in {"REJECT"} or risk in {"CRITICAL"}:
                return "CRITICAL"
            if decision in {"REVIEW", "ESCALATE"} or risk in {"HIGH", "MEDIUM"}:
                return "WARNING"
            return "SUCCESS"

        if et in {"PIPELINE_COMPLETED", "DISCOVERY_DONE"}:
            return "SUCCESS"

        if et in {"PIPELINE_STARTED", "DISCOVERY_STARTED", "DECISION_RUN_STARTED", "GROUP_EVAL_STARTED"}:
            return "INFO"

        return "INFO"

    @staticmethod
    def _map_title(event_type: str, payload: Dict[str, Any]) -> str:
        et = event_type.upper()

        if et == "DECISION_RUN_STARTED":
            return "Decision run started"
        if et == "DECISION_RUN_DONE":
            decision = payload.get("decision")
            return f"Decision completed: {decision}" if decision else "Decision completed"
        if et == "DECISION_RUN_FAILED":
            return "Decision run failed"

        if et == "PIPELINE_STARTED":
            return "Pipeline started"
        if et == "PIPELINE_COMPLETED":
            return "Pipeline completed"
        if et == "DISCOVERY_STARTED":
            return "Discovery started"
        if et == "DISCOVERY_DONE":
            return "Discovery completed"

        if et == "GROUP_EVAL_STARTED":
            gid = payload.get("group_id")
            return f"Group evaluation started" if not gid else f"Evaluating group {gid}"
        if et == "GROUP_DECISION_FINALIZED":
            decision = payload.get("decision")
            return "Group decision finalized" if not decision else f"Group decision finalized: {decision}"

        if et == "BASELINE_SELECTED":
            return "Baseline selected"

        # fallback
        return event_type

    @staticmethod
    def _map_message(event_type: str, payload: Dict[str, Any], domain: str) -> str:
        et = event_type.upper()

        if et == "DECISION_RUN_STARTED":
            pid = payload.get("policy_id")
            ver = payload.get("policy_version")
            if pid and ver:
                return f"{domain} | policy {pid} {ver}"
            return f"{domain} decision run started"

        if et == "DECISION_RUN_DONE":
            d = payload.get("decision")
            c = payload.get("confidence")
            if d is not None and c is not None:
                return f"Result: {d} | Confidence: {c}"
            return "Decision run completed"

        if et == "GROUP_EVAL_STARTED":
            gid = payload.get("group_id")
            return f"Evaluating group {gid}" if gid else "Evaluating group"

        if et == "GROUP_DECISION_FINALIZED":
            gid = payload.get("group_id")
            d = payload.get("decision")
            reasons = payload.get("reason_codes") or []
            if gid and d:
                if reasons:
                    return f"Decision: {d} | Group: {gid} | Reasons: {', '.join(reasons)}"
                return f"Decision: {d} | Group: {gid}"
            return "Group decision finalized"

        if et == "BASELINE_SELECTED":
            baseline = payload.get("baseline") or {}
            val = baseline.get("value")
            cur = baseline.get("currency")
            tech = payload.get("technique")
            if val is not None and cur:
                if tech:
                    return f"Baseline {val} {cur} selected using {tech}"
                return f"Baseline {val} {cur} selected"
            return "Baseline selected"

        if et == "PIPELINE_STARTED":
            return f"{domain} pipeline started" if domain in {"procurement", "finance_ap"} else "Pipeline started"
        if et == "PIPELINE_COMPLETED":
            return "Pipeline completed"

        # fallback: keep deterministic
        return event_type

    # -----------------------------
    # Tags, refs, actor, ui
    # -----------------------------
    @staticmethod
    def _build_tags(domain: str, category: str, severity: str, event_type: str, payload: Dict[str, Any]) -> List[str]:
        tags = []
        if domain and domain != "system":
            tags.append(domain)
        tags.append(category.lower())
        tags.append(severity.lower())

        # decision tags
        decision = _upper_or_none(payload.get("decision"))
        if decision in {"REVIEW", "REJECT", "ESCALATE"}:
            tags.append(decision.lower())

        # normalize unique
        out = []
        seen = set()
        for t in tags:
            if t and t not in seen:
                out.append(t)
                seen.add(t)
        return out

    @staticmethod
    def _build_refs(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "entity_id": payload.get("entity_id"),
            "po_number": payload.get("po_number"),
            "invoice_number": payload.get("invoice_number"),
            "transaction_id": payload.get("transaction_id"),
        }

    @staticmethod
    def _build_actor(raw_actor: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
        # raw_actor sometimes is string "SYSTEM" in older rows
        if isinstance(raw_actor, dict):
            at = _upper_or_none(raw_actor.get("type")) or "SYSTEM"
            aid = raw_actor.get("id") or raw_actor.get("actor_id") or "SYSTEM"
            dn = raw_actor.get("display_name") or raw_actor.get("name") or str(aid)
            return {"type": at, "id": str(aid), "display_name": str(dn)}

        # fallback from payload or default
        actor = payload.get("actor") or payload.get("created_by") or "SYSTEM"
        return {"type": "SYSTEM", "id": str(actor), "display_name": "System" if str(actor) == "SYSTEM" else str(actor)}

    @staticmethod
    def _build_ui(event_type: str, severity: str, category: str, domain: str) -> Dict[str, Any]:
        et = event_type.upper()

        # icon mapping
        icon = "search"
        if et.endswith("_STARTED") or et in {"PIPELINE_STARTED", "DECISION_RUN_STARTED"}:
            icon = "play"
        elif et.endswith("_DONE") or et.endswith("_COMPLETED"):
            icon = "check"
        elif et.endswith("_FAILED"):
            icon = "x"
        elif et == "GROUP_DECISION_FINALIZED":
            icon = "alert" if severity in {"WARNING", "CRITICAL"} else "check"

        # color mapping (tailwind theme tokens expected by UI)
        color = "slate"
        if severity == "SUCCESS":
            color = "emerald"
        elif severity == "WARNING":
            color = "amber"
        elif severity in {"ERROR", "CRITICAL"}:
            color = "rose"
        elif category == "RUN":
            color = "indigo"
        elif category == "PIPELINE":
            color = "indigo"

        return {"icon": icon, "color": color}