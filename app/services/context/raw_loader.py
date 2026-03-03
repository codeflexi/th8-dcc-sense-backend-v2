from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Request


def _validate_uuid(v: str, name: str):
    try:
        uuid.UUID(str(v))
    except Exception:
        raise HTTPException(status_code=400, detail=f"{name} is not valid uuid: {v}")


async def load_raw_decision_results_by_case(request: Request, case_id: str, *, raw_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Raw loader only. No mapping. No aggregation.
    Assumption (confirmed by you): repo.list_by_case() returns list of item-level dicts
    with keys like group_id/status/item/quantity/price/rules/artifacts, plus run metadata in row(s).
    """
    _validate_uuid(case_id, "case_id")

   
     

    results: List[Dict[str, Any]] = raw_events

    first = results[0] if results else {}
    run_id = first.get("run_id") if first else None

    policy = first.get("policy") or {}
    policy_id = policy.get("policy_id") or first.get("policy_id") or ""
    policy_version = policy.get("policy_version") or first.get("policy_version") or ""

    technique = first.get("technique") or first.get("technique_id") or ""
    created_at = first.get("created_at")
    summary = first.get("summary")  # optional

    return {
        "case_id": case_id,
        "run_id": run_id,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "technique": technique,
        "created_at": created_at,
        "summary": summary,
        "count": len(results),
        "results": results,
    }