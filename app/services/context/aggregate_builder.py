from __future__ import annotations

from typing import Any, Dict
from pydantic import BaseModel

from app.services.context.models import ContextDecisionView


class CaseDetail(BaseModel):
    """
    Minimal wrapper to avoid guessing your case schema.
    Replace later with your real CaseDetail model without touching DecisionView.
    """
    case_id: str
    data: Dict[str, Any] = {}


class CaseAggregateView(BaseModel):
    case: CaseDetail
    decision: ContextDecisionView


async def build_case_aggregate_view(*, case_detail: Dict[str, Any], decision_view: ContextDecisionView) -> CaseAggregateView:
    cid = str(case_detail.get("case_id") or decision_view.case_id)
    return CaseAggregateView(
        case=CaseDetail(case_id=cid, data=dict(case_detail)),
        decision=decision_view,
    )