# app/models/case_aggregate.py

from pydantic import BaseModel
from app.models.case_detail import CaseDetail
from app.models.view_contract import DecisionRunViewContext


class CaseAggregateView(BaseModel):
    case: CaseDetail
    decision: DecisionRunViewContext