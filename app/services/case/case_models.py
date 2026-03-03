from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


# ============================================================
# Core Entity
# ============================================================

class EntityInfo(BaseModel):
    entity_id: str
    entity_type: str
    entity_code: Optional[str] = None
    entity_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================
# Line Item (Immutable Snapshot)
# ============================================================

class CaseLineItem(BaseModel):
    item_id: Optional[str] = None
    source_line_ref: Optional[str] = None

    sku: Optional[str] = None
    item_name: Optional[str] = None
    description: Optional[str] = None

    quantity: Optional[float] = None
    uom: Optional[str] = None

    unit_price: Optional[float] = None
    currency: Optional[str] = None
    total_price: Optional[float] = None


# ============================================================
# Artifact Summary
# ============================================================

class ArtifactSummary(BaseModel):
    total_count: int = 0
    by_type: Dict[str, int] = Field(default_factory=dict)


# ============================================================
# Decision Track (Per Domain)
# ============================================================

class DomainDecisionTrack(BaseModel):
    domain: str
    latest_decision: Optional[str] = None
    risk_level: Optional[str] = None
    confidence: Optional[float] = None
    last_run_id: Optional[str] = None


# ============================================================
# Audit Summary (Lightweight)
# ============================================================

class CaseAuditSummary(BaseModel):
    has_transaction: bool = False
    status: Optional[str] = None


# ============================================================
# Case Master (Frontend-ready)
# ============================================================

class CaseMaster(BaseModel):
    case_id: str

    reference_type: Optional[str] = None
    reference_id: Optional[str] = None

    domain: Optional[str] = None
    status: Optional[str] = None

    transaction_id: Optional[str] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    entity: Optional[EntityInfo] = None


# ============================================================
# ENTERPRISE AGGREGATE RESPONSE (Frontend-ready)
# ============================================================

class CaseAggregateResponse(BaseModel):
    case: CaseMaster
    artifacts: ArtifactSummary
    decision_tracks: List[DomainDecisionTrack] = Field(default_factory=list)
    audit: CaseAuditSummary
    line_items: List[CaseLineItem] = Field(default_factory=list)


# ============================================================
# Existing Request/Response (Keep for compatibility)
# ============================================================

class POLineItemInput(BaseModel):
    source_line_ref: Optional[str] = None
    sku: Optional[str] = None
    item_name: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[float] = None
    uom: Optional[str] = None
    unit_price: Optional[float] = None
    currency: Optional[str] = None


class CreateCaseFromPORequest(BaseModel):
    reference_type: str
    reference_id: str

    entity_id: str
    entity_type: str
    domain: str

    currency: Optional[str] = "THB"
    amount_total: Optional[float] = None

    line_items: List[POLineItemInput]


class CaseResponse(BaseModel):
    case_id: str
    reference_type: str
    reference_id: str
    status: str

class CaseListItem(BaseModel):
    """
    1 row จาก vw_case_list
    """
    model_config = ConfigDict(extra="allow")

    case_id: str

    entity_id: Optional[str] = None
    entity_type: Optional[str] = None
    domain: Optional[str] = None

    reference_type: Optional[str] = None
    reference_id: Optional[str] = None

    contract_id: Optional[str] = None
    amount_total: Optional[float] = None
    currency: Optional[str] = None

    status: Optional[str] = None
    decision: Optional[str] = None
    risk_level: Optional[str] = None
    confidence_score: Optional[float] = None

    case_detail: Optional[Dict[str, Any]] = None

    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CaseListResponse(BaseModel):
    items: List[CaseListItem]
    page: int
    limit: int
    total: Optional[int] = None
