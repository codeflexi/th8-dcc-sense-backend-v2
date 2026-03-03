# app/models/case_detail.py

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class EntityRef(BaseModel):
    entity_id: str
    entity_type: str
    entity_name: Optional[str] = None


class UserRef(BaseModel):
    user_id: str
    name: Optional[str] = None
    role: Optional[str] = None


class CaseStatus(str):
    pass  # e.g. OPEN / CLOSED / CANCELLED / ARCHIVED


class SLAInfo(BaseModel):
    sla_hours: Optional[int] = None
    breached: bool = False
    remaining_minutes: Optional[int] = None


class AttachmentSummary(BaseModel):
    total_count: int = 0
    by_type: Dict[str, int] = Field(default_factory=dict)


class DomainTrackStatus(BaseModel):
    domain: str
    latest_decision: Optional[str] = None
    risk_level: Optional[str] = None
    last_run_id: Optional[str] = None


class CaseDetail(BaseModel):
    case_id: str
    case_type: str
    status: str

    entity: EntityRef
    owner: Optional[UserRef] = None
    created_by: Optional[UserRef] = None

    created_at: datetime
    updated_at: Optional[datetime] = None

    sla: Optional[SLAInfo] = None

    attachments: AttachmentSummary = Field(default_factory=AttachmentSummary)

    domain_tracks: List[DomainTrackStatus] = Field(default_factory=list)

    metadata: Dict[str, Any] = Field(default_factory=dict)