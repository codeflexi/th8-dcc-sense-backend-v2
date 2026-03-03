from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


DecisionStatus = Literal["APPROVE", "REVIEW", "ESCALATE", "REJECT"]
RiskLevel = Literal["LOW", "MED", "HIGH", "CRITICAL"]
RuleResult = Literal["PASS", "FAIL", "WARN", "INFO"]
PriceContext = Literal["BASELINE", "3WAY_MATCH", "UNKNOWN"]
DriverType = Literal["RULE", "MODEL", "ANOMALY", "UNKNOWN"]


class PolicyRef(BaseModel):
    policy_id: str = ""
    policy_version: str = ""


class ExposureInfo(BaseModel):
    """
    Option 3C: totals + metrics
    - totals: multi-currency totals for UI/cross-domain summary
    - metrics: domain-specific metrics preserved for audit/debug
    """
    totals: Dict[str, float] = Field(default_factory=dict)   # {"THB": 6980.0}
    metrics: Dict[str, Any] = Field(default_factory=dict)    # {"unit_variance_sum": 6980.0}


class ReasonCodeCount(BaseModel):
    code: str
    count: int


class ContextSummaryView(BaseModel):
    overall_decision: DecisionStatus = "REVIEW"
    risk_level: RiskLevel = "LOW"
    confidence_avg: float = 0.0
    item_count: int = 0
    exposure: ExposureInfo = Field(default_factory=ExposureInfo)
    top_reason_codes: List[ReasonCodeCount] = Field(default_factory=list)


class StatusInfo(BaseModel):
    decision: DecisionStatus = "REVIEW"
    risk_level: RiskLevel = "LOW"
    confidence: float = 0.0


class ItemIdentity(BaseModel):
    # Option 2B: keep item object
    sku: str = ""
    item_name: str = ""
    uom: str = ""


class QuantityFlags(BaseModel):
    gr_exceeds_po: bool = False
    inv_exceeds_gr: bool = False
    inv_without_gr: bool = False


class QuantityInfo(BaseModel):
    ordered: float = 0.0
    received: float = 0.0
    invoiced: float = 0.0
    over_gr_qty: float = 0.0
    over_inv_qty: float = 0.0
    flags: QuantityFlags = Field(default_factory=QuantityFlags)


class PriceInfo(BaseModel):
    context: PriceContext = "UNKNOWN"
    currency: str = "THB"

    po_unit: float = 0.0
    inv_unit: float = 0.0
    grn_unit: float = 0.0

    baseline_unit: Optional[float] = None
    has_baseline: bool = False

    variance_pct: float = 0.0
    variance_abs: float = 0.0

    tolerance_abs: float = 0.0
    within_tolerance: bool = True


class ArtifactFlags(BaseModel):
    po: bool = False
    grn: bool = False
    invoice: bool = False


class FailAction(BaseModel):
    type: str
    meta: Dict[str, Any] = Field(default_factory=dict)


class RuleView(BaseModel):
    rule_id: str
    group: str
    domain: str

    result: RuleResult
    severity: RiskLevel

    exec_message: str = ""
    audit_message: str = ""

    calculation: Dict[str, Any] = Field(default_factory=dict)
    fail_actions: List[FailAction] = Field(default_factory=list)

    # optional audit extensions
    reason_codes: List[str] = Field(default_factory=list)
    extra: Dict[str, Any] = Field(default_factory=dict)


class DriverInfo(BaseModel):
    type: DriverType = "RULE"
    rule_id: Optional[str] = None
    group: Optional[str] = None
    severity: Optional[RiskLevel] = None

    label: str = ""
    exec_message: str = ""
    audit_message: str = ""


class ContextDecisionItemView(BaseModel):
    group_id: str
    domain: str = ""

    status: StatusInfo = Field(default_factory=StatusInfo)
    item: ItemIdentity = Field(default_factory=ItemIdentity)

    quantity: QuantityInfo = Field(default_factory=QuantityInfo)
    price: PriceInfo = Field(default_factory=PriceInfo)

    drivers: List[DriverInfo] = Field(default_factory=list)
    next_action: Optional[DecisionStatus] = None

    rules: List[RuleView] = Field(default_factory=list)
    artifacts: ArtifactFlags = Field(default_factory=ArtifactFlags)

    created_at: Optional[datetime] = None


class ContextDecisionView(BaseModel):
    view_version: str = "v1"

    case_id: str
    run_id: Optional[str] = None

    policy: PolicyRef = Field(default_factory=PolicyRef)
    technique: str = ""
    created_at: Optional[datetime] = None

    summary: ContextSummaryView = Field(default_factory=ContextSummaryView)
    items: List[ContextDecisionItemView] = Field(default_factory=list)


# ----------------------------
# Copilot Lite Models
# ----------------------------

class CopilotSignals(BaseModel):
    blocking_rule_count: int = 0
    failing_item_count: int = 0
    artifact_readiness: ArtifactFlags = Field(default_factory=ArtifactFlags)


class CopilotTopRule(BaseModel):
    rule_id: str
    group: str = ""
    domain: str = ""
    result: RuleResult
    severity: RiskLevel
    calculation: Dict[str, Any] = Field(default_factory=dict)
    fail_actions: List[FailAction] = Field(default_factory=list)


class CopilotKeyNumbers(BaseModel):
    quantity: Dict[str, Any] = Field(default_factory=dict)
    price: Dict[str, Any] = Field(default_factory=dict)


class CopilotLiteItem(BaseModel):
    group_id: str
    domain: str = ""
    item: ItemIdentity = Field(default_factory=ItemIdentity)
    status: StatusInfo = Field(default_factory=StatusInfo)

    key_numbers: CopilotKeyNumbers = Field(default_factory=CopilotKeyNumbers)
    drivers: List[DriverInfo] = Field(default_factory=list)
    top_rules: List[CopilotTopRule] = Field(default_factory=list)

    artifacts: ArtifactFlags = Field(default_factory=ArtifactFlags)


class CopilotContextLite(BaseModel):
    view_version: str = "v1"
    case_id: str
    run_id: Optional[str] = None
    policy: PolicyRef = Field(default_factory=PolicyRef)
    created_at: Optional[datetime] = None

    summary: ContextSummaryView = Field(default_factory=ContextSummaryView)
    signals: CopilotSignals = Field(default_factory=CopilotSignals)
    items: List[CopilotLiteItem] = Field(default_factory=list)