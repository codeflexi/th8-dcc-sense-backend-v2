from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


# =========================================================
# META
# =========================================================

class PolicyMetaDefaults(BaseModel):
    currency: Optional[str] = "THB"
    rounding: Dict[str, Any] = Field(default_factory=dict)
    tolerances: Dict[str, Any] = Field(default_factory=dict)


class PolicyMeta(BaseModel):
    policy_id: str
    version: str
    description: Optional[str] = None
    defaults: Optional[PolicyMetaDefaults] = None
    discovery: Optional[Dict[str, Any]] = None


# =========================================================
# RULE
# =========================================================

class RuleExplanation(BaseModel):
    exec: Optional[str] = None
    audit: Optional[str] = None


class RuleSpec(BaseModel):
    rule_id: str
    group: Optional[str] = None
    severity: Optional[str] = None
    description: Optional[str] = None
    uses: List[str] = Field(default_factory=list)
    logic: Dict[str, Any] = Field(default_factory=dict)
    fail_actions: List[Any] = Field(default_factory=list)
    explanation: Optional[RuleExplanation] = None


# =========================================================
# TECHNIQUE
# =========================================================

class TechniqueSpec(BaseModel):
    id: str
    domain: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    baseline_key: Optional[str] = None
    baseline_layer: Optional[str] = None
    baseline_source_tag: Optional[str] = None
    required_facts: List[str] = Field(default_factory=list)
    gates: Dict[str, Any] = Field(default_factory=dict)
    derive: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        extra = "allow"

# =========================================================
# DOMAIN
# =========================================================

class DomainSpec(BaseModel):
    description: Optional[str] = None
    profile: Dict[str, Any] = Field(default_factory=dict)
    calculations: Dict[str, Any] = Field(default_factory=dict)
    rules: List[RuleSpec] = Field(default_factory=list)
    techniques: Dict[str, TechniqueSpec] = Field(default_factory=dict)


# =========================================================
# ROOT
# =========================================================

class PolicyBundle(BaseModel):
    meta: PolicyMeta
    domains: Dict[str, DomainSpec]
    decision_logic: Dict[str, Any] = Field(default_factory=dict)
