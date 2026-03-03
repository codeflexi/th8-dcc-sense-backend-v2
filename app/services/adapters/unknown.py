from __future__ import annotations

from typing import Any, Dict

from app.services.adapters.base import BaseAdapter
from app.services.context.models import ContextDecisionItemView


class UnknownDomainAdapter(BaseAdapter):
    domain = "unknown"

    def to_item_view(self, raw: Dict[str, Any]) -> ContextDecisionItemView:
        domain = self.detect_domain(raw) or self.domain

        rules = self.norm_rules(raw, domain_fallback=domain)

        item = ContextDecisionItemView(
            group_id=str(raw.get("group_id") or ""),
            domain=domain,
            status=self.norm_status(raw),
            item=self.norm_item_identity(raw),
            quantity=self.norm_quantity(raw),
            price=self.norm_price(raw),
            drivers=[],
            next_action=raw.get("next_action"),
            rules=rules,
            artifacts=self.norm_artifacts(raw),
            created_at=raw.get("created_at"),
        )

        item.drivers = self.norm_drivers(raw, rules)

        if not item.price.context:
            item.price.context = "UNKNOWN"

        return item