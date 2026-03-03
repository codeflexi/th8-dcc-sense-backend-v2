from __future__ import annotations

from typing import Dict

from app.services.adapters.unknown import UnknownDomainAdapter
from app.services.context.models import ContextDecisionItemView


class FinanceAPAdapter(UnknownDomainAdapter):
    domain = "finance_ap"

    def to_item_view(self, raw: Dict) -> ContextDecisionItemView:
        item = super().to_item_view(raw)
        # finance_ap default price context
        if not item.price.context or item.price.context == "UNKNOWN":
            item.price.context = "3WAY_MATCH"
        return item