from __future__ import annotations

from typing import Dict

from app.services.adapters.unknown import UnknownDomainAdapter
from app.services.context.models import ContextDecisionItemView


class ProcurementAdapter(UnknownDomainAdapter):
    domain = "procurement"

    def to_item_view(self, raw: Dict) -> ContextDecisionItemView:
        item = super().to_item_view(raw)
        # procurement default price context
        if not item.price.context or item.price.context == "UNKNOWN":
            item.price.context = "BASELINE"
        # ensure baseline flag consistency if baseline_unit present
        if item.price.baseline_unit is not None and not item.price.has_baseline:
            item.price.has_baseline = True
        return item