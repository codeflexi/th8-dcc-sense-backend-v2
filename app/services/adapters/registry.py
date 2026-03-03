from __future__ import annotations

from typing import Dict, Type

from app.services.adapters.base import BaseAdapter
from app.services.adapters.unknown import UnknownDomainAdapter
from app.services.adapters.procurement import ProcurementAdapter
from app.services.adapters.finance_ap import FinanceAPAdapter


class AdapterRegistry:
    """
    Registry-driven (Option C):
    - view_builder does NOT branch per domain
    - Add new domain: create adapter + register here (explicit, deterministic)
    """

    _registry: Dict[str, Type[BaseAdapter]] = {}
    _default: Type[BaseAdapter] = UnknownDomainAdapter

    @classmethod
    def init_defaults(cls) -> None:
        # idempotent
        if cls._registry:
            return
        cls.register("procurement", ProcurementAdapter)
        cls.register("finance_ap", FinanceAPAdapter)

    @classmethod
    def register(cls, domain: str, adapter_cls: Type[BaseAdapter]) -> None:
        key = str(domain or "").strip()
        if not key:
            raise ValueError("domain must be non-empty")
        cls._registry[key] = adapter_cls

    @classmethod
    def set_default(cls, adapter_cls: Type[BaseAdapter]) -> None:
        cls._default = adapter_cls

    @classmethod
    def get(cls, domain: str) -> BaseAdapter:
        cls.init_defaults()
        key = str(domain or "").strip()
        adapter_cls = cls._registry.get(key, cls._default)
        return adapter_cls()