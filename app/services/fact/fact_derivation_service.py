from __future__ import annotations

from statistics import median
from typing import Any, Dict, List, Optional

from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.case_fact_repo import CaseFactRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository


class FactDerivationService:
    """
    C3.5 — Fact Derivation (FINAL / LOCKED)

    RULES:
    - Fact OWNED by group_id
    - Contract facts derive from evidences attached to group
    - Historical facts derive directly from dcc_transaction_line_items
      (not from dcc_case_evidences, because document_id is required there)

    ENTERPRISE CONSTRAINT:
    - Repositories MUST be constructed with sb (single lifecycle)
    - No Repo() without sb

    PATCHES:
    - Fix MEDIAN fact_type to MEDIAN_12M_PRICE (align with policy)
    - Use deterministic ordering for LAST_OBSERVED_PRICE
    - Pull historical PO prices from dcc_transaction_line_items directly
    """

    def __init__(self, *, sb):
        self.sb = sb
        self.group_repo = CaseEvidenceGroupRepository(sb)
        self.evidence_repo = CaseEvidenceRepository(sb)
        self.fact_repo = CaseFactRepository(sb)
        self.line_repo = CaseLineItemRepository(sb)

    # =====================================================
    # Public API
    # =====================================================
    def derive(self, case_id: str, actor_id: str = "SYSTEM") -> Dict[str, Any]:
        groups = self.group_repo.list_by_case(case_id) or []
        if not groups:
            return {
                "case_id": case_id,
                "status": "no_groups",
                "facts_created": 0,
            }

        case_ctx = self._load_case_context(case_id)
        line_by_item_id = self._index_case_lines(case_id)

        facts_created = 0

        for group in groups:
            group_id = group["group_id"]
            fact_key = group.get("group_key") or f"GROUP:{group_id}"

            anchor_id = group.get("anchor_id")
            po_line = line_by_item_id.get(str(anchor_id)) if anchor_id else None

            if not po_line:
                continue

            sku = po_line.get("sku")
            uom = po_line.get("uom")

            # -------------------------------------------------
            # 1) Contract-derived facts from grouped evidence
            # -------------------------------------------------
            contract_result = self._derive_contract_min_price(
                case_id=case_id,
                group_id=group_id,
                fact_key=fact_key,
                actor_id=actor_id,
            )
            facts_created += contract_result["facts_created"]

            # -------------------------------------------------
            # 2) Historical-derived facts from transaction lines
            # -------------------------------------------------
            historical_result = self._derive_historical_price_facts(
                case_id=case_id,
                group_id=group_id,
                fact_key=fact_key,
                sku=sku,
                uom=uom,
                entity_id=case_ctx.get("entity_id"),
                current_transaction_id=case_ctx.get("transaction_id"),
                actor_id=actor_id,
            )
            facts_created += historical_result["facts_created"]

        return {
            "case_id": case_id,
            "status": "facts_derived",
            "facts_created": facts_created,
            "debug_marker": "FD_v2026_03_06_txn_history_enabled",
        }

    # =====================================================
    # Case / line context
    # =====================================================
    def _load_case_context(self, case_id: str) -> Dict[str, Any]:
        rows = (
            self.sb.table("dcc_cases")
            .select("case_id,entity_id,transaction_id")
            .eq("case_id", case_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        return rows[0] if rows else {}

    def _index_case_lines(self, case_id: str) -> Dict[str, Dict[str, Any]]:
        rows = self.line_repo.list_by_case(case_id) or []
        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            item_id = r.get("item_id")
            if item_id:
                out[str(item_id)] = r
        return out

    # =====================================================
    # Contract facts
    # =====================================================
    def _derive_contract_min_price(
        self,
        *,
        case_id: str,
        group_id: str,
        fact_key: str,
        actor_id: str,
    ) -> Dict[str, Any]:
        evidences = self.evidence_repo.list_by_group_id(group_id) or []

        contract_prices: List[Dict[str, Any]] = []
        evidence_ids: List[str] = []

        for ev in evidences:
            if ev.get("evidence_type") != "PRICE":
                continue
            if str(ev.get("source") or "").upper() != "CONTRACT":
                continue

            payload = ev.get("evidence_payload") or {}
            unit_price = payload.get("unit_price")
            currency = payload.get("currency")

            if unit_price is None:
                continue

            try:
                price_value = float(unit_price)
            except Exception:
                continue

            evidence_id = ev.get("evidence_id")
            if evidence_id:
                evidence_ids.append(evidence_id)

            contract_prices.append({
                "price": price_value,
                "currency": currency,
            })

        if not contract_prices:
            return {"facts_created": 0}

        best = min(contract_prices, key=lambda x: x["price"])
        value = best["price"]
        currency = best["currency"]

        self.fact_repo.upsert_fact({
            "case_id": case_id,
            "group_id": group_id,
            "fact_type": "CONTRACT_MIN_PRICE",
            "fact_key": fact_key,
            "value": value,
            "currency": currency,
            "value_json": {
                "price": value,
                "currency": currency,
                "method": "CONTRACT_MIN",
            },
            "confidence": 0.95,
            "derivation_method": "CONTRACT_MIN",
            "source_evidence_ids": evidence_ids,
            "created_by": actor_id,
        })

        return {"facts_created": 1}

    # =====================================================
    # Historical facts from dcc_transaction_line_items
    # =====================================================
    def _derive_historical_price_facts(
        self,
        *,
        case_id: str,
        group_id: str,
        fact_key: str,
        sku: Optional[str],
        uom: Optional[str],
        entity_id: Optional[str],
        current_transaction_id: Optional[str],
        actor_id: str,
    ) -> Dict[str, Any]:
        if not sku:
            return {"facts_created": 0}

        history_rows = self._list_historical_po_rows(
            sku=sku,
            uom=uom,
            entity_id=entity_id,
            exclude_transaction_id=current_transaction_id,
            limit=12,
        )

        if not history_rows:
            return {"facts_created": 0}

        # newest first (deterministic)
        history_rows = sorted(
            history_rows,
            key=lambda r: str(r.get("created_at") or ""),
            reverse=True,
        )

        prices: List[float] = []
        currencies: List[str] = []
        source_refs: List[str] = []

        for row in history_rows:
            unit_price = row.get("unit_price")
            currency = row.get("currency")

            try:
                price_value = float(unit_price)
            except Exception:
                continue

            prices.append(price_value)
            if currency:
                currencies.append(str(currency))

            source_ref_id = row.get("source_ref_id") or row.get("transaction_id")
            source_line_ref = row.get("source_line_ref")
            if source_ref_id:
                if source_line_ref:
                    source_refs.append(f"{source_ref_id}:{source_line_ref}")
                else:
                    source_refs.append(str(source_ref_id))

        if not prices:
            return {"facts_created": 0}

        facts_created = 0
        currency = currencies[0] if currencies else None

        # ----------------------------
        # FACT 1: MEDIAN_12M_PRICE
        # policy expects this exact fact_type
        # ----------------------------
        if len(prices) >= 3:
            median_value = float(median(prices))

            self.fact_repo.upsert_fact({
                "case_id": case_id,
                "group_id": group_id,
                "fact_type": "MEDIAN_12M_PRICE",
                "fact_key": fact_key,
                "value": median_value,
                "currency": currency,
                "value_json": {
                    "price": median_value,
                    "currency": currency,
                    "method": "MEDIAN_12M",
                    "sample_size": len(prices),
                    "source_refs": source_refs,
                },
                "confidence": 0.70,
                "derivation_method": "MEDIAN_12M",
                "source_evidence_ids": [],
                "created_by": actor_id,
            })
            facts_created += 1

        # ----------------------------
        # FACT 2: LAST_OBSERVED_PRICE
        # newest created_at wins
        # ----------------------------
        latest = history_rows[0]
        last_value = float(latest["unit_price"])
        last_currency = latest.get("currency")
        last_source_ref = latest.get("source_ref_id") or latest.get("transaction_id")

        self.fact_repo.upsert_fact({
            "case_id": case_id,
            "group_id": group_id,
            "fact_type": "LAST_OBSERVED_PRICE",
            "fact_key": fact_key,
            "value": last_value,
            "currency": last_currency,
            "value_json": {
                "price": last_value,
                "currency": last_currency,
                "method": "LAST_OBSERVED",
                "observed_at": latest.get("created_at"),
                "source_ref_id": last_source_ref,
                "source_line_ref": latest.get("source_line_ref"),
            },
            "confidence": 0.40,
            "derivation_method": "LAST_OBSERVED",
            "source_evidence_ids": [],
            "created_by": actor_id,
        })
        facts_created += 1

        return {"facts_created": facts_created}

    def _list_historical_po_rows(
        self,
        *,
        sku: str,
        uom: Optional[str],
        entity_id: Optional[str],
        exclude_transaction_id: Optional[str],
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        """
        Historical PO lookup directly from dcc_transaction_line_items.

        Rules:
        - source_type must be PO
        - same SKU
        - same entity when available
        - prefer same UOM, but fall back to SKU-only if none found
        - exclude current transaction
        """
        rows = self._query_txn_rows(
            sku=sku,
            uom=uom,
            entity_id=entity_id,
            exclude_transaction_id=exclude_transaction_id,
            limit=limit,
        )

        if rows:
            return rows

        # fallback without UOM
        return self._query_txn_rows(
            sku=sku,
            uom=None,
            entity_id=entity_id,
            exclude_transaction_id=exclude_transaction_id,
            limit=limit,
        )

    def _query_txn_rows(
        self,
        *,
        sku: str,
        uom: Optional[str],
        entity_id: Optional[str],
        exclude_transaction_id: Optional[str],
        limit: int,
    ) -> List[Dict[str, Any]]:
        q = (
            self.sb.table("dcc_transaction_line_items")
            .select(
                "txn_item_id,transaction_id,source_type,source_ref_id,source_line_ref,"
                "sku,item_name,description,uom,quantity,unit_price,currency,amount,"
                "source_system,trust_level,document_id,metadata_json,created_by,created_at,entity_id"
            )
            .eq("source_type", "PO")
            .eq("sku", sku)
            .order("created_at", desc=True)
            .limit(limit)
        )

        if entity_id:
            q = q.eq("entity_id", entity_id)
        if uom:
            q = q.eq("uom", uom)

        rows = q.execute().data or []

        if exclude_transaction_id:
            rows = [
                r for r in rows
                if str(r.get("transaction_id") or "") != str(exclude_transaction_id)
            ]

        return rows