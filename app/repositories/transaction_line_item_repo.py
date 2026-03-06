from __future__ import annotations
from typing import Any, Dict, List, Optional


class TransactionLineItemRepository:
    TABLE = "dcc_transaction_line_items"

    def __init__(self, sb):
        self.sb = sb

    def insert_many(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        res = self.sb.table(self.TABLE).insert(rows).execute()
        return res.data or []

    def exists_doc_for_entity(
        self,
        *,
        transaction_id: str,
        source_type: str,
        source_ref_id: str,
        entity_id: str,
    ) -> bool:
        res = (
            self.sb.table(self.TABLE)
            .select("txn_item_id")
            .eq("transaction_id", transaction_id)
            .eq("source_type", source_type)
            .eq("source_ref_id", source_ref_id)
            .eq("entity_id", entity_id)
            .limit(1)
            .execute()
        )
        return bool(res.data)

    def sum_qty_by_sku(
        self,
        *,
        transaction_id: str,
        source_type: str,
        sku: str,
    ) -> float:
        res = (
            self.sb.table(self.TABLE)
            .select("quantity")
            .eq("transaction_id", transaction_id)
            .eq("source_type", source_type)
            .eq("sku", sku)
            .execute()
        )
        total = 0.0
        for r in (res.data or []):
            try:
                total += float(r.get("quantity") or 0)
            except Exception:
                continue
        return total

    def list_by_transaction(self, *, transaction_id: str, entity_id: Optional[str] = None) -> List[Dict[str, Any]]:
        q = self.sb.table(self.TABLE).select("*").eq("transaction_id", transaction_id)
        if entity_id:
            q = q.eq("entity_id", entity_id)
        res = q.execute()
        return res.data or []

    def list_by_transaction_and_source(
        self,
        *,
        transaction_id: str,
        source_type: str,
        entity_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        q = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("transaction_id", transaction_id)
            .eq("source_type", source_type)
        )
        if entity_id:
            q = q.eq("entity_id", entity_id)
        res = q.execute()
        return res.data or []

    def list_by_transaction_and_sources(
        self,
        *,
        transaction_id: str,
        source_types: List[str],
        entity_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        try:
            q = (
                self.sb.table(self.TABLE)
                .select("*")
                .eq("transaction_id", transaction_id)
                .in_("source_type", source_types)
            )
            if entity_id:
                q = q.eq("entity_id", entity_id)
            res = q.execute()
            return res.data or []
        except Exception:
            out: List[Dict[str, Any]] = []
            for t in source_types or []:
                out.extend(
                    self.list_by_transaction_and_source(
                        transaction_id=transaction_id,
                        source_type=t,
                        entity_id=entity_id,
                    )
                )
            return out

    def list_recent_po_prices_by_sku(
        self,
        *,
        sku: str,
        entity_id: Optional[str] = None,
        exclude_transaction_id: Optional[str] = None,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        """
        Historical PO prices for procurement baseline derivation.

        Rules:
        - only PO source_type
        - same SKU
        - optional entity filter
        - exclude current transaction if provided
        - newest first (created_at desc)
        """
        q = (
            self.sb.table(self.TABLE)
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

        res = q.execute()
        rows = res.data or []

        if exclude_transaction_id:
            rows = [
                r for r in rows
                if str(r.get("transaction_id") or "") != str(exclude_transaction_id)
            ]

        return rows

    def list_recent_po_prices_by_sku_uom(
        self,
        *,
        sku: str,
        uom: Optional[str] = None,
        entity_id: Optional[str] = None,
        exclude_transaction_id: Optional[str] = None,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        """
        Same as list_recent_po_prices_by_sku, but prefer matching UOM.
        Falls back to SKU-only if no UOM provided.
        """
        q = (
            self.sb.table(self.TABLE)
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

        res = q.execute()
        rows = res.data or []

        if exclude_transaction_id:
            rows = [
                r for r in rows
                if str(r.get("transaction_id") or "") != str(exclude_transaction_id)
            ]

        return rows