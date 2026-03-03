# app/repositories/case_repo.py

from app.repositories.base import BaseRepository
from typing import List, Dict, Any, Optional


class CaseRepository(BaseRepository):
    TABLE = "dcc_cases"
    VIEW = "vw_case_list"
    LINE_TABLE = "dcc_case_line_items"

    # =====================================================
    # Constructor (REQUIRED)
    # =====================================================
    def __init__(self, sb):
        super().__init__(sb)

    # =====================================================
    # Read – single case
    # =====================================================
    def get(self, case_id: str) -> Optional[Dict[str, Any]]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    def get_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        """
        Alias of get() – kept for backward compatibility
        """
        return self.get(case_id)

    # =====================================================
    # Idempotency helper
    # =====================================================
    def find_by_reference(
        self,
        reference_type: str,
        reference_id: str,
    ) -> Optional[Dict[str, Any]]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("reference_type", reference_type)
            .eq("reference_id", reference_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    # =====================================================
    # Write
    # =====================================================
    def create(self, payload: dict) -> Dict[str, Any]:
        res = (
            self.sb
            .table(self.TABLE)
            .insert(payload)
            .execute()
        )
        if not res.data:
            raise RuntimeError("Failed to create case")
        return res.data[0]
    
    def update_transaction_id(self, case_id: str, transaction_id: str):
        res = (
            self.sb.table(self.TABLE)
            .update({"transaction_id": transaction_id})
            .eq("case_id", case_id)
            .execute()
        )
        return res.data
    
    # ตรวจสอบว่า เตรียมข้อมูล evidence ยัง ถ้าเตรียมแล้วให้ข้าม
    def merge_case_detail(self, case_id: str, patch: dict):
        current = (
            self.sb.table(self.TABLE)
            .select("case_detail")
            .eq("case_id", case_id)
            .single()
            .execute()
        )

        existing = current.data.get("case_detail") or {}

        # shallow merge (safe for ui object)
        merged = {**existing, **patch}

        return (
            self.sb.table(self.TABLE)
            .update({"case_detail": merged})
            .eq("case_id", case_id)
            .execute()
        )

    # =====================================================
    # List – cases (VIEW)
    # =====================================================
    def list_cases(self) -> List[Dict[str, Any]]:
        res = (
            self.sb
            .table(self.VIEW)
            .select("*")
            .order("created_at", desc=True)
            .execute()
        )
        return res.data or []

    def list_cases_paginated(
        self,
        *,
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Pagination via Supabase range()
        """
        res = (
            self.sb
            .table(self.VIEW)
            .select("*")
            .order("created_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return res.data or []

    def count_cases(self) -> int:
        res = (
            self.sb
            .table(self.VIEW)
            .select("case_id", count="exact")
            .execute()
        )
        return res.count or 0

    # =====================================================
    # Read – line items (snapshot)
    # =====================================================
    def list_line_items(self, case_id: str) -> List[Dict[str, Any]]:
        res = (
            self.sb
            .table(self.LINE_TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("source_line_ref")
            .execute()
        )
        return res.data or []

        # =====================================================
    # Read – single case (with entity join)
    # =====================================================
    def get_with_entity(self, case_id: str) -> Optional[Dict[str, Any]]:
        """
        Returns case joined with entity information.
        Does NOT modify original get() for backward compatibility.
        """

        res = (
            self.sb
            .table(self.TABLE)
            .select(
                """
                *,
                entity:dcc_entities(
                    entity_id,
                    entity_type,
                    entity_code,
                    entity_name,
                    metadata
                )
                """
            )
            .eq("case_id", case_id)
            .limit(1)
            .execute()
        )

        if not res.data:
            return None

        row = res.data[0]

        entity = row.pop("entity", None)

        if entity:
            row["entity"] = {
                "entity_id": entity.get("entity_id"),
                "entity_type": entity.get("entity_type"),
                "entity_code": entity.get("entity_code"),
                "entity_name": entity.get("entity_name"),
                "metadata": entity.get("metadata") or {},
            }

        return row
    
# =====================================================
# Decision pointer update (enterprise minimal)
# =====================================================

    def update_after_run(
        self,
        case_id: str,
        *,
        run_id: str,
        decision: str,
        risk_level: str,
        confidence: float,
    ):
        """
        Update case pointer after successful decision run.

        This does NOT trigger any decision logic.
        Pure persistence layer.
        """

        res = (
            self.sb
            .table(self.TABLE)
            .update({
                "current_run_id": run_id,
                "decision": decision,
                "risk_level": risk_level,
                "confidence_score": confidence,
                "case_uuid" : case_id,
                "updated_at": "now()",
            })
            .eq("case_id", case_id)
            .execute()
        )

        return res.data
    
   