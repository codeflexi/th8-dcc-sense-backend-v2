from __future__ import annotations

from typing import List, Dict, Any, Optional

from fastapi.encoders import jsonable_encoder

from app.repositories.base import BaseRepository
from app.repositories.case_repo import CaseRepository


class CaseDecisionResultRepository(BaseRepository):
    """
    Repository for dcc_case_decision_results
    1 row = 1 (run_id, group_id)
    """

    TABLE = "dcc_case_decision_results"
    RUN_TABLE = "dcc_decision_runs"

    def __init__(self,sb):
        super().__init__(sb)
        self.case_repo = CaseRepository(sb)

    def _encode(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Ensure supabase client never sees datetime/Decimal/UUID objects
        return jsonable_encoder(payload)

    def upsert_result(
        self,
        *,
        run_id: str,
        group_id: str,
        decision_status: str,
        risk_level: str,
        confidence: float,
        reason_codes: list,
        fail_actions: list,
        trace: dict,
        evidence_refs: dict,
        created_by: str,
    ) -> None:
        payload = {
            "run_id": run_id,
            "group_id": group_id,
            "decision_status": decision_status,
            "risk_level": risk_level or "LOW",
            "confidence": confidence,
            "reason_codes": reason_codes or [],
            "fail_actions": fail_actions or [],
            "trace": trace or {},
            "evidence_refs": evidence_refs or {"fact_ids": [], "evidence_ids": []},
            "created_by": created_by,
        }

        payload = self._encode(payload)

        self.sb.table(self.TABLE).upsert(
            payload,
            on_conflict="run_id,group_id"
        ).execute()

    def list_by_run(self, run_id: str) -> list[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("run_id", run_id)
            .execute()
        )
        return res.data or []

    def get_latest_by_group(
        self,
        *,
        group_id: str,
    ) -> Optional[Dict[str, Any]]:

        # 1) Get COMPLETED run_ids
        runs_res = (
            self.sb
            .table(self.RUN_TABLE)
            .select("run_id")
            .eq("run_status", "COMPLETED")
            .execute()
        )

        run_ids = [r["run_id"] for r in (runs_res.data or [])]
        if not run_ids:
            return None

        # 2) Get latest result for group
        res = (
            self.sb
            .table(self.TABLE)
            .select("""
                result_id,
                run_id,
                group_id,
                decision_status,
                risk_level,
                confidence,
                reason_codes,
                fail_actions,
                trace,
                evidence_refs,
                created_at
            """)
            .eq("group_id", group_id)
            .in_("run_id", run_ids)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        return res.data[0] if res.data else None
    
    # =========================================================
    # Enterprise: list decision results by case (+ optional run)
    # =========================================================
    def list_by_case(
        self,
        *,
        case_id: str,
        run_id: Optional[str] = None,
    ) -> list[dict]:

        # 1️⃣ หา run_id ทั้งหมดของ case นี้ก่อน
        run_query = (
            self.sb
            .table(self.RUN_TABLE)
            .select("run_id")
            .eq("case_id", case_id)
            .eq("run_status", "COMPLETED")
            .order("created_at", desc=True)
            .limit(1)
        )

        if run_id:
            run_query = run_query.eq("run_id", run_id)

        run_res = run_query.execute()
        run_ids = [r["run_id"] for r in (run_res.data or [])]
        
       
        if not run_ids:
            return []

        # 2️⃣ ดึง results จาก run_ids เหล่านั้น
        res = (
            self.sb
            .table(self.TABLE)
            .select("""
                result_id,
                run_id,
                group_id,
                decision_status,
                risk_level,
                confidence,
                reason_codes,
                fail_actions,
                trace,
                evidence_refs,
                created_at
            """)
            .in_("run_id", run_ids)
            .order("created_at", desc=False)
            .execute()
        )

        return res.data or []

    def sync_after_success(
        self,
        case_id: str,
        run_id: str,
        summary,
    ):
        """
        summary ต้องมี:
            - overall_decision
            - risk_level
            - confidence_avg
        """

        return self.case_repo.update_after_run(
            case_id,
            run_id=run_id,
            decision=summary.get("decision"),
            risk_level=summary.get("risk_level"),
            confidence=summary.get("confidence")
        )
        
    
     # ==========================================================
    # INTERNAL — latest run_id แนะนำให้ใช้ ตามด้านล่าง
    # ==========================================================
    def _get_latest_run_id(self, case_id: str) -> Optional[str]:

        res = (
            self.sb
            .table(self.TABLE)
            .select("run_id, created_at")
            .eq("case_id", case_id)
            .eq("run_status", "COMPLETED")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if not res.data:
            return None

        return res.data[0].get("run_id")

    # ==========================================================
    # PUBLIC — list results (LATEST ONLY)
    # ==========================================================
    def list_by_case_last_run(
        self,
        *,
        case_id: str,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:

        # ---------------------------------------
        # if run_id explicitly provided → use it
        # ---------------------------------------
        if not run_id:
            run_id = self._get_latest_run_id(case_id)

        if not run_id:
            return []

        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("run_id", run_id)
            .order("group_id")
            .execute()
        )

        return res.data or []

    # ==========================================================
    # OPTIONAL — for audit/debug only
    # ==========================================================
    def list_all_runs(
        self,
        case_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Use only for audit/debug
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at", desc=True)
            .execute()
        )
        return res.data or []