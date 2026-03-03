from typing import List, Dict, Any

from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.audit_repo import AuditRepository
from app.repositories.base import json_safe

from app.repositories.transaction_repo import TransactionRepository
from app.repositories.transaction_line_item_repo import TransactionLineItemRepository
from app.repositories.case_repo_ext import CaseRepositoryExt


class CaseService:
    """
    CaseService (enterprise wiring)

    Responsibilities:
    - Create Procurement Case
    - Seed Transaction Root (PROCUREMENT_FLOW)
    - Seed Ledger (PO lines)
    - Update case.transaction_id
    - Emit audit events

    No redesign of existing procurement logic.
    """

    def __init__(self, sb):
        self.sb = sb
        self.case_repo = CaseRepository(sb)
        self.line_item_repo = CaseLineItemRepository(sb)
        self.audit_repo = AuditRepository(sb)

        self.txn_repo = TransactionRepository(sb)
        self.txn_line_repo = TransactionLineItemRepository(sb)

    # ==========================================================
    # CREATE CASE FROM PO
    # ==========================================================

    def create_case_from_po(self, po_payload: dict, actor_id: str = "SYSTEM"):

        reference_type = po_payload["reference_type"]
        reference_id = po_payload["reference_id"]

        # ------------------------------------------------------
        # 1) Idempotency check (reference-based)
        # ------------------------------------------------------
        existing = self.case_repo.find_by_reference(
            reference_type,
            reference_id,
        )
        if existing:
            return existing

        # ------------------------------------------------------
        # 2) Create Case Header
        # ------------------------------------------------------
        case = self.case_repo.create({
            "entity_id": po_payload["entity_id"],
            "entity_type": po_payload["entity_type"],
            "domain": po_payload["domain"],
            "reference_type": reference_type,
            "reference_id": reference_id,
            "amount_total": po_payload.get("amount_total"),
            "currency": po_payload.get("currency"),
            "status": "OPEN",
            "created_by": actor_id,
        })

        case_id = case["case_id"]
        po_number = reference_id

        # ------------------------------------------------------
        # 3) Snapshot PO Line Items (IMMUTABLE)
        # ------------------------------------------------------
        line_items_payload = []
        for item in po_payload.get("line_items", []) or []:

            qty = item.get("quantity") or 0
            unit = item.get("unit_price") or 0

            line_items_payload.append({
                "case_id": case_id,
                "source_line_ref": item.get("source_line_ref"),
                "sku": item.get("sku"),
                "item_name": item.get("item_name"),
                "description": item.get("description"),
                "quantity": qty,
                "uom": item.get("uom"),
                "unit_price": unit,
                "currency": item.get("currency"),
                "total_price": qty * unit,
            })

        if line_items_payload:
            self.line_item_repo.bulk_insert(line_items_payload)

        # ======================================================
        # 4) Seed Transaction Root (PROCUREMENT_FLOW)
        # ======================================================

        txn = self.txn_repo.get_by_aggregate(
            aggregate_type="PROCUREMENT_FLOW",
            aggregate_key=po_number,
        )

        if not txn:
            txn = self.txn_repo.create(
                aggregate_type="PROCUREMENT_FLOW",
                aggregate_key=po_number,
                entity_id=po_payload["entity_id"],
                entity_type=po_payload["entity_type"],
                currency=po_payload.get("currency"),
                amount_total=po_payload.get("amount_total"),
                lifecycle_status="OPEN",
                metadata_json={
                    "seeded_from": "PROCUREMENT_CASE",
                    "case_id": case_id,
                },
                created_by=actor_id,
            )

        transaction_id = txn["transaction_id"]

        # ------------------------------------------------------
        # 5) Update Case with transaction_id (CRITICAL)
        # ------------------------------------------------------

        self.case_repo.update_transaction_id(
            case_id=case_id,
            transaction_id=transaction_id
        )
        
        
    

        # ======================================================
        # 6) Copy PO lines into Transaction Ledger
        # ======================================================

        ledger_rows = []

        for idx, item in enumerate(po_payload.get("line_items", []) or []):
            qty = item.get("quantity") or 0
            unit = item.get("unit_price") or 0

            ledger_rows.append({
                "transaction_id": transaction_id,
                "source_type": "PO",
                "source_ref_id": po_number,
                "source_line_ref": item.get("source_line_ref") or str(idx + 1),

                "entity_id": po_payload["entity_id"],

                "sku": item.get("sku"),
                "item_name": item.get("item_name"),
                "description": item.get("description"),
                "uom": item.get("uom"),
                "quantity": qty,
                "unit_price": unit,
                "currency": item.get("currency"),
                "amount": qty * unit,

                "source_system": "ERP",
                "trust_level": "HIGH",
                "document_id": None,
                "metadata_json": {
                    "seeded_from_case_id": case_id,
                },
                "created_by": actor_id,
            })

        if ledger_rows:
            try:
                self.txn_line_repo.insert_many(ledger_rows)
            except Exception:
                # Ignore duplicate insertion (idempotent safe)
                pass

        self.case_repo.merge_case_detail(
            case_id=case_id,
            patch={
                "ui": {
                    "transaction_seeded": True,
                    "ledger_seeded": True,
                    "po_line_count": len(ledger_rows),
                }
            }
        )        
        # ======================================================
        # 7) Audit Events
        # ======================================================

        self.audit_repo.emit(
            case_id=case_id,
            event_type="CASE_CREATED_FROM_PO",
            actor=actor_id,
            payload={
                "reference_type": reference_type,
                "reference_id": reference_id,
                "entity_id": po_payload["entity_id"],
                "entity_type": po_payload["entity_type"],
                "domain": po_payload["domain"],
            },
        )

        self.audit_repo.emit(
            case_id=case_id,
            event_type="PROCUREMENT_TRANSACTION_SEEDED",
            actor=actor_id,
            payload={
                "transaction_id": transaction_id,
                "aggregate_type": "PROCUREMENT_FLOW",
                "aggregate_key": po_number,
                "ledger_lines": len(ledger_rows),
            },
        )

        return case

    # ==========================================================
    # LIST CASES
    # ==========================================================

    def get_case_list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:

        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 20

        offset = (page - 1) * page_size

        rows = self.case_repo.list_cases_paginated(
            offset=offset,
            limit=page_size,
        )
        total = self.case_repo.count_cases()

        items: List[Dict[str, Any]] = []
        for r in rows or []:
            items.append(json_safe({
                "case_id": r.get("case_id"),
                "transaction_id": r.get("transaction_id"),
                "domain": r.get("domain"),
                "reference_type": r.get("reference_type"),
                "reference_id": r.get("reference_id"),

                "entity_id": r.get("entity_id"),
                "entity_type": r.get("entity_type"),
                "entity_name": r.get("entity_name"),

                "amount_total": r.get("amount_total"),
                "currency": r.get("currency"),

                "status": r.get("status"),
                "decision": r.get("decision"),
                "risk_level": r.get("risk_level"),
                "confidence": r.get("confidence_score"),

                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
            }))

        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    # ==========================================================
    # CASE DETAIL
    # ==========================================================

    def get_case_detail(self, case_id: str) -> Dict[str, Any]:
        case = self.case_repo.get_case(case_id)
        if not case:
            raise ValueError("Case not found")

        line_items = self.line_item_repo.list_by_case(case_id)

        return json_safe({
            "case": {
                "case_id": case.get("case_id"),
                "transaction_id": case.get("transaction_id"),
                "entity_id": case.get("entity_id"),
                "entity_type": case.get("entity_type"),
                "domain": case.get("domain"),
                "reference_type": case.get("reference_type"),
                "reference_id": case.get("reference_id"),
                "amount_total": case.get("amount_total"),
                "currency": case.get("currency"),
                "status": case.get("status"),
                "created_by": case.get("created_by"),
                "created_at": case.get("created_at"),
                "updated_at": case.get("updated_at"),
            },
            "line_items": [
                {
                    "item_id": li.get("item_id"),
                    "source_line_ref": li.get("source_line_ref"),
                    "sku": li.get("sku"),
                    "item_name": li.get("item_name"),
                    "description": li.get("description"),
                    "quantity": li.get("quantity"),
                    "uom": li.get("uom"),
                    "unit_price": li.get("unit_price"),
                    "currency": li.get("currency"),
                    "total_price": li.get("total_price"),
                    "created_at": li.get("created_at"),
                }
                for li in (line_items or [])
            ],
        })

        # ==========================================================
    # ENTERPRISE AGGREGATE (Model-aligned, frontend-ready)
    # ==========================================================
    def get_case_aggregate(self, case_id: str) -> Dict[str, Any]:

        case = self.case_repo.get_with_entity(case_id)
        if not case:
            raise ValueError("Case not found")

        # immutable snapshot
        line_items = self.case_repo.list_line_items(case_id) or []

        # =====================================================
        # Artifact Summary (from case_detail.ui)
        # =====================================================
        attachment_summary: Dict[str, Any] = {
            "total_count": 0,
            "by_type": {}
        }

        case_detail = case.get("case_detail") or {}
        if isinstance(case_detail, dict):
            ui = case_detail.get("ui") or {}

            attachment_summary["total_count"] = int(
                ui.get("attachment_count", 0) or 0
            )

            if isinstance(ui.get("attachment_by_type"), dict):
                attachment_summary["by_type"] = ui.get("attachment_by_type") or {}

        # =====================================================
        # Decision Tracks (multi-domain ready)
        # =====================================================
        decision_tracks: List[Dict[str, Any]] = []

        domain_val = case.get("domain")
        if domain_val:
            decision_tracks.append({
                "domain": str(domain_val).lower(),
                "latest_decision": case.get("decision"),
                "risk_level": case.get("risk_level"),
                "confidence": case.get("confidence_score"),
                "last_run_id": case.get("current_run_id"),
            })

        # =====================================================
        # Audit Summary
        # =====================================================
        audit_summary: Dict[str, Any] = {
            "has_transaction": bool(case.get("transaction_id")),
            "status": case.get("status"),
        }

        # =====================================================
        # Case Master (aligned with CaseMaster model)
        # =====================================================
        case_master: Dict[str, Any] = {
            "case_id": case.get("case_id"),
            "reference_type": case.get("reference_type"),
            "reference_id": case.get("reference_id"),
            "domain": case.get("domain"),
            "status": case.get("status"),
            "transaction_id": case.get("transaction_id"),
            "created_at": case.get("created_at"),
            "updated_at": case.get("updated_at"),
            "entity": case.get("entity") or None,
        }

        # =====================================================
        # Line Items (aligned with CaseLineItem model)
        # =====================================================
        items: List[Dict[str, Any]] = []

        for li in line_items:
            items.append({
                "item_id": li.get("item_id"),
                "source_line_ref": li.get("source_line_ref"),
                "sku": li.get("sku"),
                "item_name": li.get("item_name"),
                "description": li.get("description"),
                "quantity": li.get("quantity"),
                "uom": li.get("uom"),
                "unit_price": li.get("unit_price"),
                "currency": li.get("currency"),
                "total_price": li.get("total_price"),
            })

        # =====================================================
        # Final Aggregate (match CaseAggregateResponse)
        # =====================================================
        return json_safe({
            "case": case_master,
            "artifacts": attachment_summary,
            "decision_tracks": decision_tracks,
            "audit": audit_summary,
            "line_items": items,
        })