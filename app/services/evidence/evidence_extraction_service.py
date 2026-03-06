from __future__ import annotations

from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.clause_repo import ClauseRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository


class EvidenceExtractionService:
    """
    C3 — Evidence Extraction (LOCKED)

    PURPOSE:
    - Extract atomic evidences from confirmed documents
    - Attach PRICE evidences to PO_ITEM anchor
    - CLAUSE evidences are unanchored (grouping later)

    IMPORTANT:
    - dcc_case_evidences.document_id is NOT NULL
    - therefore only document-backed evidence may be inserted here
    - historical ERP / PO transactions must NOT be inserted into this table
    """

    def __init__(self, *, sb):
        self.sb = sb

        self.link_repo = CaseDocumentLinkRepository(sb)
        self.price_repo = PriceItemRepository(sb)
        self.clause_repo = ClauseRepository(sb)
        self.evidence_repo = CaseEvidenceRepository(sb)
        self.doc_repo = DocumentRepository(sb)
        self.line_repo = CaseLineItemRepository(sb)

    def extract(self, case_id: str, actor_id: str = "SYSTEM"):
        po_lines = self.line_repo.list_by_case(case_id) or []
        sku_to_item_id = {
            li.get("sku"): li.get("item_id")
            for li in po_lines
            if li.get("sku") and li.get("item_id")
        }

        confirmed_links = self.link_repo.list_confirmed(case_id) or []

        if not confirmed_links:
            return {
                "case_id": case_id,
                "status": "no_confirmed_documents",
                "evidence_created": 0,
            }

        evidence_count = 0

        for link in confirmed_links:
            document_id = link["document_id"]

            document = self.doc_repo.get(document_id)
            if not document:
                continue

            source = "CONTRACT" if document.get("contract_id") else "OTHER"

            # =========================
            # PRICE EVIDENCE (ANCHOR REQUIRED)
            # =========================
            for item in self.price_repo.list_by_document(document_id) or []:
                sku = item.get("sku")
                item_id = sku_to_item_id.get(sku)

                if not item_id:
                    continue

                self.evidence_repo.insert({
                    "case_id": case_id,
                    "document_id": document_id,
                    "evidence_type": "PRICE",
                    "extraction_method": "STRUCTURED_TABLE",
                    "source": source,
                    "anchor_type": "PO_ITEM",
                    "anchor_id": item_id,
                    "evidence_payload": {
                        "sku": sku,
                        "unit_price": item.get("unit_price"),
                        "currency": item.get("currency"),
                        "uom": item.get("uom"),
                        "observed_at": document.get("created_at"),
                        "source_ref": document_id,
                    },
                    "source_snippet": item.get("snippet"),
                    "source_page": item.get("page_number"),
                    "confidence": item.get("confidence_score", 0.0),
                    "created_by": actor_id,
                })
                evidence_count += 1

            # =========================
            # CLAUSE EVIDENCE (NO ANCHOR)
            # =========================
            for clause in self.clause_repo.list_by_document(document_id) or []:
                self.evidence_repo.insert({
                    "case_id": case_id,
                    "document_id": document_id,
                    "evidence_type": "CLAUSE",
                    "extraction_method": "CLAUSE_PARSE",
                    "source": source,
                    "evidence_payload": {
                        "clause_id": clause.get("clause_id"),
                        "type": clause.get("clause_type"),
                        "title": clause.get("clause_title"),
                    },
                    "source_snippet": clause.get("clause_text"),
                    "source_page": clause.get("page_number"),
                    "confidence": clause.get("confidence_score", 1.0),
                    "created_by": actor_id,
                })
                evidence_count += 1

        return {
            "case_id": case_id,
            "status": "evidence_extracted",
            "evidence_created": evidence_count,
        }