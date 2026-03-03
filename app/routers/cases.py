from fastapi import APIRouter, Header, HTTPException , Depends, Query , Request
from app.services.case.case_service import CaseService

from app.services.signal.signal_extraction_service import SignalExtractionService
from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.services.case.case_decision_summary_service import CaseDecisionSummaryService
from app.services.case.case_group_service import CaseGroupService
from app.services.case.case_processing_run_service import CaseProcessingRunService
from app.repositories.case_decision_result_repo import CaseDecisionResultRepository

from app.services.result.decision_run_view_mapper  import to_decision_run_view_context
from app.schemas.decision_run_view_model import DecisionRunViewContext
from app.services.policy.registry import PolicyRegistry


from typing import Dict, Any, List ,Optional

from app.services.case.case_service import CaseService
from app.services.audit.audit_timeline_builder import AuditTimelineBuilder
from app.services.audit.audit_models import AuditTimelineContext
from app.repositories.audit_repo import AuditRepository
from app.services.audit.audit_timeline_builder_v1 import AuditTimelineBuilderV1
import uuid

from fastapi import Request
from app.services.context.raw_loader import load_raw_decision_results_by_case
from app.services.context.view_builder import build_decision_view
from app.services.context.copilot_projection import project_copilot_lite

from app.services.case.case_models import (
    CreateCaseFromPORequest,
    CaseResponse,
    CaseAggregateResponse,CreateCaseFromPORequest,CaseResponse
)



router = APIRouter()


@router.post("/cases/ingest-from-po", response_model=CaseResponse)
def create_case_from_po(
    request: Request,
    payload: CreateCaseFromPORequest,
    x_actor_id: str = Header(default="SYSTEM")
):
    sb = request.state.sb
    service = CaseService(sb)
    case = service.create_case_from_po(
        payload.model_dump(),
        actor_id=x_actor_id
    )

    if not case:
        raise HTTPException(status_code=500, detail="Failed to create case")

    return {
        "case_id": case["case_id"],
        "reference_type": case["reference_type"],
        "reference_id": case["reference_id"],
        "status": case["status"]
    }

@router.get("/cases/{case_id}/signals")
def debug_case_signals(request: Request, case_id: str):
    """
    DEBUG endpoint
    - Extract signals from case + PO snapshot
    - No DB write
    - Deterministic, recomputable
    """
    sb = request.state.sb
    case_repo = CaseRepository(sb)
    line_item_repo = CaseLineItemRepository(sb)
    # 1. Load case
    case = case_repo.get(case_id)
    
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    # 2. Load immutable PO snapshot
    line_items = line_item_repo.list_by_case(case_id)
    if not line_items:
        raise HTTPException(
            status_code=400,
            detail="No line items found for case"
        )

    # 3. Extract signals
    # ไม่ได้เรียก database no neeed sb
    signals = SignalExtractionService.extract(case, line_items)

    # 4. Return as JSON (Pydantic -> dict)
    return signals.model_dump()

@router.get("/cases/{case_id}/documents")
def list_case_documents(request: Request, case_id: str):
    sb = request.state.sb
    repo = CaseDocumentLinkRepository(sb)
    return {
        "case_id": case_id,
        "documents": repo.list_by_case(case_id)
    }

@router.post("/case-document-links/{link_id}/confirm")
def confirm_document(request: Request, link_id: str, body: dict):
    actor_id = body.get("actor_id")
    if not actor_id:
        raise HTTPException(400, "actor_id required")

    repo = CaseDocumentLinkRepository(sb=request.state.sb)
    repo.confirm(link_id, actor_id)

    return {"status": "confirmed", "link_id": link_id}


@router.post("/case-document-links/{link_id}/remove")
def remove_document(request: Request, link_id: str, body: dict):
    actor_id = body.get("actor_id")
    if not actor_id:
        raise HTTPException(400, "actor_id required")

    repo = CaseDocumentLinkRepository()
    repo.remove(link_id, actor_id)

    return {"status": "removed", "link_id": link_id}

@router.get("/cases", summary="List cases")
def list_cases(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    sb = request.state.sb
    try:
        service = CaseService(sb)
        return service.get_case_list(
            page=page,
            page_size=page_size,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
        
        
@router.get(
    "/cases/{case_id}",
    summary="Get case detail",
)
def get_case_detail(request: Request, case_id: str) -> Dict[str, Any]:
    """
    Case Detail
    - Case header
    - Immutable PO line items
    """

    try:
        service = CaseService(sb=request.state.sb)
       

        return service.get_case_detail(case_id)

    except ValueError as ve:
        raise HTTPException(
            status_code=404,
            detail=str(ve),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
        
@router.get(
    "/cases/{case_id}/decision-summary",
    summary="Case decision summary (latest COMPLETED run)"
)
def get_case_decision_summary(request: Request, case_id: str):
    case_decision = CaseDecisionSummaryService(request.state.sb)
    try:
        return case_decision.get_decision_summary(case_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/cases/{case_id}/decision-results")
def get_case_decision_results(
    case_id: str,
    request: Request,
    run_id: str | None = None,
):
    sb = request.state.sb

    repo = CaseDecisionResultRepository(sb)

    results = repo.list_by_case(
        case_id=case_id,
        run_id=run_id,
    )

    return {
        "case_id": case_id,
        "run_id": run_id,
        "count": len(results),
        "results": results,
    }

 
@router.get(
    "/cases/{case_id}/groups",
    summary="Audit-grade group drill-down"
)
def get_case_groups(request: Request, case_id: str):
    case_group_service = CaseGroupService(request.state.sb)
    try:
        return case_group_service.get_groups(case_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/cases/{case_id}/process")
def process_case(
    request: Request,
    case_id: str,
    actor_id: str = Query("SYSTEM"),
):
    sb = request.state.sb
    service = CaseProcessingRunService(sb)
    
    case_repo = CaseRepository(sb)
    case = case_repo.get_with_entity(case_id)
    case_domain = case.get("domain")
    
    print(case_domain)

    result = service.run(
        case_id=case_id,
        domain=case_domain,
        actor_id=actor_id,
    )

    return result    

async def _load_raw_decision_run(request: Request, case_id: str):

    sb = request.state.sb
    repo = CaseDecisionResultRepository(sb)

    _validate_uuid(case_id, "case_id")

    # repo คืน list[dict]
    results: list[dict] = repo.list_by_case(case_id=case_id) or []

    # derive run_id จาก item แรก (ถ้ามี)
    run_id = results[0].get("run_id") if results else None

    return {
        "case_id": case_id,
        "run_id": run_id,
        "count": len(results),
        "results": results,
    }
   
def _validate_uuid(v: str, name: str):
    try:
        uuid.UUID(str(v))
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"{name} is not valid uuid: {v}"
        )

@router.get("/cases/{case_id}/view", response_model=DecisionRunViewContext)
async def get_decision_run_view(request: Request, case_id: str):

    raw = await _load_raw_decision_run(request, case_id)

    policy_registry = PolicyRegistry.get()

    return to_decision_run_view_context(raw, policy_registry)

@router.get("/cases/{case_id}/audit", response_model=AuditTimelineContext)
async def get_case_audit(request: Request, case_id: str):

    repo = AuditRepository(request.state.sb)

    raw_events = repo.list_events_by_case(case_id)

    return AuditTimelineBuilder.build(case_id, raw_events)

@router.get("/cases/{case_id}/audit-timeline")
async def get_case_timeline(request: Request, case_id: str):
    sb = request.state.sb
    repo = AuditRepository(sb)

    raw_events = repo.list_events_by_case(case_id)

    return AuditTimelineBuilderV1.build(
        case_id=case_id,
        raw_events=raw_events,
    )
    
@router.get("/cases/{case_id}/v2/view")
async def get_case_view_v2(request: Request, case_id: str):
    
    sb = request.state.sb
    repo = AuditRepository(sb)

    raw_events = repo.list_events_by_case(case_id)
    
    raw = await load_raw_decision_results_by_case(
        request,
        case_id,
        raw_events=raw_events,
    )
    return build_decision_view(raw)

# ==========================================================
# ENTERPRISE AGGREGATE (Frontend Ready)
# ==========================================================

@router.get(
    "/cases/{case_id}/aggregate",
    response_model=CaseAggregateResponse,
    summary="Enterprise Case Aggregate (Frontend Ready)"
)
def get_case_aggregate(request: Request, case_id: str):

    _validate_uuid(case_id, "case_id")

    try:
        service = CaseService(request.state.sb)
        return service.get_case_aggregate(case_id)

    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
