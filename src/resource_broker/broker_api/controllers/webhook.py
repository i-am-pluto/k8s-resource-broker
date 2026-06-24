from __future__ import annotations

from fastapi import APIRouter, Request
from structlog import get_logger

from resource_broker.broker_api.controllers.handler import handle_admission_review
from resource_broker.common.models.schemas import AdmissionReviewResponse

logger = get_logger(__name__)

router = APIRouter()


@router.post("/mutate")
async def mutate(request: Request) -> AdmissionReviewResponse:
    body = await request.json()
    logger.debug("admission review received", uid=body.get("request", {}).get("uid"))

    svc = request.app.state.recommendation_svc
    response = await handle_admission_review(body, svc)
    return AdmissionReviewResponse(response=response)
