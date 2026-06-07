from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from structlog import get_logger

from resource_broker.api.schemas import PatchOperationSchema, RecommendationRequest, RecommendationResponse

logger = get_logger(__name__)

router = APIRouter()


@router.post("/", response_model=RecommendationResponse)
async def get_recommendations(req: RecommendationRequest, request: Request) -> RecommendationResponse:
    svc = request.app.state.recommendation_svc
    profile = svc.registry.get(req.profile_name, req.profile_namespace)

    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"profile '{req.profile_name}' not found in namespace '{req.profile_namespace}'",
        )

    patches = await svc.get_patches(profile, req.pod_spec)
    return RecommendationResponse(
        profile_name=profile.name,
        patches=[PatchOperationSchema(**p) for p in patches],
    )
