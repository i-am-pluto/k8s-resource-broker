from __future__ import annotations

from fastapi import APIRouter

from resource_broker.common.dao.database import check_connection
from resource_broker.common.models.schemas import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    db_ok = await check_connection()
    return HealthResponse(database=db_ok)
