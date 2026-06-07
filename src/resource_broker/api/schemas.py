from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "0.1.0"
    database: bool = False


class FieldEntrySchema(BaseModel):
    locator: str | None = None
    min: str | None = None
    max: str | None = None
    strategy: dict[str, Any] | None = None


class ProfileCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., min_length=1, max_length=255)
    namespace: str = "default"
    resource_type: str = Field(..., alias="resource-type")
    mode: str = "recommendation"
    strategy: dict[str, Any] | None = None
    fields: dict[str, FieldEntrySchema] = Field(default_factory=dict)


class ProfileResponse(BaseModel):
    name: str
    namespace: str
    resource_type: str
    mode: str
    strategy: dict[str, Any] | None
    fields: dict[str, Any]


class ProfileListResponse(BaseModel):
    profiles: list[ProfileResponse]
    total: int


class RecommendationRequest(BaseModel):
    profile_name: str
    profile_namespace: str = "default"
    pod_spec: dict[str, Any] = Field(default_factory=dict)


class PatchOperationSchema(BaseModel):
    op: str
    path: str
    value: Any = None


class RecommendationResponse(BaseModel):
    profile_name: str
    patches: list[PatchOperationSchema]


class AdmissionReviewResponse(BaseModel):
    apiVersion: str = "admission.k8s.io/v1"
    kind: str = "AdmissionReview"
    response: dict[str, Any]
