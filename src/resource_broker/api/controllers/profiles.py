from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.api.schemas import ProfileCreate, ProfileListResponse, ProfileResponse
from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.models.profile import ResourceProfile
from resource_broker.common.services.profile_registry import CRD_GROUP, CRD_PLURAL, CRD_VERSION

logger = get_logger(__name__)

router = APIRouter()


@router.post("/", response_model=ProfileResponse, status_code=status.HTTP_201_CREATED)
async def create_profile(data: ProfileCreate) -> ProfileResponse:
    api = create_k8s_api(k8s_client.CustomObjectsApi)
    body = {
        "apiVersion": f"{CRD_GROUP}/{CRD_VERSION}",
        "kind": "ResourceProfile",
        "metadata": {"name": data.name, "namespace": data.namespace},
        "spec": {
            "resource-type": data.resource_type,
            "mode": data.mode,
            "strategy": data.strategy,
            "fields": {
                name: entry.model_dump(exclude_none=True)
                for name, entry in data.fields.items()
            },
        },
    }
    try:
        api.create_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=data.namespace,
            plural=CRD_PLURAL,
            body=body,
        )
        profile = ResourceProfile.from_crd(body)
        return ProfileResponse(**profile.to_dict())
    except k8s_client.exceptions.ApiException as exc:
        if exc.status == 409:
            msg = f"profile '{data.name}' already exists"
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=msg) from exc
        logger.error("failed to create profile", name=data.name, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to create profile",
        ) from exc


@router.get("/", response_model=ProfileListResponse)
async def list_profiles(namespace: str = "default") -> ProfileListResponse:
    api = create_k8s_api(k8s_client.CustomObjectsApi)
    try:
        crd_list = api.list_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=namespace,
            plural=CRD_PLURAL,
        )
        items = crd_list.get("items", [])
        profiles = [ResourceProfile.from_crd(item) for item in items]
        return ProfileListResponse(
            profiles=[ProfileResponse(**p.to_dict()) for p in profiles],
            total=len(profiles),
        )
    except k8s_client.exceptions.ApiException as exc:
        logger.error("failed to list profiles", namespace=namespace, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to list profiles",
        ) from exc


@router.get("/{name}", response_model=ProfileResponse)
async def get_profile(name: str, namespace: str = "default") -> ProfileResponse:
    api = create_k8s_api(k8s_client.CustomObjectsApi)
    try:
        crd = api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=namespace,
            plural=CRD_PLURAL,
            name=name,
        )
        profile = ResourceProfile.from_crd(crd)
        return ProfileResponse(**profile.to_dict())
    except k8s_client.exceptions.ApiException as exc:
        if exc.status == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="profile not found",
            ) from exc
        logger.error("failed to get profile", name=name, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to get profile",
        ) from exc


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_profile(name: str, namespace: str = "default") -> None:
    api = create_k8s_api(k8s_client.CustomObjectsApi)
    try:
        api.delete_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=namespace,
            plural=CRD_PLURAL,
            name=name,
        )
    except k8s_client.exceptions.ApiException as exc:
        if exc.status == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="profile not found",
            ) from exc
        logger.error("failed to delete profile", name=name, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to delete profile",
        ) from exc
