from __future__ import annotations

import base64
import json
from typing import Any

from structlog import get_logger

from resource_broker.common.services.recommendation_service import RecommendationService
from resource_broker.config import settings

logger = get_logger(__name__)


async def handle_admission_review(
    body: dict[str, Any],
    svc: RecommendationService,
) -> dict[str, Any]:
    req = body.get("request") or {}
    uid = req.get("uid", "")
    obj = req.get("object", {})
    metadata = obj.get("metadata", {})
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})

    # objectSelector in MutatingWebhookConfiguration matches on labels, so check
    # labels first; fall back to annotations for backward compatibility.
    profile_name = labels.get(settings.profile_annotation_key) or annotations.get(settings.profile_annotation_key)

    if not profile_name:
        logger.debug("no profile label/annotation, allowing without patch", uid=uid)
        return _allow_response(uid)

    try:
        return await _process_profile(uid, profile_name, obj, svc)
    except Exception:
        logger.exception("webhook handler failed, failing open", uid=uid)
        return _allow_response(uid)


async def _process_profile(
    uid: str,
    profile_name: str,
    obj: dict[str, Any],
    svc: RecommendationService,
) -> dict[str, Any]:
    namespace = obj.get("metadata", {}).get("namespace", "default")
    profile = svc.registry.get(profile_name, namespace)

    if profile is None:
        logger.warning("profile not found in registry, allowing without patch", name=profile_name, namespace=namespace)
        return _allow_response(uid)

    patches = await svc.get_patches(profile, obj)

    if not patches:
        return _allow_response(uid)

    patch_json = json.dumps(patches)
    patch_b64 = base64.b64encode(patch_json.encode()).decode()

    return {
        "uid": uid,
        "allowed": True,
        "patchType": "JSONPatch",
        "patch": patch_b64,
    }


def _allow_response(uid: str) -> dict[str, Any]:
    return {"uid": uid, "allowed": True}
