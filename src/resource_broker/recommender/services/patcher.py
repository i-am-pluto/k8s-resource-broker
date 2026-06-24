from __future__ import annotations

from typing import Any

from structlog import get_logger

from resource_broker.common.models.profile import ResourceProfile
from resource_broker.common.resource_types.registry import resource_type_registry

logger = get_logger(__name__)


async def compute_patches(
    profile: ResourceProfile,
    pod_spec: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    ctx: dict[str, Any] = dict(context or {})
    ctx["pod_spec"] = pod_spec
    ctx["profile_name"] = profile.name

    rtype = resource_type_registry.get(profile.resource_type)

    patches = await rtype.build_patches(
        fields=profile.fields,
        strategy=profile.strategy,
        context=ctx,
    )

    result = [p.to_dict() for p in patches]

    logger.debug(
        "patches computed",
        profile=profile.name,
        resource_type=profile.resource_type,
        patch_count=len(result),
    )

    return result
