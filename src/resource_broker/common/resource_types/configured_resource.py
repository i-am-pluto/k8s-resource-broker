"""Re-exported from common/models/configured_resources/.

ConfiguredResource and its registry have moved to common/models/configured_resources/.
This shim exists for backward compat during the transition.
"""
from resource_broker.common.models.configured_resources import (  # noqa: F401
    ConfiguredResource,
    ConfiguredResourceRegistry,
    K8sConfiguredResource,
    configured_resource_registry,
)
