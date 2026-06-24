from __future__ import annotations

from resource_broker.common.resource_types.base import FieldDef, ResourceType


class K8sResources(ResourceType):
    name = "k8s-pod"
    description = "Standard Kubernetes container resource fields (CPU, memory, ephemeral-storage)"

    @property
    def fields(self) -> dict[str, FieldDef]:
        return {
            "cpu_request": FieldDef(
                path="/spec/containers/0/resources/requests/cpu",
                default_algorithm="static",
                patch_type="replace",
                description="CPU request for the first container",
            ),
            "memory_request": FieldDef(
                path="/spec/containers/0/resources/requests/memory",
                default_algorithm="static",
                patch_type="replace",
                description="Memory request for the first container",
            ),
            "cpu_limit": FieldDef(
                path="/spec/containers/0/resources/limits/cpu",
                default_algorithm="static",
                patch_type="replace",
                description="CPU limit for the first container",
            ),
            "memory_limit": FieldDef(
                path="/spec/containers/0/resources/limits/memory",
                default_algorithm="static",
                patch_type="replace",
                description="Memory limit for the first container",
            ),
            "ephemeral_storage": FieldDef(
                path="/spec/containers/0/resources/limits/ephemeral-storage",
                default_algorithm="static",
                patch_type="replace",
                description="Ephemeral storage limit for the first container",
            ),
        }
