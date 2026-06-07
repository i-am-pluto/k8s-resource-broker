from __future__ import annotations

from pathlib import Path
from typing import Any

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from structlog import get_logger

from resource_broker.config import settings

logger = get_logger(__name__)


def _load_config() -> None:
    if settings.k8s_in_cluster:
        k8s_config.load_incluster_config()
        logger.debug("loaded in-cluster k8s config")
    else:
        cfg = Path(settings.k8s_config_file) if settings.k8s_config_file else None
        k8s_config.load_kube_config(config_file=str(cfg) if cfg else None)
        logger.debug("loaded kubeconfig", path=str(cfg or "default"))


def create_k8s_api(api_type: type[Any]) -> Any:
    """Create and return a Kubernetes API client of the given type."""
    _load_config()
    return api_type()


def create_k8s_client() -> k8s_client.CoreV1Api:
    """Shorthand to create CoreV1Api."""
    return create_k8s_api(k8s_client.CoreV1Api)


def create_admission_client() -> k8s_client.AdmissionregistrationV1Api:
    """Create Admissionregistration API client."""
    return create_k8s_api(k8s_client.AdmissionregistrationV1Api)


def create_custom_objects_client() -> k8s_client.CustomObjectsApi:
    """Create CustomObjectsApi for CRD access."""
    return create_k8s_api(k8s_client.CustomObjectsApi)
