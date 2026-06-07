from __future__ import annotations

from resource_broker.common.models.profile import FieldEntry, ResourceProfile


def test_profile_from_crd() -> None:
    crd = {
        "metadata": {"name": "test-profile", "namespace": "default"},
        "spec": {
            "resource-type": "k8s-pod",
            "mode": "recommendation",
            "strategy": {"algo": "percentile", "transform": "p75"},
            "fields": {
                "cpu_request": {},
                "memory_request": {"strategy": {"algo": "static", "value": "512Mi"}},
            },
        },
    }
    profile = ResourceProfile.from_crd(crd)

    assert profile.name == "test-profile"
    assert profile.namespace == "default"
    assert profile.resource_type == "k8s-pod"
    assert profile.mode == "recommendation"
    assert profile.strategy == {"algo": "percentile", "transform": "p75"}
    assert len(profile.fields) == 2
    assert profile.fields["cpu_request"].strategy is None
    assert profile.fields["memory_request"].strategy == {"algo": "static", "value": "512Mi"}


def test_profile_is_enforce_mode() -> None:
    profile = ResourceProfile(
        name="test", namespace="default", resource_type="k8s-pod", mode="enforce"
    )
    assert profile.is_enforce_mode() is True

    profile.mode = "recommendation"
    assert profile.is_enforce_mode() is False


def test_profile_to_dict() -> None:
    profile = ResourceProfile(
        name="test",
        namespace="default",
        resource_type="k8s-pod",
        fields={
            "cpu_request": FieldEntry(strategy={"algo": "static", "value": "250m"}),
        },
    )
    d = profile.to_dict()
    assert d["name"] == "test"
    assert d["resource_type"] == "k8s-pod"
    assert d["mode"] == "recommendation"
    assert "cpu_request" in d["fields"]
