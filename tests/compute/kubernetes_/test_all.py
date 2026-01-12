import pytest

from x8.core.exceptions import NotFoundError


def test_azure_provider():
    from x8.compute.kubernetes.providers.azure_kubernetes_service import (  # noqa
        AzureKubernetesService,
    )

    rg = "test-resource-group"
    name = "rpdtest2"
    location = "westus2"
    client = AzureKubernetesService()

    with pytest.raises(NotFoundError):
        client.get_resource(
            resource_group=rg,
            name=name,
        )

    res = client.create_resource(
        resource_group=rg,
        name=name,
        location=location,
    )
    result = res.result
    assert result is not None
    assert result.id is not None
    assert result.name == name
    assert result.location == location
    assert result.fqdn is not None

    res = client.get_resource(
        resource_group=rg,
        name=name,
    )
    result = res.result
    assert result is not None
    assert result.id is not None
    assert result.name == name
    assert result.location == location
    assert result.fqdn is not None

    client.delete_resource(
        resource_group=rg,
        name=name,
        delete_empty_resource_group=True,
    )
    with pytest.raises(NotFoundError):
        client.get_resource(
            resource_group=rg,
            name=name,
        )
