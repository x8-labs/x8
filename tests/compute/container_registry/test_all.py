import os
from typing import Any

import pytest

from x8.compute.containerizer import BuildConfig, Containerizer
from x8.core import RunContext
from x8.core.exceptions import NotFoundError

from ._providers import ContainerRegistryProvider
from ._sync_and_async_client import ContainerRegistryClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ContainerRegistryProvider.AMAZON_ELASTIC_CONTAINER_REGISTRY,
        ContainerRegistryProvider.AZURE_CONTAINER_REGISTRY,
        ContainerRegistryProvider.GOOGLE_ARTIFACT_REGISTRY,
        ContainerRegistryProvider.DOCKER_LOCAL,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_tag(provider_type: str, async_call: bool):
    client = ContainerRegistryClient(
        provider_type=provider_type, async_call=async_call
    )

    containerizer = Containerizer(__provider__="default")
    image_name = "X8test2"
    image_tag = "v1.0"
    base_path = os.path.dirname(os.path.abspath(__file__))
    service_path = os.path.join(base_path, "service")
    image_with_tag = f"{image_name}:{image_tag}"
    res: Any = containerizer.prepare(
        handle="api", run_context=RunContext(path=service_path)
    )
    res = containerizer.build(
        source=res.result.source,
        config=BuildConfig(image_name=image_with_tag, nocache=False),
    )

    res = await client.push(image_name=image_with_tag)
    result = res.result

    assert result.image_name == image_with_tag
    assert f":{image_tag}" in result.image_uri

    image_uri = result.image_uri

    res = await client.list_images()
    for item in res.result:
        if item.image_name == image_name:
            assert item.image_uri in image_uri

    res = await client.get_digests(image_name=image_name)
    all_tags = []
    for item in res.result:
        if item.tags:
            all_tags.extend(item.tags)
    assert image_tag in all_tags

    await client.delete(image_name=image_name)

    res = await client.list_images()
    assert image_name not in [item.image_name for item in res.result]

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ContainerRegistryProvider.AMAZON_ELASTIC_CONTAINER_REGISTRY,
        ContainerRegistryProvider.AZURE_CONTAINER_REGISTRY,
        ContainerRegistryProvider.GOOGLE_ARTIFACT_REGISTRY,
        ContainerRegistryProvider.DOCKER_LOCAL,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_no_tag(provider_type: str, async_call: bool):
    client = ContainerRegistryClient(
        provider_type=provider_type, async_call=async_call
    )

    containerizer = Containerizer(__provider__="default")
    image_name = "X8test1"
    base_path = os.path.dirname(os.path.abspath(__file__))
    service_path = os.path.join(base_path, "service")
    res: Any = containerizer.prepare(
        handle="api", run_context=RunContext(path=service_path)
    )
    res = containerizer.build(
        source=res.result.source,
        config=BuildConfig(image_name=image_name, nocache=False),
    )
    res = await client.push(image_name=image_name)
    result = res.result
    assert result.image_name == image_name
    assert result.image_uri is not None

    image_uri = result.image_uri

    res = await client.list_images()
    for item in res.result:
        if item.image_name == image_name:
            assert item.image_uri == image_uri

    res = await client.get_digests(image_name=image_name)
    assert len(res.result) > 0
    for item in res.result:
        assert item.image_uri is not None
        assert item.digest is not None
        assert item.image_size_bytes is not None

    new_tag = "v1.0"
    res = await client.tag(
        image_name=image_name,
        tag=new_tag,
    )

    res = await client.get_digests(image_name=image_name)
    all_tags = []
    for item in res.result:
        if item.tags:
            all_tags.extend(item.tags)
    assert new_tag in all_tags

    res = await client.pull(
        image_name=image_name,
        tag=new_tag,
    )
    await client.delete(
        image_name=image_name,
        # digest=result[0].digest,
        tag=new_tag,
    )
    res = await client.get_digests(image_name=image_name)
    all_tags = []
    for item in res.result:
        if item.tags:
            all_tags.extend(item.tags)
    assert new_tag not in all_tags

    await client.delete(image_name=image_name)
    res = await client.list_images()
    assert image_name not in [item.image_name for item in res.result]
    await client.close()


def test_azure_provider():
    from x8.compute.container_registry.providers.azure_container_registry import (  # noqa
        AzureContainerRegistry,
    )

    rg = "test-resource-group"
    name = "rpdtest2"
    location = "westus2"
    client = AzureContainerRegistry()

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
    assert result.login_server == f"{name}.azurecr.io"

    res = client.get_resource(
        resource_group=rg,
        name=name,
    )
    result = res.result
    assert result is not None
    assert result.id is not None
    assert result.name == name
    assert result.location == location
    assert result.login_server == f"{name}.azurecr.io"

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


def test_google_provider():
    from x8.compute.container_registry.providers.google_artifact_registry import (  # noqa
        GoogleArtifactRegistry,
    )

    project = None  # Use defult project
    location = "us-central1"
    name = "rpdtest2"
    client = GoogleArtifactRegistry()

    with pytest.raises(NotFoundError):
        client.get_resource(
            project=project,
            location=location,
            name=name,
        )

    res = client.create_resource(
        project=project,
        location=location,
        name=name,
        description="Test repository",
        format="DOCKER",
        mode="STANDARD",
    )
    result = res.result
    assert result is not None
    assert result.name is not None
    assert result.location == location
    assert result.login_server.startswith(f"{location}-docker.pkg.dev")

    res = client.get_resource(
        project=project,
        location=location,
        name=name,
    )
    result = res.result
    assert result is not None
    assert result.name == name
    assert result.location == location
    assert result.login_server.startswith(f"{location}-docker.pkg.dev")

    client.delete_resource(
        project=project,
        location=location,
        name=name,
    )
    with pytest.raises(NotFoundError):
        client.get_resource(
            project=project,
            location=location,
            name=name,
        )
