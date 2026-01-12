import os
import time

import httpx
import pytest

from x8.compute.container_deployment import (
    Container,
    ContainerDeploymentFeature,
    EnvVar,
    ImageMap,
    Port,
    Revision,
    ServiceDefinition,
    ServiceItem,
    TrafficAllocation,
)
from x8.compute.containerizer import BuildConfig
from x8.core import Loader, RunContext
from x8.core.exceptions import NotFoundError, PreconditionFailedError

from ._providers import ContainerDeploymentProvider
from ._sync_and_async_client import ContainerDeploymentClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ContainerDeploymentProvider.AWS_APP_RUNNER,
        ContainerDeploymentProvider.AMAZON_ECS_EC2,
        ContainerDeploymentProvider.AMAZON_ECS_FARGATE,
        ContainerDeploymentProvider.AZURE_CONTAINER_INSTANCES,
        ContainerDeploymentProvider.AZURE_CONTAINER_APPS,
        ContainerDeploymentProvider.GOOGLE_CLOUD_RUN,
        ContainerDeploymentProvider.LOCAL,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_single_container(provider_type: str, async_call: bool):
    client = ContainerDeploymentClient(
        provider_type=provider_type, async_call=async_call
    )
    service_name = "X8test2"
    try:
        await client.delete_service(name=service_name)
        wait_to_delete(provider_type=provider_type)
    except NotFoundError:
        pass

    with pytest.raises(NotFoundError):
        await client.get_service(name=service_name)

    base_path = os.path.dirname(os.path.abspath(__file__))
    service_path = os.path.join(base_path, "service")
    loader = Loader(path=service_path)
    api_main = loader.load_component("api_single_main")
    service_def = get_single_container_service_definition(
        service_name, api_main, "v1"
    )
    res = await client.create_service(
        service=service_def,
        run_context=RunContext(path=service_path),
    )
    response = httpx.post(
        url=f"{res.result.uri}/add",
        json={"a": "5", "b": "10"},
    )
    assert response.json() == 15
    response = httpx.post(
        url=f"{res.result.uri}/version",
    )
    assert response.json() == "v1"

    res = await client.get_service(name=service_name)
    assert_service_definition(res.result, service_def)
    revision_name = res.result.service.latest_created_revision
    assert_traffic(
        res.result.service.traffic,
        [
            TrafficAllocation(
                revision=revision_name,
                percent=100,
            )
        ],
    )

    res = await client.list_services()
    assert service_name in [s.name for s in res.result]

    res = await client.list_revisions(name=service_name)
    assert_revisions(res.result, [Revision(name=revision_name, traffic=100)])

    res = await client.get_revision(
        name=service_name,
        revision=res.result[0].name,
    )
    assert_revision(res.result, service_def)

    with pytest.raises(PreconditionFailedError):
        await client.delete_revision(
            name=service_name,
            revision=res.result.name,
        )

    service_def_2 = get_single_container_service_definition(
        service_name, api_main, "v2"
    )
    with pytest.raises(PreconditionFailedError):
        res = await client.create_service(
            service=service_def_2,
            run_context=RunContext(path=service_path),
            where="not_exists()",
        )

    res = await client.create_service(
        service=service_def_2,
        run_context=RunContext(path=service_path),
    )
    response = httpx.post(
        url=f"{res.result.uri}/subtract",
        json={"a": "5", "b": "10"},
    )
    assert response.json() == -5
    response = httpx.post(
        url=f"{res.result.uri}/version",
    )
    assert response.json() == "v2"

    res = await client.get_service(name=service_name)
    assert_service_definition(res.result, service_def_2)

    if client.client.__supports__(
        ContainerDeploymentFeature.MULTIPLE_REVISIONS
    ):
        res = await client.list_revisions(name=service_name)
        revisions = res.result
        current_revision = res.result[0].name
        old_revision = res.result[1].name
        for rev in res.result:
            if rev.traffic == 100:
                current_revision = rev.name
            else:
                old_revision = rev.name
        assert_revisions(
            res.result,
            [
                Revision(name=current_revision, traffic=100),
                Revision(name=old_revision, traffic=0),
            ],
        )

        res = await client.get_revision(
            name=service_name,
            revision=current_revision,
        )
        assert_revision(res.result, service_def_2)

        traffic = []
        for rev in revisions:
            if rev.traffic == 100:
                traffic.append(TrafficAllocation(revision=rev.name, percent=0))
            else:
                traffic.append(
                    TrafficAllocation(revision=rev.name, percent=100)
                )
        res = await client.update_traffic(
            name=service_name,
            traffic=traffic,
        )
        response = httpx.post(
            url=f"{res.result.uri}/do",
            json={"a": "5"},
        )
        assert response.json() == 15

        if client.client.__supports__(
            ContainerDeploymentFeature.REVISION_DELETE
        ):
            # Go back to the old traffic allocation
            # We cannot delete revisions that have traffic
            # allocated or is the latest revision
            traffic = []
            for rev in revisions:
                if rev.traffic == 0:
                    traffic.append(
                        TrafficAllocation(revision=rev.name, percent=0)
                    )
                else:
                    traffic.append(
                        TrafficAllocation(revision=rev.name, percent=100)
                    )

            res = await client.update_traffic(
                name=service_name,
                traffic=traffic,
            )
            await client.delete_revision(
                name=service_name,
                revision=old_revision,
            )

            res = await client.list_revisions(name=service_name)
            assert_revisions(
                res.result,
                [
                    Revision(name=current_revision, traffic=100),
                ],
            )

    await client.delete_service(name=service_name)
    wait_to_delete(provider_type=provider_type)

    with pytest.raises(NotFoundError):
        await client.get_service(name=service_name)

    # container_registry = client.client.__provider__.container_registry
    # for container in service_def.containers:
    #    image_name = container.name
    #    container_registry.delete(image_name=image_name)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ContainerDeploymentProvider.AMAZON_ECS_FARGATE,
        ContainerDeploymentProvider.AMAZON_ECS_EC2,
        ContainerDeploymentProvider.AZURE_CONTAINER_INSTANCES,
        ContainerDeploymentProvider.AZURE_CONTAINER_APPS,
        ContainerDeploymentProvider.GOOGLE_CLOUD_RUN,
        ContainerDeploymentProvider.LOCAL,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_multiple_containers(provider_type: str, async_call: bool):
    client = ContainerDeploymentClient(
        provider_type=provider_type, async_call=async_call
    )
    service_name = f"X8test1-{str(async_call).lower()}"
    try:
        await client.delete_service(name=service_name)
        wait_to_delete(provider_type=provider_type)
    except NotFoundError:
        pass

    with pytest.raises(NotFoundError):
        await client.get_service(name=service_name)

    base_path = os.path.dirname(os.path.abspath(__file__))
    service_path = os.path.join(base_path, "service")
    loader = Loader(path=service_path)
    api_main = loader.load_component("api_main")
    api_side = loader.load_component("api_side")
    service_def = get_multi_container_service_definition(
        service_name, api_main, api_side, "v1"
    )
    res = await client.create_service(
        service=service_def,
        run_context=RunContext(path=service_path, tag="add"),
    )
    response = httpx.post(
        url=f"{res.result.uri}/do",
        json={"a": "5"},
    )
    assert response.json() == 15

    res = await client.get_service(name=service_name)
    assert_service_definition(res.result, service_def)
    revision_name = res.result.service.latest_created_revision
    assert_traffic(
        res.result.service.traffic,
        [
            TrafficAllocation(
                revision=revision_name,
                percent=100,
            )
        ],
    )

    res = await client.list_services()
    assert service_name in [s.name for s in res.result]

    res = await client.list_revisions(name=service_name)
    assert_revisions(res.result, [Revision(name=revision_name, traffic=100)])

    res = await client.get_revision(
        name=service_name,
        revision=res.result[0].name,
    )
    assert_revision(res.result, service_def)

    with pytest.raises(PreconditionFailedError):
        await client.delete_revision(
            name=service_name,
            revision=res.result.name,
        )

    service_def_2 = get_multi_container_service_definition(
        service_name, api_main, api_side, "v2"
    )
    with pytest.raises(PreconditionFailedError):
        res = await client.create_service(
            service=service_def_2,
            run_context=RunContext(path=service_path, tag="subtract"),
            where="not_exists()",
        )

    res = await client.create_service(
        service=service_def_2,
        run_context=RunContext(path=service_path, tag="subtract"),
    )
    time.sleep(5)
    response = httpx.post(
        url=f"{res.result.uri}/do",
        json={"a": "5"},
    )
    assert response.json() == -5

    res = await client.get_service(name=service_name)
    assert_service_definition(res.result, service_def_2)
    revision_name = res.result.service.latest_created_revision

    if client.client.__supports__(
        ContainerDeploymentFeature.MULTIPLE_REVISIONS
    ):
        res = await client.list_revisions(name=service_name)
        revisions = res.result
        current_revision = res.result[0].name
        old_revision = res.result[1].name
        for rev in res.result:
            if rev.traffic == 100:
                current_revision = rev.name
            else:
                old_revision = rev.name
        assert_revisions(
            res.result,
            [
                Revision(name=current_revision, traffic=100),
                Revision(name=old_revision, traffic=0),
            ],
        )

        res = await client.get_revision(
            name=service_name,
            revision=current_revision,
        )
        assert_revision(res.result, service_def_2)

        traffic = []
        for rev in revisions:
            if rev.traffic == 100:
                traffic.append(TrafficAllocation(revision=rev.name, percent=0))
            else:
                traffic.append(
                    TrafficAllocation(revision=rev.name, percent=100)
                )
        res = await client.update_traffic(
            name=service_name,
            traffic=traffic,
        )
        response = httpx.post(
            url=f"{res.result.uri}/do",
            json={"a": "5"},
        )
        assert response.json() == 15

        if client.client.__supports__(
            ContainerDeploymentFeature.REVISION_DELETE
        ):
            # Go back to the old traffic allocation
            # We cannot delete revisions that have traffic
            # allocated or is the latest revision
            traffic = []
            for rev in revisions:
                if rev.traffic == 0:
                    traffic.append(
                        TrafficAllocation(revision=rev.name, percent=0)
                    )
                else:
                    traffic.append(
                        TrafficAllocation(revision=rev.name, percent=100)
                    )

            res = await client.update_traffic(
                name=service_name,
                traffic=traffic,
            )
            await client.delete_revision(
                name=service_name,
                revision=old_revision,
            )

            res = await client.list_revisions(name=service_name)
            assert_revisions(
                res.result,
                [
                    Revision(name=current_revision, traffic=100),
                ],
            )
    else:
        res = await client.list_revisions(name=service_name)
        assert_revisions(
            res.result, [Revision(name=revision_name, traffic=100)]
        )

        res = await client.get_revision(
            name=service_name,
            revision=res.result[0].name,
        )

    await client.delete_service(name=service_name)
    wait_to_delete(provider_type=provider_type)

    with pytest.raises(NotFoundError):
        await client.get_service(name=service_name)

    # container_registry = client.client.__provider__.container_registry
    # for container in service_def.containers:
    #    image_name = container.name
    #    container_registry.delete(image_name=image_name)

    await client.close()


def get_single_container_service_definition(
    name, main, version
) -> ServiceDefinition:
    return ServiceDefinition(
        name=name,
        images=[
            ImageMap(
                name="main",
                component=main,
            ),
        ],
        containers=[
            Container(
                name="main",
                ports=[
                    Port(container_port=80),
                ],
                env=[
                    EnvVar(name="BACKEND_URL", value="http://localhost:8000"),
                    EnvVar(name="VERSION", value=version),
                ],
            ),
        ],
    )


def get_multi_container_service_definition(
    name, main, side, version
) -> ServiceDefinition:
    return ServiceDefinition(
        name=name,
        images=[
            ImageMap(
                name="main",
                component=main,
                build=BuildConfig(
                    image_name=f"main:{version}",
                    nocache=False,
                ),
            ),
            ImageMap(
                name="side",
                component=side,
                build=BuildConfig(
                    image_name=f"side:{version}",
                    nocache=False,
                ),
            ),
        ],
        containers=[
            Container(
                name="main",
                ports=[
                    Port(container_port=80),
                ],
                env=[
                    EnvVar(name="BACKEND_URL", value="http://localhost:8000"),
                    EnvVar(name="VERSION", value=version),
                ],
            ),
            Container(name="side"),
        ],
    )


def assert_service_definition(
    result: ServiceItem, expected: ServiceDefinition
):
    assert result.name == expected.name
    assert result.uri is not None
    assert result.service is not None
    assert result.service.name == expected.name
    assert len(result.service.containers) == len(expected.containers)
    for container in result.service.containers:
        assert_container(
            result=container,
            expected=next(
                c for c in expected.containers if c.name == container.name
            ),
        )


def assert_revision(result: Revision, expected: ServiceDefinition):
    for container in result.containers:
        assert_container(
            result=container,
            expected=next(
                c for c in expected.containers if c.name == container.name
            ),
        )


def assert_revisions(result: list[Revision], expected: list[Revision]):
    assert len(result) == len(expected)
    for item in result:
        assert item.name in [e.name for e in expected]
        assert item.traffic in [e.traffic for e in expected]


def assert_traffic(
    result: list[TrafficAllocation],
    expected: list[TrafficAllocation],
):
    assert len(result) == len(expected)
    for item in result:
        assert item.revision is not None
        assert item.percent in [e.percent for e in expected]


def assert_container(result: Container, expected: Container):
    assert result.name == expected.name
    # assert len(result.ports) == len(expected.ports)
    # assert_ports(result.ports, expected.ports)
    assert_env(result.env, expected.env)


def assert_ports(result: list[Port], expected: list[Port]):
    assert len(result) == len(expected)
    for port in result:
        assert port.container_port in [p.container_port for p in expected]


def assert_env(result: list[EnvVar], expected: list[EnvVar]):
    assert len(result) == len(expected)
    for env in result:
        assert env.name in [e.name for e in expected]
        assert env.value == next(
            e.value for e in expected if e.name == env.name
        )


def wait_to_delete(provider_type: str):
    if provider_type == ContainerDeploymentProvider.GOOGLE_CLOUD_RUN:
        time.sleep(10)
