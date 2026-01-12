from x8.compute.container_registry import ContainerRegistry
from x8.compute.container_registry.providers.azure_container_registry import (  # noqa
    AzureContainerRegistry,
)
from x8.compute.container_registry.providers.docker_local import DockerLocal
from x8.compute.container_registry.providers.google_artifact_registry import (  # noqa
    GoogleArtifactRegistry,
)
from x8.compute.containerizer import BuildConfig, Containerizer
from x8.core import RunContext


def run():
    image_name = "api"
    component = Containerizer(__provider__="default")
    result = component.prepare(
        handle="api", run_context=RunContext(path="../samples/inline")
    )
    print(result)
    result = component.build(
        source=result.source,
        config=BuildConfig(image_name=image_name, nocache=False),
    )

    provider1 = GoogleArtifactRegistry(
        region="us-west1",
        name="test-registry",
    )
    provider2 = DockerLocal()
    provider3 = AzureContainerRegistry(
        name="rpdtest1",
    )
    component = ContainerRegistry(__provider__=provider3)
    component = ContainerRegistry(__provider__=provider2)
    component = ContainerRegistry(__provider__=provider1)
    result = component.push(image_name=image_name)
    print(result)
    result = component.list_images()
    print(result)
    result = component.get_digests(image_name=image_name)
    for item in result:
        print(item.digest, item.tags)
    print(
        component.tag(
            image_name=image_name,
            tag="v3.3",
            # digest=result[0].digest,
        )
    )
    result = component.get_digests(image_name=image_name)
    for item in result:
        print(item.digest, item.tags)

    result = component.pull(
        image_name=image_name,
        tag="v3.3",
    )
    component.delete(
        image_name=image_name,
        # digest=result[0].digest,
        tag="v3.3",
    )
    result = component.get_digests(image_name=image_name)
    for item in result:
        print(item.digest, item.tags)
    component.delete(image_name=image_name)
    print(component.list_images())


if __name__ == "__main__":
    run()
