from x8.compute._common._image_helper import aget_images, get_images
from x8.compute._common._models import ImageMap
from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import RunContext
from x8.core.exceptions import BadRequestError

from ._models import Container, EnvVar, ServiceDefinition, ServiceOverlay


def map_images(
    containers: list[Container],
    images: list[ImageMap],
    containerizer: Containerizer | None = None,
    container_registry: ContainerRegistry | None = None,
    run_context: RunContext = RunContext(),
) -> list[str]:
    image_uris = get_images(
        images=images,
        containerizer=containerizer,
        container_registry=container_registry,
        run_context=run_context,
    )
    container_images = []
    for container in containers:
        found = False
        for i, image in enumerate(images):
            if image.name == container.name:
                container_images.append(image_uris[i])
                found = True
                break
        if not found:
            if container.image:
                container_images.append(container.image)
            else:
                raise BadRequestError(
                    (
                        f"Container image not found for container: "
                        f"{container.name}"
                    )
                )
    return container_images


async def amap_images(
    containers: list[Container],
    images: list[ImageMap],
    containerizer: Containerizer | None = None,
    container_registry: ContainerRegistry | None = None,
    run_context: RunContext = RunContext(),
) -> list[str]:
    image_uris = await aget_images(
        images=images,
        containerizer=containerizer,
        container_registry=container_registry,
        run_context=run_context,
    )
    container_images = []
    for container in containers:
        found = False
        for i, image in enumerate(images):
            if image.name == container.name:
                container_images.append(image_uris[i])
                found = True
                break
        if not found:
            if container.image:
                container_images.append(container.image)
            else:
                raise BadRequestError(
                    (
                        f"Container image not found for container: "
                        f"{container.name}"
                    )
                )
    return container_images


def get_env(base_env: list[EnvVar], overlay: list[EnvVar]) -> list[EnvVar]:
    env_dict = {item.name: item.value for item in base_env}
    for item in overlay:
        env_dict[item.name] = item.value
    return [EnvVar(name=k, value=v) for k, v in env_dict.items()]


def merge_service_overlay(
    service: ServiceDefinition,
    overlay: ServiceOverlay | None,
) -> ServiceDefinition:
    if overlay is None:
        return service
    if overlay.containers:
        for container_override in overlay.containers:
            for container in service.containers:
                if container.name == container_override.name:
                    container.env = get_env(
                        base_env=container.env,
                        overlay=container_override.env,
                    )
    return service


def requires_container_registry(
    service: ServiceDefinition,
) -> bool:
    for container in service.containers:
        if not container.image:
            return True
    return False
