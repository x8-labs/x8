from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import BuildConfig, Containerizer
from x8.core import RunContext
from x8.core.exceptions import BadRequestError

from ._models import ImageMap


def get_image(
    image_map: ImageMap,
    containerizer: Containerizer | None = None,
    container_registry: ContainerRegistry | None = None,
    run_context: RunContext = RunContext(),
) -> str:
    source = None
    if image_map.handle or image_map.component:
        handle = image_map.handle
        if not handle and image_map.component:
            handle = image_map.component.__handle__
        if not handle:
            raise BadRequestError(
                "Container handle or component is required to get image."
            )
        if containerizer is None:
            raise BadRequestError(
                "Containerizer is required to get image from component."
            )
        prepare_res = containerizer.prepare(
            handle=handle,
            config=image_map.prepare,
            run_context=run_context,
        )
        source = prepare_res.result.source
    elif image_map.source:
        source = image_map.source
    local_image = None
    if source:
        if containerizer is None:
            raise BadRequestError(
                "Containerizer is required to get image from source."
            )
        build_config = image_map.build
        if not build_config:
            build_config = BuildConfig(image_name=image_map.name)
        if not build_config.image_name:
            build_config.image_name = image_map.name
        _ = containerizer.build(source=source, config=build_config)
        local_image = build_config.image_name
    elif image_map.local_image:
        local_image = image_map.local_image

    if local_image:
        if container_registry is None:
            raise BadRequestError(
                "Container registry is required to push local image."
            )
        push_res = container_registry.push(image_name=local_image)
        image_uri = push_res.result.image_uri
    else:
        raise BadRequestError(
            "Container image or local image or source is required."
        )
    return image_uri


async def aget_image(
    image_map: ImageMap,
    containerizer: Containerizer | None = None,
    container_registry: ContainerRegistry | None = None,
    run_context: RunContext = RunContext(),
) -> str:
    source = None
    if image_map.handle or image_map.component:
        handle = image_map.handle
        if not handle and image_map.component:
            handle = image_map.component.__handle__
        if not handle:
            raise BadRequestError(
                "Container handle or component is required to get image."
            )
        if containerizer is None:
            raise BadRequestError(
                "Containerizer is required to get image from component."
            )
        prepare_res = await containerizer.aprepare(
            handle=handle,
            config=image_map.prepare,
            run_context=run_context,
        )
        source = prepare_res.result.source
    elif image_map.source:
        source = image_map.source
    local_image = None
    if source:
        if containerizer is None:
            raise BadRequestError(
                "Containerizer is required to get image from source."
            )
        build_config = image_map.build
        if not build_config:
            build_config = BuildConfig(image_name=image_map.name)
        if not build_config.image_name:
            build_config.image_name = image_map.name
        _ = await containerizer.abuild(source=source, config=build_config)
        local_image = build_config.image_name
    elif image_map.local_image:
        local_image = image_map.local_image

    if local_image:
        if container_registry is None:
            raise BadRequestError(
                "Container registry is required to push image."
            )
        push_res = await container_registry.apush(image_name=local_image)
        image_uri = push_res.result.image_uri
    else:
        raise BadRequestError(
            "Container image or local image or source is required."
        )
    return image_uri


def get_images(
    images: list[ImageMap],
    containerizer: Containerizer | None = None,
    container_registry: ContainerRegistry | None = None,
    run_context: RunContext = RunContext(),
) -> list[str]:
    result: list[str] = []
    for image_map in images:
        res = get_image(
            image_map=image_map,
            containerizer=containerizer,
            container_registry=container_registry,
            run_context=run_context,
        )
        result.append(res)
    return result


async def aget_images(
    images: list[ImageMap],
    containerizer: Containerizer | None = None,
    container_registry: ContainerRegistry | None = None,
    run_context: RunContext = RunContext(),
) -> list[str]:
    result: list[str] = []
    for image_map in images:
        res = await aget_image(
            image_map=image_map,
            containerizer=containerizer,
            container_registry=container_registry,
            run_context=run_context,
        )
        result.append(res)
    return result
