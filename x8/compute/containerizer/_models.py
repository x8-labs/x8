from x8.core import DataModel

from ._config import DEFAULT_BASE_IMAGE, DEFAULT_PLATFORM


class PrepareConfig(DataModel):
    """Prepare config.

    Args:
        base_image:
            Base image for the container.
        expose:
            Ports to expose from the container.
        requirements:
            Path to requirements files to pip install.
        prepare_in_place:
            A value indicating whether the preparation of the source
            folder and build should happen in place.
    """

    base_image: str = DEFAULT_BASE_IMAGE
    expose: int | list[int] | None = None
    requirements: str | list[str] | None = None
    prepare_in_place: bool = False


class BuildConfig(DataModel):
    """Build config.

    Args:
        image_name:
            Name of the built image.
        platform:
            The platform architecture used to build the container.
        nocache:
            A value indicating whether the container should be built
            in no-cache mode.
    """

    image_name: str | None = None
    platform: str = DEFAULT_PLATFORM
    nocache: bool = False


class RunConfig(DataModel):
    """Run config.

    Args:
        detach:
            A value indicating whether the container should be
            run in detached mode.
        remove:
            A value indicating whether the container should be
            removed when stopped.
        ports:
            Port mapping when running the container.
        env:
            Environment variables to set in the container.
    """

    detach: bool = True
    remove: bool = True
    ports: dict[str, int] | None = None
    env: dict[str, str] | None = None


class SourceItem(DataModel):
    source: str


class ImageItem(DataModel):
    name: str
    digest: str | None = None
    tags: list[str] = []
    error: str | None = None


class ContainerItem(DataModel):
    id: str
    name: str | None = None
    image: ImageItem | None = None
