from x8.core import Component, Response, RunContext, operation

from ._models import (
    BuildConfig,
    ContainerItem,
    ImageItem,
    PrepareConfig,
    RunConfig,
    SourceItem,
)


class Containerizer(Component):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

    @operation()
    def prepare(
        self,
        handle: str,
        config: PrepareConfig = PrepareConfig(),
        run_context: RunContext = RunContext(),
        **kwargs,
    ) -> Response[SourceItem]:
        """Prepare the container.

        Args:
            handle:
                Component handle to build and run.
            config:
                Configuration for the preparation of the source folder.
            run_context:
                Context for the run.

        Returns:
            Source item.
        """
        ...

    @operation()
    def build(
        self,
        source: str,
        config: BuildConfig = BuildConfig(),
        **kwargs,
    ) -> Response[ImageItem]:
        """Build the container.

        Args:
            source:
                Source folder from which container is built.
                Assumes the folder has a Dockerfile.
            config:
                Configuration for the build process.

        Returns:
            Image item.
        """
        ...

    @operation()
    def run(
        self,
        image_name: str,
        config: RunConfig = RunConfig(),
        **kwargs,
    ) -> Response[ContainerItem]:
        """Run the container.

        Args:
            image_name:
                Name of the built image.
            config:
                Configuration for running the container.

        Returns:
            Container item.
        """
        ...

    @operation()
    def stop(
        self,
        container_id: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Stop the container.

        Args:
            container_id:
                ID of the container to stop.
        """
        ...

    @operation()
    def remove(
        self,
        container_id: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Remove the container.

        Args:
            container_id:
                ID of the container to remove.
        """
        ...

    @operation()
    def delete(
        self,
        image_name: str | None = None,
        digest: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Delete the container.

        Args:
            image_name:
                Name of the built image.
            digest:
                Digest of the image to delete.
        """
        ...

    @operation()
    def tag(
        self,
        image_name: str | None = None,
        tag: str | None = None,
        digest: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Tag the container.

        Args:
            image_name:
                Name of the built image.
            tag:
                Tag to assign to the image.
            digest:
                Digest of the image to tag.
        """
        ...

    @operation()
    def push(
        self,
        image_name: str | None = None,
        repository_name: str | None = None,
        digest: str | None = None,
        tag: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Push the container to the registry.

        Args:
            image_name:
                Name of the built image.
            repository_name:
                Name of the repository to push to.
            digest:
                Digest of the image to push.
            tag:
                Tag to assign to the image.
        """
        ...

    @operation()
    def pull(
        self,
        image_name: str | None = None,
        tag: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Pull the container from the registry.

        Args:
            image_name:
                Name of the built image.
            tag:
                Tag to assign to the image.
        """
        ...

    @operation()
    def list_containers(
        self,
        **kwargs,
    ) -> Response[list[ContainerItem]]:
        """List the containers.

        Returns:
            List of containers.
        """
        ...

    @operation()
    def list_images(
        self,
        **kwargs,
    ) -> Response[list[ImageItem]]:
        """List the images.

        Returns:
            List of images.
        """
        ...

    @operation()
    async def aprepare(
        self,
        handle: str,
        config: PrepareConfig = PrepareConfig(),
        run_context: RunContext = RunContext(),
        **kwargs,
    ) -> Response[SourceItem]:
        """Prepare the container.

        Args:
            handle:
                Component handle to build and run.
            config:
                Configuration for the preparation of the source folder.
            run_context:
                Context for the run.

        Returns:
            Source item.
        """
        ...

    @operation()
    async def abuild(
        self,
        source: str,
        config: BuildConfig = BuildConfig(),
        **kwargs,
    ) -> Response[ImageItem]:
        """Build the container.

        Args:
            source:
                Source folder from which container is built.
                Assumes the folder has a Dockerfile.
            config:
                Configuration for the build process.

        Returns:
            Image item.
        """
        ...

    @operation()
    async def arun(
        self,
        image_name: str,
        config: RunConfig = RunConfig(),
        **kwargs,
    ) -> Response[ContainerItem]:
        """Run the container.

        Args:
            image_name:
                Name of the built image.
            config:
                Configuration for running the container.

        Returns:
            Container item.
        """
        ...

    @operation()
    async def astop(
        self,
        container_id: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Stop the container.

        Args:
            container_id:
                ID of the container to stop.
        """
        ...

    @operation()
    async def aremove(
        self,
        container_id: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Remove the container.

        Args:
            container_id:
                ID of the container to remove.
        """
        ...

    @operation()
    async def adelete(
        self,
        image_name: str | None = None,
        digest: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Delete the container.

        Args:
            image_name:
                Name of the built image.
            digest:
                Digest of the image to delete.
        """
        ...

    @operation()
    async def atag(
        self,
        image_name: str | None = None,
        tag: str | None = None,
        digest: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Tag the container.

        Args:
            image_name:
                Name of the built image.
            tag:
                Tag to assign to the image.
            digest:
                Digest of the image to tag.
        """
        ...

    @operation()
    async def apush(
        self,
        image_name: str | None = None,
        digest: str | None = None,
        tag: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Push the container to the registry.

        Args:
            image_name:
                Name of the built image.
            digest:
                Digest of the image to push.
            tag:
                Tag to assign to the image.
        """
        ...

    @operation()
    async def apull(
        self,
        image_name: str | None = None,
        tag: str | None = None,
        **kwargs,
    ) -> Response[None]:
        """Pull the container from the registry.

        Args:
            image_name:
                Name of the built image.
            tag:
                Tag to assign to the image.
        """
        ...

    @operation()
    async def alist_containers(
        self,
        **kwargs,
    ) -> Response[list[ContainerItem]]:
        """List the containers.

        Returns:
            List of containers.
        """
        ...

    @operation()
    async def alist_images(
        self,
        **kwargs,
    ) -> Response[list[ImageItem]]:
        """List the images.

        Returns:
            List of images.
        """
        ...
