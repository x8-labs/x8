from x8.core import Component, Response, operation

from ._models import ContainerRegistryItem, ContainerRegistryItemDigest


class ContainerRegistry(Component):
    def __init__(self, **kwargs):
        """Initialize.

        Args:
            image_name:
                Name of the image to push.
        """
        super().__init__(**kwargs)

    @operation()
    def push(self, image_name: str) -> Response[ContainerRegistryItem]:
        """Push image to registry.

        Args:
            image_name: Image name to push.

        Returns:
            Registry item.
        """
        raise NotImplementedError

    @operation()
    def pull(
        self,
        image_name: str,
        tag: str | None = None,
    ) -> Response[ContainerRegistryItem]:
        """Pull image from registry.

        Args:
            image_name: Image name to pull.
            tag: Tag to pull. If None, pull latest digest.

        Returns:
            Registry item.
        """
        raise NotImplementedError

    @operation()
    def tag(
        self,
        image_name: str,
        tag: str,
        digest: str | None = None,
    ) -> Response[ContainerRegistryItemDigest]:
        """Tag an image with a new tag.

        Args:
            image_name: Image name to tag.
            tag: New tag to assign.
            digest: Digest of the image. If None, use latest digests.

        Returns:
            Updated image.
        """
        raise NotImplementedError

    @operation()
    def delete(
        self,
        image_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ) -> Response[None]:
        """Delete image from registry.

        Args:
            image_name: Image name to delete.
            digest: Digest to delete. If None, delete all digests.
            tag: Tag to delete. If None, delete all tags.
        """
        raise NotImplementedError

    @operation()
    def list_images(
        self,
    ) -> Response[list[ContainerRegistryItem]]:
        """List images in registry.

        Returns:
            List of registry items.
        """
        raise NotImplementedError

    @operation()
    def get_digests(
        self,
        image_name: str,
    ) -> Response[list[ContainerRegistryItemDigest]]:
        """Get digests of image in registry.

        Args:
            image_name: Image name to get digests.

        Returns:
            List of digests.
        """
        raise NotImplementedError

    @operation()
    def close(self) -> Response[None]:
        """Close the container registry client."""
        raise NotImplementedError

    @operation()
    async def apush(self, image_name: str) -> Response[ContainerRegistryItem]:
        """Push image to registry.

        Args:
            image_name: Image name to push.

        Returns:
            Registry item.
        """
        raise NotImplementedError

    @operation()
    async def apull(
        self,
        image_name: str,
        tag: str | None = None,
    ) -> Response[ContainerRegistryItem]:
        """Pull image from registry.

        Args:
            image_name: Image name to pull.
            tag: Tag to pull. If None, pull latest digest.

        Returns:
            Registry item.
        """
        raise NotImplementedError

    @operation()
    async def atag(
        self,
        image_name: str,
        tag: str,
        digest: str | None = None,
    ) -> Response[ContainerRegistryItemDigest]:
        """Tag an image with a new tag.

        Args:
            image_name: Image name to tag.
            tag: New tag to assign.
            digest: Digest of the image. If None, use latest digests.

        Returns:
            Updated image.
        """
        raise NotImplementedError

    @operation()
    async def adelete(
        self,
        image_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ) -> Response[None]:
        """Delete image from registry.

        Args:
            image_name: Image name to delete.
            digest: Digest to delete. If None, delete all digests.
            tag: Tag to delete. If None, delete all tags.
        """
        raise NotImplementedError

    @operation()
    async def alist_images(
        self,
    ) -> Response[list[ContainerRegistryItem]]:
        """List images in registry.

        Returns:
            List of registry items.
        """
        raise NotImplementedError

    @operation()
    async def aget_digests(
        self,
        image_name: str,
    ) -> Response[list[ContainerRegistryItemDigest]]:
        """Get digests of image in registry.

        Args:
            image_name: Image name to get digests.

        Returns:
            List of digests.
        """
        raise NotImplementedError

    @operation()
    async def aclose(self) -> Response[None]:
        """Close the container registry client."""
        raise NotImplementedError
