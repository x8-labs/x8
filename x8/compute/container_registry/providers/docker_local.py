"""
Docker local for container registry.
"""

__all__ = ["DockerLocal"]

from datetime import datetime

import docker
from x8.core import Provider, Response
from x8.core.exceptions import BadRequestError

from .._models import ContainerRegistryItem, ContainerRegistryItemDigest


class DockerLocal(Provider):
    def __init__(
        self,
        **kwargs,
    ):
        """Initialize."""
        super().__init__(**kwargs)

    def push(self, image_name: str) -> Response[ContainerRegistryItem]:
        client = docker.from_env()
        image = client.images.get(image_name)
        client.images.push(image_name)
        result = ContainerRegistryItem(
            image_name=image_name, image_uri=image.tags[0]
        )
        return Response(result=result)

    def pull(
        self,
        image_name: str,
        tag: str | None = None,
    ) -> Response[ContainerRegistryItem]:
        client = docker.from_env()
        if tag is None:
            image = client.images.get(image_name)
        else:
            image = client.images.get(f"{image_name}:{tag}")
        result = ContainerRegistryItem(
            image_name=image_name,
            image_uri=image.tags[0],
        )
        return Response(result=result)

    def tag(
        self,
        image_name: str,
        tag: str,
        digest: str | None = None,
    ) -> Response[ContainerRegistryItemDigest]:
        image = self._get_image(image_name, digest)
        image.tag(image_name, tag=tag)
        result = self._get_digest(image, image.tags[0])
        return Response(result=result)

    def delete(
        self,
        image_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ) -> Response[None]:
        client = docker.from_env()

        if digest:
            image_ref = f"{image_name}@{digest}"
            client.images.remove(image=image_ref, force=True)
        elif tag:
            image_ref = f"{image_name}:{tag}"
            client.images.remove(image=image_ref, force=True)
        else:
            images = client.images.list(name=image_name)
            for image in images:
                if any(tag.startswith(image_name) for tag in image.tags):
                    client.images.remove(image.id, force=True)

        return Response(result=None)

    def list_images(
        self,
    ) -> Response[list[ContainerRegistryItem]]:
        client = docker.from_env()
        images = client.images.list()
        image_items = []
        for image in images:
            if image.tags:
                image_items.append(
                    ContainerRegistryItem(
                        image_name=image.tags[0].split(":")[0],
                        image_uri=image.tags[0],
                    )
                )
        result = image_items
        return Response(result=result)

    def get_digests(
        self,
        image_name: str,
    ) -> Response[list[ContainerRegistryItemDigest]]:
        client = docker.from_env()
        images = client.images.list(name=image_name)

        if not images:
            return Response(result=[])

        digests = []
        for image in images:
            for tag in image.tags:
                if tag.startswith(image_name):
                    digests.append(self._get_digest(image, tag=tag))

        return Response(result=digests)

    def _get_image(
        self,
        image_name: str,
        digest: str | None = None,
        tag: str | None = None,
    ):
        client = docker.from_env()
        if tag is not None:
            image_ref = f"{image_name}:{tag}"
        else:
            image_ref = image_name
        if digest is None:
            image = client.images.get(image_ref)
        else:
            matching_images = [
                img
                for img in client.images.list()
                if f"{image_name}@{digest}"
                in (img.attrs.get("RepoDigests") or [])
            ]
            if not matching_images:
                raise BadRequestError(f"No image found with digest {digest}")
            image = matching_images[0]
        return image

    def _get_digest(self, image, tag: str) -> ContainerRegistryItemDigest:
        return ContainerRegistryItemDigest(
            image_uri=tag,
            digest=image.id,
            image_size_bytes=image.attrs["Size"],
            tags=[tag.split(":")[1] for tag in image.tags],
            upload_time=datetime.fromisoformat(
                image.attrs["Created"].replace("Z", "+00:00")
            ).timestamp(),
        )

    def close(self) -> Response[None]:
        return Response(result=None)
