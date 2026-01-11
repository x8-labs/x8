from x8.core import DataModel


class ContainerRegistryItem(DataModel):
    """Container registry item.

    Attributes:
        image_name: Image name.
        image_uri: Image path.
    """

    image_name: str
    image_uri: str


class ContainerRegistryItemDigest(DataModel):
    """Container registry item digest.

    Attributes:
        image_uri: Image path.
        digest: Image digest.
        image_size_bytes: Image size in bytes.
        upload_time: Upload time in seconds since epoch.
        tags: Tags.
    """

    image_uri: str
    digest: str
    image_size_bytes: int | None = None
    upload_time: float | None = None
    tags: list[str] | None = None
