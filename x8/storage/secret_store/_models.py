from x8.core import DataModel


class SecretKey(DataModel):
    """Secret key."""

    id: str
    """Secret id."""

    version: str | None = None
    """Secret version."""


class SecretProperties(DataModel):
    """Secret properties."""

    created_time: float | None = None
    """Created time."""


class SecretVersion(DataModel):
    """Secret version."""

    version: str
    "Version id."

    created_time: float | None = None
    """Created time."""


class SecretItem(DataModel):
    """Secret item."""

    key: SecretKey
    """Secret key."""

    value: str | None = None
    """Secret value."""

    metadata: dict | None = None
    """Secret metadata."""

    properties: SecretProperties | None = None
    """Secret properties."""

    versions: list[SecretVersion] | None = None
    """Secret versions."""


class SecretList(DataModel):
    """Secret list."""

    items: list[SecretItem]
    """List of secret items."""
