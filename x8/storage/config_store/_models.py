from x8.core import DataModel

from ._constants import DEFAULT_LABEL


class ConfigKey(DataModel):
    """Config key."""

    id: str
    """Config id.
    """

    label: str | None = DEFAULT_LABEL
    """Config label.
    """


class ConfigProperties(DataModel):
    """Config properties."""

    updated_time: float | None = None
    """Config updated time.
    """


class ConfigItem(DataModel):
    """Config item."""

    key: ConfigKey
    """Config key.
    """

    value: str | None = None
    """Config value.
    """

    metadata: dict | None = None
    """Config metadata.
    """

    properties: ConfigProperties | None = None
    """Config properties.
    """


class ConfigList(DataModel):
    """Config list."""

    items: list[ConfigItem]
    """List of config items.
    """
