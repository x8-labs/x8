from typing import Any

from x8.core import Response, operation
from x8.ql import Expression
from x8.storage._common import StoreComponent

from ._models import ConfigItem, ConfigKey, ConfigList


class ConfigStore(StoreComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @operation()
    def get(
        self,
        key: str | dict | ConfigKey,
        **kwargs: Any,
    ) -> Response[ConfigItem]:
        """Get config value.

        Args:
            key: Config key.

        Returns:
            Config item with value.

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    def put(
        self,
        key: str | dict | ConfigKey,
        value: str,
        **kwargs: Any,
    ) -> Response[ConfigItem]:
        """Put config value.

        Args:
            key: Config key.
            value: Config value.

        Returns:
            Config item.
        """
        ...

    @operation()
    def delete(
        self,
        key: str | dict | ConfigKey,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete config.

        Args:
            key: Config key.

        Returns:
           None

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    def query(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[ConfigList]:
        """Query config.

        Args:
            where: Condition expression, defaults to None.

        Returns:
            List of config items.
        """
        ...

    @operation()
    def count(
        self,
        where: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count config.

        Args:
            where: Condition expression, defaults to None.

        Returns:
            Count of config items.
        """
        ...

    @operation()
    def close(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close the client.

        Returns:
            None.
        """
        ...

    @operation()
    async def aget(
        self,
        key: str | dict | ConfigKey,
        **kwargs: Any,
    ) -> Response[ConfigItem]:
        """Get config value.

        Args:
            key: Config key.

        Returns:
            Config item with value.

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    async def aput(
        self,
        key: str | dict | ConfigKey,
        value: str,
        **kwargs: Any,
    ) -> Response[ConfigItem]:
        """Put config value.

        Args:
            key: Config key.
            value: Config value.

        Returns:
            Config item.
        """
        ...

    @operation()
    async def adelete(
        self,
        key: str | dict | ConfigKey,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete config.

        Args:
            key: Config key.

        Returns:
           None

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    async def aquery(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[ConfigList]:
        """Query config.

        Args:
            where: Condition expression, defaults to None.

        Returns:
            List of config items.
        """
        ...

    @operation()
    async def acount(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count config.

        Args:
            where: Condition expression, defaults to None.

        Returns:
            Count of config items.
        """
        ...

    @operation()
    async def aclose(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close the async client.

        Returns:
            None.
        """
        ...
