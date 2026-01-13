from typing import Any

from x8.core import Response, operation
from x8.ql import Expression
from x8.storage._common import StoreComponent

from ._models import SecretItem, SecretKey, SecretList


class SecretStore(StoreComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @operation()
    def get(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Get secret value.

        Args:
            key: Secret key.

        Returns:
            Secret item with value.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    def get_metadata(
        self, key: str | dict | SecretKey, **kwargs: Any
    ) -> Response[SecretItem]:
        """Get secret metadata.

        Args:
            key: Secret key.

        Returns:
            Secret item with metadata.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    def get_versions(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Get secret versions.

        Args:
            key: Secret key.

        Returns:
            Secret item with versions.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    def put(
        self,
        key: str | dict | SecretKey,
        value: str,
        metadata: dict | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Put secret.

        Args:
            key: Secret key.
            value: Secret value.
            metadata: Secret metadata.
            where: Conditional expression.

        Returns:
            Secret item.

        Raises:
            PreconditionFailedError:
                Condition not satisfied.
        """
        raise NotImplementedError

    @operation()
    def update(
        self,
        key: str | dict | SecretKey,
        value: str,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Update secret value.

        Args:
            key: Secret key.
            value: Secret value.

        Returns:
            Secret item.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    def update_metadata(
        self,
        key: str | dict | SecretKey,
        metadata: dict | None,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Update secret metadata.

        Args:
            key: Secret key.
            metadata: Secret metadata.

        Returns:
            Secret item.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    def delete(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete secret.

        Args:
            key: Secret key.

        Returns:
            None.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    def query(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[SecretList]:
        """Query secrets.

        Args:
            where: Condition expression.

        Returns:
            Secret list.
        """
        raise NotImplementedError

    @operation()
    def count(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count secrets.

        Args:
            where: Condition expression.

        Returns:
            Count of secrets.
        """
        raise NotImplementedError

    @operation()
    def close(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close client.

        Returns:
            None.
        """
        raise NotImplementedError

    @operation()
    async def aget(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Get secret value.

        Args:
            key: Secret key.

        Returns:
            Secret item with value.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    async def aget_metadata(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Get secret metadata.

        Args:
            key: Secret key.

        Returns:
            Secret item with metadata.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    async def aget_versions(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Get secret versions.

        Args:
            key: Secret key.

        Returns:
            Secret item with versions.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    async def aput(
        self,
        key: str | dict | SecretKey,
        value: str,
        metadata: dict | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Put secret.

        Args:
            key: Secret key.
            value: Secret value.
            metadata: Secret metadata.
            where: Conditional expression.

        Returns:
            Secret item.

        Raises:
            PreconditionFailedError:
                Condition not satisfied.
        """
        raise NotImplementedError

    @operation()
    async def aupdate(
        self,
        key: str | dict | SecretKey,
        value: str,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Update secret value.

        Args:
            key: Secret key.
            value: Secret value.

        Returns:
            Secret item.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    async def aupdate_metadata(
        self,
        key: str | dict | SecretKey,
        metadata: dict | None,
        **kwargs: Any,
    ) -> Response[SecretItem]:
        """Update secret metadata.

        Args:
            key: Secret key.
            metadata: Secret metadata.

        Returns:
            Secret item.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    async def adelete(
        self,
        key: str | dict | SecretKey,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete secret.

        Args:
            key: Secret key.

        Returns:
            None.

        Raises:
            NotFoundError: Key not found.
        """
        raise NotImplementedError

    @operation()
    async def aquery(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[SecretList]:
        """Query secrets.

        Args:
            where: Condition expression.

        Returns:
            Secret list.
        """
        raise NotImplementedError

    @operation()
    async def acount(
        self,
        where: str | Expression | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count secrets.

        Args:
            where: Condition expression.

        Returns:
            Count of secrets.
        """
        raise NotImplementedError

    @operation()
    async def aclose(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close async client.

        Returns:
            None.
        """
        raise NotImplementedError
