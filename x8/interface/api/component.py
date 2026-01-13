from x8.core import Component, Response, operation

from ._models import APIAuth, APIInfo, ComponentMapping


class API(Component):
    components: list[ComponentMapping]
    auth: APIAuth | None
    prefix: str | None = None
    host: str
    port: int

    def __init__(
        self,
        components: list[ComponentMapping] = [],
        auth: APIAuth | None = None,
        prefix: str | None = None,
        host: str = "127.0.0.1",
        port: int = 8080,
        **kwargs,
    ):
        """Initialize.

        Args:
            components:
                Components to host in a single API.
            auth:
                Component to handle authentication.
            prefix:
                Prefix for the API.
            host:
                Host IP address.
            port:
                HTTP port.
        """
        self.components = components
        self.prefix = prefix
        self.auth = auth
        self.host = host
        self.port = port
        super().__init__(**kwargs)

    @operation()
    def run(self) -> Response[None]:
        """Run the API."""
        raise NotImplementedError

    @operation()
    async def arun(self) -> Response[None]:
        """Run the API asynchronously."""
        raise NotImplementedError

    @operation()
    def get_info(self) -> Response[APIInfo]:
        """Get API Info"""
        raise NotImplementedError

    @operation()
    async def aget_info(self) -> Response[APIInfo]:
        """Get API Info"""
        raise NotImplementedError
