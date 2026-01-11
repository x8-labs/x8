from typing import Any

import httpx
from x8.core import Context, DataModel, Operation, Provider, Response
from x8.core.exceptions import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    InternalError,
    NotFoundError,
    NotModified,
    NotSupportedError,
    PreconditionFailedError,
    UnauthorizedError,
)


class PostRequest(DataModel):
    operation: Operation | None
    context: Context | None


class HostedProvider(Provider):
    endpoint: str
    credential: str | None
    timeout: float | None
    nparams: dict[str, Any]

    def __init__(
        self,
        endpoint: str,
        credential: str | None = None,
        timeout: float | None = 60,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            endpoint:
                HTTP endpoint.
            credential:
                API credential.
            timeout:
                HTTP timeout. Defaults to 60 seconds.
            nparams:
                Native params to httpx client.
        """
        self.endpoint = endpoint
        self.credential = credential
        self.timeout = timeout
        self.nparams = nparams

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        headers = {
            "Authorization": f"Bearer {self.credential}",
            "Content-Type": "application/json",
        }
        request = PostRequest(operation=operation, context=context)
        with httpx.Client(timeout=self.timeout) as client:
            try:
                response = client.post(
                    self.endpoint,
                    json=request.to_dict(),
                    headers=headers,
                )
                response.raise_for_status()
                return Response(**response.json())
            except httpx.HTTPStatusError as e:
                raise self.handle_http_error(e.response)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        headers = {
            "Authorization": f"Bearer {self.credential}",
            "Content-Type": "application/json",
        }
        request = PostRequest(operation=operation, context=context)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.post(
                    self.endpoint,
                    json=request.to_dict(),
                    headers=headers,
                )
                response.raise_for_status()
                return Response(**response.json())
            except httpx.HTTPStatusError as e:
                raise self.handle_http_error(e.response)

    def handle_http_error(self, response: httpx.Response) -> Exception:
        if response.status_code == 304:
            return NotModified(response.text)
        if response.status_code == 400:
            return BadRequestError(response.text)
        elif response.status_code == 401:
            return UnauthorizedError(response.text)
        elif response.status_code == 403:
            return ForbiddenError(response.text)
        elif response.status_code == 404:
            return NotFoundError(response.text)
        elif response.status_code == 409:
            return ConflictError(response.text)
        elif response.status_code == 412:
            return PreconditionFailedError(response.text)
        elif response.status_code == 415:
            return NotSupportedError(response.text)
        elif response.status_code == 500:
            return InternalError(response.text)
        return InternalError(f"{response.status_code}: {response.text}")
