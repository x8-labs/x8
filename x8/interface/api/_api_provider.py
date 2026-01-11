from typing import Any

import httpx
from x8.core import Context, Operation, Provider, TypeConverter
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

from ._helper import OperationInfo, get_component, get_operations
from ._models import ComponentMapping


class APIProvider(Provider):
    base_url: str
    credential: str | None
    timeout: float | None
    nparams: dict[str, Any]

    _init: bool
    _component_mapping: ComponentMapping
    _operation_mappings: dict[str, OperationInfo]

    def __init__(
        self,
        base_url: str,
        credential: str | None = None,
        timeout: float | None = 60,
        nparams: dict[str, object] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            base_url:
                HTTP base URL.
            credential:
                API credential.
            timeout:
                HTTP timeout. Defaults to 60 seconds.
            nparams:
                Native params to httpx client.
        """
        self.base_url = base_url
        self.credential = credential
        self.timeout = timeout
        self.nparams = nparams
        self._init = False

    def _init_mappings(self) -> None:
        if self._init:
            return
        self._component_mapping = get_component(
            ComponentMapping(component=self.__component__)
        )
        self._operation_mappings = get_operations(self._component_mapping)
        self._init = True

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self._init_mappings()
        if not operation or not operation.name:
            # TODO: can call generic method
            raise BadRequestError("Operation name is required")

        op_info = self._get_operation_info(operation.name)
        url = self._get_url(operation, op_info)
        body = self._get_body(operation, op_info)
        headers = self._get_headers()
        with httpx.Client(timeout=self.timeout) as client:
            try:
                method = op_info.http_method.lower()
                response = client.request(
                    method, url, json=body, headers=headers
                )
                response.raise_for_status()
                content_type = response.headers.get("content-type", "").lower()
                return_type = op_info.return_type
                if "application/x-ndjson" in content_type:

                    def _iter():
                        for line in response.iter_lines():
                            if not line.strip():
                                continue
                            data = httpx.Response.json(
                                httpx.Response(200, text=line)
                            )
                            yield (
                                TypeConverter.convert_value(data, return_type)
                                if return_type
                                else data
                            )

                    return _iter()
                elif (
                    "application/octet-stream" in content_type
                    or "video/" in content_type
                    or "image/" in content_type
                    or "audio/" in content_type
                ):
                    if op_info.is_return_iterator:

                        def _iter_bytes():
                            for chunk in response.iter_bytes():
                                yield chunk

                        return _iter_bytes()
                    else:
                        # return full bytes content
                        return response.content
                elif "application/json" in content_type:
                    data = response.json()
                    if return_type:
                        return TypeConverter.convert_value(data, return_type)
                    return data
                else:
                    return response.text
            except httpx.HTTPStatusError as e:
                raise self._handle_http_error(e.response)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self._init_mappings()
        if not operation or not operation.name:
            # TODO: can call generic method
            raise BadRequestError("Operation name is required")
        op_info = self._get_operation_info(operation.name)
        url = self._get_url(operation, op_info)
        body = self._get_body(operation, op_info)
        headers = self._get_headers()
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                method = op_info.http_method.lower()
                response = await client.request(
                    method, url, json=body, headers=headers
                )
                response.raise_for_status()
                content_type = response.headers.get("content-type", "").lower()
                return_type = op_info.return_type
                if "application/x-ndjson" in content_type:

                    async def _aiter():
                        async for line in response.aiter_lines():
                            if not line.strip():
                                continue
                            data = httpx.Response.json(
                                httpx.Response(200, text=line)
                            )
                            yield (
                                TypeConverter.convert_value(data, return_type)
                                if return_type
                                else data
                            )

                    return _aiter()
                elif (
                    "application/octet-stream" in content_type
                    or "video/" in content_type
                    or "image/" in content_type
                    or "audio/" in content_type
                ):
                    if op_info.is_return_iterator:

                        async def _aiter_bytes():
                            async for chunk in response.aiter_bytes():
                                yield chunk

                        return _aiter_bytes()
                    else:
                        # return full bytes content
                        return response.content
                elif "application/json" in content_type:
                    data = response.json()
                    if return_type:
                        return TypeConverter.convert_value(data, return_type)
                    return data
                else:
                    return response.text

            except httpx.HTTPStatusError as e:
                raise self._handle_http_error(e.response)

    def _get_operation_info(self, name: str) -> OperationInfo:
        if name not in self._operation_mappings:
            raise BadRequestError(f"Operation '{name}' not found")
        return self._operation_mappings[name]

    def _get_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.credential:
            headers["Authorization"] = f"Bearer {self.credential}"
        return headers

    def _get_url(
        self,
        operation: Operation,
        operation_info: OperationInfo,
    ) -> str:
        base = self.base_url.rstrip("/")
        path = operation_info.path.lstrip("/")
        if path:
            url = f"{base}/{path}"
        else:
            url = base
        for path_param in operation_info.path_params:
            if operation.args is None or path_param not in operation.args:
                raise BadRequestError(
                    f"Path parameter '{path_param}' is required"
                )
            value = str(operation.args[path_param])
            url = url.replace(f"{{{path_param}}}", value)
        return url

    def _get_body(
        self, operation: Operation, operation_info: OperationInfo
    ) -> dict[str, Any] | None:
        args = operation.args or {}
        for path_param in operation_info.path_params:
            args.pop(path_param, None)
        return args if args else None

    def _handle_http_error(self, response: httpx.Response) -> Exception:
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
