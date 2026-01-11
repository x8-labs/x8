__all__ = ["OpenAIProvider"]

from typing import Any, Mapping

from openai import AsyncOpenAI as BaseAsyncOpenAI
from openai import OpenAI as BaseOpenAI
from x8.core import Context, Provider


class OpenAIProvider(Provider):
    model: str | None
    api_key: str | None
    organization: str | None
    project: str | None
    base_url: str | None
    websocket_base_url: str | None
    webhook_secret: str | None
    timeout: float | None
    max_retries: int | None
    default_headers: Mapping[str, str] | None
    default_query: Mapping[str, object] | None
    nparams: dict | None

    _client: BaseOpenAI
    _aclient: BaseAsyncOpenAI
    _init: bool
    _ainit: bool

    def __init__(
        self,
        model: str | None,
        api_key: str | None = None,
        organization: str | None = None,
        project: str | None = None,
        base_url: str | None = None,
        websocket_base_url: str | None = None,
        webhook_secret: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        nparams: dict | None = None,
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            model:
                OpenAI model to use for video generation.
            api_key:
                OpenAI API key.
            organization:
                OpenAI organization.
            project:
                OpenAI project.
            base_url:
                OpenAI base url.
            websocket_base_url:
                OpenAI websocket base url.
            webhook_secret:
                OpenAI webhook secret.
            timeout:
                Timeout for client.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            nparams:
                Native params for OpenAI client.
        """

        self.model = model
        self.api_key = api_key
        self.organization = organization
        self.project = project
        self.base_url = base_url
        self.websocket_base_url = websocket_base_url
        self.webhook_secret = webhook_secret
        self.timeout = timeout
        self.max_retries = max_retries
        self.default_headers = default_headers
        self.default_query = default_query
        self.nparams = nparams
        self._init = False
        self._ainit = False
        super().__init__(**kwargs)

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self._client = BaseOpenAI(**self._get_client_params())
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return

        self._aclient = BaseAsyncOpenAI(**self._get_client_params())
        self._ainit = True

    def _get_client_params(self) -> dict[str, Any]:
        params = {
            "api_key": self.api_key,
            "organization": self.organization,
            "project": self.project,
            "base_url": self.base_url,
            "websocket_base_url": getattr(self, "websocket_base_url", None),
            "webhook_secret": getattr(self, "webhook_secret", None),
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "default_headers": self.default_headers,
            "default_query": self.default_query,
        }
        return {k: v for k, v in params.items() if v is not None} | (
            self.nparams or {}
        )
