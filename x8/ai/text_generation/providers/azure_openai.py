from typing import Any, Mapping

from openai import AsyncAzureOpenAI
from openai import AzureOpenAI as AzureOpenAIClient

from .openai_legacy import OpenAILegacy


class AzureOpenAI(OpenAILegacy):
    """Azure OpenAI provider using the Chat Completions API.

    This provider uses Azure's OpenAI Service with the Chat Completions API.
    It inherits from OpenAILegacy and configures Azure-specific client
    settings.
    """

    api_version: str | None
    azure_endpoint: str | None
    azure_deployment: str | None
    azure_ad_token: str | None
    organization: str | None

    def __init__(
        self,
        api_key: str | None = None,
        api_version: str | None = None,
        azure_endpoint: str | None = None,
        azure_deployment: str | None = None,
        azure_ad_token: str | None = None,
        organization: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        model: str = "gpt-4o",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize Azure OpenAI provider.

        Args:
            api_key:
                Azure OpenAI API key.
            api_version:
                Azure API version (e.g., "2024-12-01-preview").
            azure_endpoint:
                Azure endpoint URL
                (e.g., "https://your-resource.openai.azure.com/").
            azure_deployment:
                Azure deployment name. If not provided, uses model name.
            azure_ad_token:
                Azure Active Directory token for authentication.
            organization:
                OpenAI organization ID (optional for Azure).
            timeout:
                Timeout for client requests.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            model:
                Model deployment name to use for text generation.
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for Azure OpenAI client.
        """
        self.api_version = api_version
        self.azure_endpoint = azure_endpoint
        self.azure_deployment = azure_deployment
        self.azure_ad_token = azure_ad_token

        # Call parent init
        # Note: we don't pass base_url since Azure uses azure_endpoint
        super().__init__(
            api_key=api_key,
            organization=organization,
            project=None,  # Azure doesn't use project
            base_url=None,  # Azure uses azure_endpoint instead
            timeout=timeout,
            max_retries=max_retries,
            default_headers=default_headers,
            default_query=default_query,
            model=model,
            max_tokens=max_tokens,
            nparams=nparams,
            **kwargs,
        )

    def __setup__(self, context=None):
        if self._init:
            return

        client_kwargs = self._get_azure_client_params()
        self._client = AzureOpenAIClient(**client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return

        client_kwargs = self._get_azure_client_params()
        self._async_client = AsyncAzureOpenAI(**client_kwargs)
        self._ainit = True

    def _get_azure_client_params(self) -> dict[str, Any]:
        """Build Azure-specific client parameters."""
        params: dict[str, Any] = {}

        if self.api_key is not None:
            params["api_key"] = self.api_key
        if self.api_version is not None:
            params["api_version"] = self.api_version
        if self.azure_endpoint is not None:
            params["azure_endpoint"] = self.azure_endpoint
        if self.azure_deployment is not None:
            params["azure_deployment"] = self.azure_deployment
        if self.azure_ad_token is not None:
            params["azure_ad_token"] = self.azure_ad_token
        if self.organization is not None:
            params["organization"] = self.organization
        if self.timeout is not None:
            params["timeout"] = self.timeout
        if self.max_retries is not None:
            params["max_retries"] = self.max_retries
        if self.default_headers is not None:
            params["default_headers"] = self.default_headers
        if self.default_query is not None:
            params["default_query"] = self.default_query
        if self.nparams:
            params.update(self.nparams)

        return params
