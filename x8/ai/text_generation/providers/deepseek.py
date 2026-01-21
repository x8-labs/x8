from typing import Any, Mapping

from .openai_legacy import OpenAILegacy


class DeepSeek(OpenAILegacy):
    """DeepSeek provider using the OpenAI-compatible Chat Completions API.

    DeepSeek provides an OpenAI-compatible API, so this provider inherits from
    OpenAILegacy and just sets DeepSeek-specific defaults.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = "https://api.deepseek.com",
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        model: str = "deepseek-chat",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                DeepSeek API key.
            base_url:
                DeepSeek API base url.
            timeout:
                Timeout for client requests.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            model:
                DeepSeek model to use for text generation.
                Available models: deepseek-chat, deepseek-reasoner.
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for OpenAI-compatible client.
        """
        super().__init__(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            default_headers=default_headers,
            default_query=default_query,
            model=model,
            max_tokens=max_tokens,
            nparams=nparams,
            **kwargs,
        )
