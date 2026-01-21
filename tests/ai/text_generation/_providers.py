from typing import Any

from common.secrets import get_secrets

from x8.ai.text_generation import TextGeneration

secrets = get_secrets()


class TextGenerationProvider:
    OPENAI = "openai"
    OPENAI_LEGACY = "openai_legacy"
    GOOGLE = "google"
    ANTHROPIC = "anthropic"
    XAI = "xai"
    DEEPSEEK = "deepseek"
    OLLAMA = "ollama"
    GROQ = "groq"
    GROQ_LLAMA = "groq_llama"


provider_types: dict[str, str] = {
    TextGenerationProvider.OPENAI: "openai",
    TextGenerationProvider.OPENAI_LEGACY: "openai_legacy",
    TextGenerationProvider.GOOGLE: "google",
    TextGenerationProvider.ANTHROPIC: "anthropic",
    TextGenerationProvider.XAI: "xai",
    TextGenerationProvider.DEEPSEEK: "deepseek",
    TextGenerationProvider.OLLAMA: "ollama",
    TextGenerationProvider.GROQ: "groq",
    TextGenerationProvider.GROQ_LLAMA: "groq",
}


provider_parameters: dict[str, dict[str, Any]] = {
    TextGenerationProvider.OPENAI: {
        "api_key": secrets["OPENAI_API_KEY"],
    },
    TextGenerationProvider.OPENAI_LEGACY: {
        "api_key": secrets["OPENAI_API_KEY"],
    },
    TextGenerationProvider.GOOGLE: {},
    TextGenerationProvider.ANTHROPIC: {
        "api_key": secrets["ANTHROPIC_API_KEY"],
    },
    TextGenerationProvider.XAI: {
        "api_key": secrets["XAI_API_KEY"],
    },
    TextGenerationProvider.DEEPSEEK: {
        "api_key": secrets["DEEPSEEK_API_KEY"],
    },
    TextGenerationProvider.OLLAMA: {},
    TextGenerationProvider.GROQ: {
        "api_key": secrets["GROQ_API_KEY"],
    },
    TextGenerationProvider.GROQ_LLAMA: {
        "api_key": secrets["GROQ_API_KEY"],
        "model": "meta-llama/llama-4-scout-17b-16e-instruct",
    },
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = TextGeneration(
        __unpack__=True,
        __provider__=dict(
            type=provider_types[provider_type],
            parameters=parameters,
        ),
    )
    return component
