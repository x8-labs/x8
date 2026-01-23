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
    MISTRAL = "mistral"
    COHERE = "cohere"
    COHERE_VISION = "cohere_vision"
    AZURE_OPENAI = "azure_openai"
    AMAZON_BEDROCK = "amazon_bedrock"


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
    TextGenerationProvider.MISTRAL: "mistral",
    TextGenerationProvider.COHERE: "cohere",
    TextGenerationProvider.COHERE_VISION: "cohere",
    TextGenerationProvider.AZURE_OPENAI: "azure_openai",
    TextGenerationProvider.AMAZON_BEDROCK: "amazon_bedrock",
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
    TextGenerationProvider.MISTRAL: {
        "api_key": secrets["MISTRAL_API_KEY"],
    },
    TextGenerationProvider.COHERE: {
        "api_key": secrets["COHERE_API_KEY"],
    },
    TextGenerationProvider.COHERE_VISION: {
        "api_key": secrets["COHERE_API_KEY"],
        "model": "command-a-vision-07-2025",
    },
    TextGenerationProvider.AZURE_OPENAI: {
        "api_version": "2024-12-01-preview",
        "azure_endpoint": secrets["AZURE_OPENAI_ENDPOINT"],  # noqa
        "model": "model-router",
        "api_key": secrets["AZURE_OPENAI_API_KEY"],
    },
    TextGenerationProvider.AMAZON_BEDROCK: {
        "model": "us.amazon.nova-lite-v1:0",
        "region": "us-east-1",
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
