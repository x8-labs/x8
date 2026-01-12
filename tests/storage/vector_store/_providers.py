from typing import Any

from common.secrets import get_secrets

from x8.storage.vector_store import VectorStore

secrets = get_secrets()


class VectorStoreProvider:
    PINECONE = "pinecone"
    MILVUS = "milvus"
    QDRANT = "qdrant"
    CHROMA = "chroma"
    WEAVIATE = "weaviate"


provider_parameters: dict[str, dict[str, Any]] = {
    VectorStoreProvider.PINECONE: {
        "api_key": secrets["storage-vector-pinecone-api-key"],
        "index": "test",
    },
    VectorStoreProvider.MILVUS: {
        "uri": secrets["storage-vector-milvus-uri"],
        "token": secrets["storage-vector-milvus-token"],
        "collection": "test",
        "content_field": "content",
    },
    VectorStoreProvider.QDRANT: {
        "url": secrets["storage-vector-qdrant-url"],
        "api_key": secrets["storage-vector-qdrant-api-key"],
        "collection": "test",
    },
    VectorStoreProvider.CHROMA: {
        "collection": "test",
    },
    VectorStoreProvider.WEAVIATE: {
        "url": secrets["storage-vector-weaviate-url"],
        "api_key": secrets["storage-vector-weaviate-api-key"],
        "collection": "test",
    },
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = VectorStore(
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        )
    )
    return component
