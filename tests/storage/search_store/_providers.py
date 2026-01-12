from typing import Any

from common.secrets import get_secrets

from x8.storage.search_store import SearchStore

secrets = get_secrets()


class SearchStoreProvider:
    ELASTICSEARCH = "elasticsearch"


provider_parameters: dict[str, dict[str, Any]] = {
    SearchStoreProvider.ELASTICSEARCH: {
        "hosts": "http://localhost:9200",
    },
}


def get_component(provider_type: str, collection: str = "test"):
    component = SearchStore(
        collection=collection,
        __provider__=dict(
            type=provider_type,
            parameters=provider_parameters[provider_type],
        ),
    )
    return component
