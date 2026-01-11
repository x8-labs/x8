from __future__ import annotations

from typing import Any

from ._yaml_loader import YamlLoader
from .data_model import DataModel

__all__ = [
    "ComponentConfig",
    "Manifest",
    "ProviderConfig",
    "MANIFEST_FILE",
    "ManifestMetadata",
]


MANIFEST_FILE = "x8.yaml"


class ManifestMetadata(DataModel):
    name: str | None = None
    description: str | None = None
    version: str | None = None


class ProviderConfig(DataModel):
    type: str
    parameters: dict[str, Any] = dict()
    requirements: list[str] = list()


class ComponentConfig(DataModel):
    type: str
    parameters: dict[str, Any] = dict()
    providers: dict[str, ProviderConfig] = dict()
    requirements: list[str] = list()


class Manifest(DataModel):
    metadata: ManifestMetadata = ManifestMetadata()
    variables: dict[str, Any] = dict()
    components: dict[str, ComponentConfig] = dict()
    bindings: dict[str, dict[str, str]] = dict()
    requirements: list[str] = list()

    @staticmethod
    def parse(path: str) -> Manifest:
        obj = YamlLoader.load(path=path)
        manifest = Manifest.from_dict(obj)
        return manifest
