from __future__ import annotations

from .data_model import DataModel


class ComponentNode(DataModel):
    handle: str
    type: str
    kind: str
    requirements: list[str]
    provider: ProviderNode | None = None
    depends: list[ComponentEdge] = []


class ProviderNode(DataModel):
    handle: str
    type: str
    kind: str
    requirements: list[str]
    composes: list[ProviderEdge] = []
    depends: list[ComponentEdge] = []


class ComponentEdge(DataModel):
    component: ComponentNode
    ref: str


class ProviderEdge(DataModel):
    provider: ProviderNode
    ref: str
