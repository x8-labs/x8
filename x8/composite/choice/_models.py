from x8.core import DataModel, Provider


class ChoiceProviderInfo(DataModel):
    key: str | None = None
    provider: Provider
