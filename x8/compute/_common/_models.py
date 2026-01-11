from x8.compute.containerizer import BuildConfig, PrepareConfig
from x8.core import Component, DataModel


class ImageMap(DataModel):
    name: str
    local_image: str | None = None
    source: str | None = None
    component: Component | None = None
    handle: str | None = None
    prepare: PrepareConfig = PrepareConfig()
    build: BuildConfig = BuildConfig()
