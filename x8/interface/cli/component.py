from x8.core import Component


class CLI(Component):
    component: Component | None

    def __init__(self, component: Component | None = None, **kwargs):
        self.component = component
        super().__init__(**kwargs)
