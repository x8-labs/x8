from x8.core import Component


class Web(Component):
    component: Component | None

    def __init__(
        self,
        component: Component | None = None,
        **kwargs,
    ):
        """Initialize."""
        self.component = component
        super().__init__(**kwargs)
