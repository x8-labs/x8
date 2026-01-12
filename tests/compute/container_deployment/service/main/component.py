from x8.core import Component, operation


class MainService(Component):
    @operation()
    def do(self, a: int) -> int:
        raise NotImplementedError

    @operation()
    def health(self) -> str:
        return "OK"
