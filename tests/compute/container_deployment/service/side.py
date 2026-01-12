import os

from x8.core import Component, operation


class SideService(Component):
    @operation()
    def add(self, a: int, b: int) -> int:
        return a + b

    @operation()
    def subtract(self, a: int, b: int) -> int:
        return a - b

    @operation()
    def version(self) -> str:
        return os.getenv("VERSION", "default")
