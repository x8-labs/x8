from x8.core import Component, operation


class Hello(Component):
    @operation()
    def hello(self) -> str:
        return "Hello, World!"
