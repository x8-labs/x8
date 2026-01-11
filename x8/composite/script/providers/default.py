import ast
import textwrap
from typing import Any

from x8.core import Context, Operation, Provider


class Default(Provider):
    script: str
    _kwargs: Any

    def __init__(self, script: str, **kwargs):
        self.script = script
        self._kwargs = kwargs

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        # Parse and reformat the script safely
        try:
            ast.parse(self.script)
        except SyntaxError as e:
            raise ValueError(f"Invalid script: {e}")

        execution_context = {**self._kwargs}
        script_body = textwrap.indent(self.script, "    ")
        script_function = f"""
def __run__(operation, context):
{script_body}
"""
        exec(script_function, execution_context)
        return execution_context["__run__"](operation, context)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        # Parse and reformat the script safely
        try:
            ast.parse(self.script)
        except SyntaxError as e:
            raise ValueError(f"Invalid script: {e}")

        execution_context = {**self._kwargs}
        script_body = textwrap.indent(self.script, "    ")
        script_function = f"""
async def __run__(operation, context):
{script_body}
"""
        exec(script_function, execution_context)
        return await execution_context["__run__"](operation, context)
