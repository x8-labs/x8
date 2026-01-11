import argparse
import sys
from typing import Any

from ._arg_parser import ArgParser
from ._async_helper import run_sync
from ._context import Context, RunContext
from ._loader import Loader
from .manifest import MANIFEST_FILE
from .spec import SpecBuilder


def run(
    path: str,
    manifest: str,
    handle: str,
    tag: str | None,
    statement: str | None,
) -> Any:
    """
    x8 Run
    """
    loader = Loader(path=path, manifest=manifest)
    component_type = loader.get_component_type(handle=handle)
    component = loader.load_component(handle=handle, tag=tag)
    run_context = RunContext(
        handle=handle,
        tag=tag,
        path=path,
        manifest=manifest,
        component_type=component_type,
    )
    operation = None
    if statement is not None:
        operation = ArgParser.convert_execute_operation(statement, None)
    return component.__run__(
        operation=operation,
        context=Context(data=dict(__run__=run_context)),
    )


async def arun(
    path: str,
    manifest: str,
    handle: str,
    tag: str | None,
    statement: str | None,
) -> Any:
    """
    x8 Run Async
    """
    loader = Loader(path=path, manifest=manifest)
    component_type = loader.get_component_type(handle=handle)
    component = loader.load_component(handle=handle, tag=tag)
    run_context = RunContext(
        handle=handle,
        tag=tag,
        path=path,
        manifest=manifest,
        component_type=component_type,
    )
    operation = None
    if statement is not None:
        operation = ArgParser.convert_execute_operation(statement, None)
    return await component.__arun__(
        operation=operation,
        context=Context(data=dict(__run__=run_context)),
    )


def requirements(
    path: str,
    manifest: str,
    handle: str,
    tag: str | None,
    out: str | None,
):
    """
    x8 Requirements
    """
    loader = Loader(path=path, manifest=manifest)
    requirements = loader.generate_requirements(
        handle=handle, tag=tag, out=out
    )
    return requirements


def spec(
    path: str,
    manifest: str,
    type_or_handle: str,
):
    """
    x8 Spec
    """
    spec_builder = SpecBuilder(path=path)
    if "." in type_or_handle:
        component_type = type_or_handle
    else:
        component_type = Loader(
            path=path, manifest=manifest
        ).get_component_type(handle=type_or_handle)
    component_spec = spec_builder.build_component_spec(component_type)
    spec_builder.print(component_spec)


def main():
    parser = argparse.ArgumentParser(prog="x8", description="x8 CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="Run the x8 application")
    arun_parser = subparsers.add_parser(
        "arun", help="Run the x8 application in async mode"
    )
    requirements_parser = subparsers.add_parser(
        "requirements", help="Generate the pip requirements"
    )
    spec_parser = subparsers.add_parser("spec", help="Get component spec")
    run_parser_arguments = [
        (
            "handle",
            str,
            None,
            "Component handle (required positional argument)",
            None,
        ),
        (
            "tag",
            str,
            None,
            "Binding tag (optional positional argument)",
            "?",
        ),
        ("--path", str, ".", "Project directory", None),
        ("--manifest", str, MANIFEST_FILE, "Manifest filename", None),
        ("--execute", str, None, "Operation to execute", None),
    ]
    requirements_parser_arguments = [
        (
            "handle",
            str,
            None,
            "Component handle (required positional argument)",
            None,
        ),
        (
            "tag",
            str,
            None,
            "Binding tag (optional positional argument)",
            "?",
        ),
        ("--path", str, ".", "Project directory", None),
        ("--manifest", str, MANIFEST_FILE, "Manifest filename", None),
        ("--out", str, None, "Output path", None),
    ]
    spec_parser_arguments = [
        ("type_or_handle", str, None, "Component type or handle", "?"),
        ("--path", str, ".", "Project directory", None),
        ("--manifest", str, MANIFEST_FILE, "Manifest filename", None),
    ]
    for arg in run_parser_arguments:
        run_parser.add_argument(
            arg[0], type=arg[1], default=arg[2], help=arg[3], nargs=arg[4]
        )
        arun_parser.add_argument(
            arg[0], type=arg[1], default=arg[2], help=arg[3], nargs=arg[4]
        )
    for arg in requirements_parser_arguments:
        requirements_parser.add_argument(
            arg[0], type=arg[1], default=arg[2], help=arg[3], nargs=arg[4]
        )
    for arg in spec_parser_arguments:
        spec_parser.add_argument(
            arg[0], type=arg[1], default=arg[2], help=arg[3], nargs=arg[4]
        )

    args = parser.parse_args()
    if args.command == "run":
        response = run(
            path=args.path,
            manifest=args.manifest,
            handle=args.handle,
            tag=args.tag,
            statement=args.execute,
        )
        if response:
            print(response)
    elif args.command == "arun":
        try:
            response = run_sync(
                arun,
                path=args.path,
                manifest=args.manifest,
                handle=args.handle,
                tag=args.tag,
                statement=args.execute,
            )
            if response:
                print(response)
        except KeyboardInterrupt:
            sys.exit(0)
    elif args.command == "requirements":
        requirements(
            path=args.path,
            manifest=args.manifest,
            handle=args.handle,
            tag=args.tag,
            out=args.out,
        )
    elif args.command == "spec":
        spec(
            path=args.path,
            manifest=args.manifest,
            type_or_handle=args.type_or_handle,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
