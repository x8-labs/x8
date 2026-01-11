from __future__ import annotations

import ast
import importlib
import inspect
import os
import sys
from enum import Enum
from typing import Any, Literal, get_type_hints

from ._component import Component, Provider
from ._loader import Loader
from ._yaml_loader import YamlLoader
from .data_model import DataModel, Empty, get_args, get_origin
from .exceptions import NotFoundError

__all__ = [
    "Kind",
    "ComponentSpec",
    "ContentModal",
    "OperationArgSpec",
    "OperationExceptionSpec",
    "OperationReturnSpec",
    "OperationSpec",
    "ParameterOptionSpec",
    "ParameterSpec",
    "ProviderMode",
    "ProviderSpec",
    "PyInfo",
    "TypeAttributeSpec",
    "TypeSpec",
]


class Kind(str, Enum):
    BASE = "base"
    CUSTOM = "custom"
    COMPOSITE = "composite"


class ContentModal(str, Enum):
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    MULTI = "multi"


class ProviderMode(str, Enum):
    MIXED = "mixed"
    LOCAL = "local"
    SERVICE = "service"


class TypeInfo(DataModel):
    name: str
    args: list[str | TypeInfo] | None = None

    def __repr__(self):
        if self.args:
            if self.name == "Union":
                return " | ".join(repr(arg) for arg in self.args)
            formatted_args = ", ".join(repr(arg) for arg in self.args)
            return f"{self.name}[{formatted_args}]"
        return self.name

    def __str__(self):
        return self.__repr__()


class ParameterOptionSpec(DataModel):
    label: str
    value: Any


class ParameterSpec(DataModel):
    name: str
    type: TypeInfo | None = None
    description: str | None = None
    required: bool = True
    default: Any = None
    options: list[ParameterOptionSpec] = []


class OperationArgSpec(DataModel):
    name: str
    type: TypeInfo | None = None
    description: str | None = None
    required: bool = True
    default: Any = None


class OperationReturnSpec(DataModel):
    type: TypeInfo | None = None
    description: str | None = None


class OperationExceptionSpec(DataModel):
    type: TypeInfo | None = None
    description: str | None = None


class OperationSpec(DataModel):
    name: str
    args: list[OperationArgSpec] = []
    description: str | None = None
    ret: OperationReturnSpec | None = None
    exceptions: list[OperationExceptionSpec] = []


class TypeAttributeSpec(DataModel):
    name: str
    type: TypeInfo | None = None
    description: str | None = None
    required: bool | None = True
    default: Any = None


class TypeSpec(DataModel):
    name: str
    description: str | None = None
    attributes: list[TypeAttributeSpec] = []
    inherits: list[TypeSpec] = []


class PyInfo(DataModel):
    package: str | None = None
    path: str | None = None
    requirements: list[str] = []


class ProviderSpec(DataModel):
    type: str
    version: str | None = None
    title: str | None = None
    description: str | None = None
    logo: str | None = None
    author: str | None = None
    readme: str | None = None
    link: str | None = None
    kind: Kind = Kind.BASE
    mode: ProviderMode = ProviderMode.MIXED
    parameters: list[ParameterSpec] = []
    py: PyInfo | None = None


class ComponentSpec(DataModel):
    type: str
    version: str | None = None
    title: str | None = None
    description: str | None = None
    logo: str | None = None
    author: str | None = None
    readme: str | None = None
    link: str | None = None
    category: str | None = None
    modals: list[ContentModal] = []
    kind: Kind = Kind.BASE
    parameters: list[ParameterSpec] = []
    operations: list[OperationSpec] = []
    providers: list[ProviderSpec] = []
    types: list[TypeSpec] = []
    py: PyInfo | None = None

    @staticmethod
    def parse(path: str) -> ComponentSpec:
        obj = YamlLoader.load(path=path)
        manifest = ComponentSpec.from_dict(obj)
        return manifest

    def get_provider(self, name: str) -> ProviderSpec:
        for provider in self.providers:
            if name == provider.type:
                return provider
        raise NotFoundError("Provider not found")


class SpecBuilder:
    def __init__(
        self,
        path: str = ".",
    ):
        abs_project_folder = os.path.abspath(path)
        if abs_project_folder not in sys.path:
            sys.path.append(abs_project_folder)

    def build_component_spec(
        self,
        component_type: str,
    ) -> ComponentSpec:
        py_path = Loader.get_component_path(component_type)
        kind = Kind(Loader.get_component_kind(component_type))
        component = Loader.load_class(py_path, Component)
        title = component_type
        description = SpecBuilder._get_top_level_docstring_from_class(
            component
        )
        types = SpecBuilder._get_operations_custom_types(component)
        types = SpecBuilder._combine_types_list(
            types, SpecBuilder._get_parameters_custom_types(component)
        )
        types = SpecBuilder._add_dependent_types(types)
        type_specs = [SpecBuilder._get_type(type) for type in types]

        provider_types = SpecBuilder._get_provider_types(py_path)
        providers = []
        for provider_type in provider_types:
            try:
                providers.append(
                    self.build_provider_spec(component_type, provider_type)
                )
                types = SpecBuilder._combine_types_list(
                    types,
                    SpecBuilder._get_provider_parameters_custom_types(
                        component_type, provider_type
                    ),
                )
            except Exception as e:
                print(e)
        spec = ComponentSpec(
            type=component_type,
            title=title,
            description=description,
            kind=kind,
            category=None,
            operations=SpecBuilder._get_operations(component),
            parameters=SpecBuilder._get_parameters(component),
            providers=providers,
            types=type_specs,
            py=PyInfo(path=py_path),
        )
        return spec

    def build_provider_spec(
        self,
        component_type: str,
        provider_type: str,
    ) -> ProviderSpec:
        py_path = Loader.get_provider_path(component_type, provider_type)
        kind = Loader.get_provider_kind(component_type, provider_type)
        provider = Loader.load_class(py_path, Provider)
        title = provider_type
        description = SpecBuilder._get_top_level_docstring_from_class(provider)
        spec = ProviderSpec(
            type=provider_type,
            title=title,
            description=description,
            kind=kind,
            parameters=SpecBuilder._get_parameters(provider),
            py=PyInfo(path=py_path),
        )
        return spec

    def print(self, component_spec: ComponentSpec):
        print()
        title = component_spec.description or component_spec.title
        print(f"{title}")
        if title:
            print("-" * len(title))
        SpecBuilder._print_parameters(component_spec.parameters)
        SpecBuilder._print_operations(component_spec.operations)
        SpecBuilder._print_providers(component_spec.providers)
        SpecBuilder._print_types(component_spec.types)

    @staticmethod
    def _print_parameters(specs: list[ParameterSpec]):
        parameters = []
        for param in specs:
            if param.description:
                description = param.description.strip().replace("\n", " ")
            else:
                description = ""
            type = str(param.type)
            if len(param.options) > 0:
                options_str = ", ".join(
                    [option.value for option in param.options]
                )
                type = f"{type} ({options_str})"
            if param.required:
                type = f"* {type}"
            if not param.required:
                type = f"{type} ={str(param.default)}"
            parameters.append((param.name, description, type))

        if parameters:
            col_widths = [
                max(max(len(row[0]) for row in parameters), 12),
                max(max(len(row[1]) for row in parameters), 12),
                max(max(len(row[2]) for row in parameters), 12),
            ]
            print(
                f"{'Parameter'.ljust(col_widths[0])}  "
                f"{'Description'.ljust(col_widths[1])}  "
                f"{'Type'}"
            )
            print(
                f"{'---------'.ljust(col_widths[0])}  "
                f"{'-----------'.ljust(col_widths[1])}  "
                f"{'-----'}"
            )
            for name, desc, type in parameters:
                print(
                    f"{name.ljust(col_widths[0])}  "
                    f"{desc.ljust(col_widths[1])}  "
                    f"{type}"
                )
            print()

    @staticmethod
    def _print_operations(specs: list[OperationSpec]):
        operations = []
        for op in specs:
            if op.description:
                description = op.description.strip().replace("\n", " ")
            else:
                description = ""
            usage = op.name
            for arg in op.args:
                arg_usage = f"{arg.name} <{arg.type}>"
                if not arg.required:
                    arg_usage = f"[{arg_usage}]"
                usage = f"{usage} {arg_usage}"
            operations.append((op.name, description, usage))

        if operations:
            col_widths = [
                max(max(len(row[0]) for row in operations), 12),
                max(max(len(row[1]) for row in operations), 12),
                max(max(len(row[2]) for row in operations), 12),
            ]
            print(
                f"{'Operation'.ljust(col_widths[0])}  "
                f"{'Description'.ljust(col_widths[1])}  "
                f"{'Usage'}"
            )
            print(
                f"{'---------'.ljust(col_widths[0])}  "
                f"{'-----------'.ljust(col_widths[1])}  "
                f"{'-----'}"
            )

            for name, desc, usage in operations:
                usage_lines = usage.split("\n")
                print(
                    f"{name.ljust(col_widths[0])}  "
                    f"{desc.ljust(col_widths[1])}  "
                    f"{usage_lines[0]}"
                )
                for extra_line in usage_lines[1:]:
                    print(
                        " " * (col_widths[0] + 2 + col_widths[1] + 2)
                        + extra_line
                    )
        print()

    @staticmethod
    def _print_providers(specs: list[ProviderSpec]):
        print("Providers")
        print("---------")
        for provider in specs:
            print(provider.type)
            if provider.description:
                print(provider.description)
            print("------")
            SpecBuilder._print_parameters(provider.parameters)

    @staticmethod
    def _print_types(specs: list[TypeSpec]):
        for spec in specs:
            SpecBuilder._print_type(spec)

    @staticmethod
    def _print_type(spec: TypeSpec):
        if spec.description:
            desc = spec.description.strip().replace("\n", " ")
        else:
            desc = ""
        title = f"{spec.name}"
        if spec.inherits:
            inherits = ", ".join([f"{base.name}" for base in spec.inherits])
            title = f"{title} ({inherits})"
        print(title)
        print("-" * len(title))

        attributes = []
        for attr in spec.attributes:
            if attr.description:
                description = attr.description.strip().replace("\n", " ")
            else:
                description = ""
            type = str(attr.type)
            if attr.required:
                type = f"* {type}"
            if not attr.required:
                type = f"{type} ={str(attr.default)}"
            attributes.append((attr.name, description, type))

        if attributes:
            col_widths = [
                max(max(len(row[0]) for row in attributes), 12),
                max(max(len(row[1]) for row in attributes), 12),
                max(max(len(row[2]) for row in attributes), 12),
            ]
            print(
                f"{'Attribute'.ljust(col_widths[0])}  "
                f"{'Description'.ljust(col_widths[1])}  "
                f"{'Type'}"
            )
            print(
                f"{'---------'.ljust(col_widths[0])}  "
                f"{'-----------'.ljust(col_widths[1])}  "
                f"{'-----'}"
            )
            for name, desc, type in attributes:
                print(
                    f"{name.ljust(col_widths[0])}  "
                    f"{desc.ljust(col_widths[1])}  "
                    f"{type}"
                )
        print()

    @staticmethod
    def _get_provider_types(component_package: str) -> list[str]:
        component_path = SpecBuilder._get_component_file_path(
            component_package
        )
        if ":" in component_package:
            return []
        providers_path = os.path.join(component_path, "providers")
        providers = [
            f[:-3]
            for f in os.listdir(providers_path)
            if f.endswith(".py")
            and f != "__init__.py"
            and not f.startswith("_")
        ]
        return providers

    @staticmethod
    def _get_component_file_path(component_package: str) -> str:
        spec = importlib.util.find_spec(component_package.split(":")[0])
        if spec and spec.origin:
            return os.path.dirname(spec.origin)
        raise NotFoundError(f"Component not found: {component_package}")

    @staticmethod
    def _get_top_level_docstring_from_class(cls):
        file_path = inspect.getfile(cls)
        with open(file_path, "r", encoding="utf-8") as file:
            source_code = file.read()
        parsed_ast = ast.parse(source_code)
        return ast.get_docstring(parsed_ast)

    @staticmethod
    def _get_operations(cls) -> list[OperationSpec]:
        operations: list = []
        for method_name, method in inspect.getmembers(
            cls, predicate=inspect.isfunction
        ):
            if not SpecBuilder._is_operation(method):
                continue

            is_async = inspect.iscoroutinefunction(method)
            if is_async and method_name.startswith("a"):
                name = method_name[1:]
            else:
                name = method_name

            docstring = SpecBuilder._parse_docstring(method.__doc__)
            sig = inspect.signature(method)
            args = []
            parameters: Any = iter(sig.parameters.items())
            if "self" in sig.parameters:
                first_param = next(parameters)
                if first_param[0] == "self":
                    parameters = list(sig.parameters.items())[1:]

            hints = get_type_hints(method)
            for param_name, param in sig.parameters.items():
                if param_name == "self":
                    continue
                if param_name == "kwargs":
                    continue
                param_type = SpecBuilder._get_type_info(
                    hints.get(param_name, None)
                )
                required = param.default == inspect.Parameter.empty
                default = SpecBuilder._get_default_value(param)
                description = None
                if docstring:
                    for p in docstring.params:
                        if p.arg_name == param_name:
                            description = p.description
                            break

                arg_spec = OperationArgSpec(
                    name=param_name,
                    type=param_type,
                    required=required,
                    default=default,
                    description=description,
                )
                args.append(arg_spec)

            ret = None
            if sig.return_annotation != inspect.Signature.empty:
                if docstring and docstring.returns:
                    description = docstring.returns.description
                else:
                    description = None
                ret = OperationReturnSpec(
                    type=SpecBuilder._get_type_info(sig.return_annotation),
                    description=description,
                )
            if docstring:
                description = docstring.short_description
            else:
                description = None

            if not any(op.name == name for op in operations):
                operations.append(
                    OperationSpec(
                        name=name,
                        args=args,
                        ret=ret,
                        description=description,
                    )
                )
        return operations

    @staticmethod
    def _get_type(cls) -> TypeSpec:
        attributes = []
        parsed_doc = SpecBuilder._parse_docstring(inspect.getdoc(cls))
        doc_params = (
            {p.arg_name: p.description for p in parsed_doc.params}
            if parsed_doc
            else {}
        )
        if issubclass(cls, Enum):
            enum_type = type(next(iter(cls)).value)
            for member in cls:
                attributes.append(
                    TypeAttributeSpec(
                        name=member.name,
                        type=SpecBuilder._get_type_info(enum_type),
                        description=doc_params.get(member.name, None),
                        required=False,
                        default=member.value,
                    )
                )
        else:
            type_hints = get_type_hints(cls)
            for name, type_hint in type_hints.items():
                if name.startswith("_"):
                    continue
                if DataModel in cls.__mro__:
                    default = cls.get_default_value(name)
                    if default is Empty:
                        required = True
                        default = None
                    else:
                        required = False
                else:
                    if name in cls.__dict__:
                        default = cls.__dict__[name]
                        required = False
                    else:
                        default = None
                        required = True

                type_ = SpecBuilder._get_type_info(type_hint)
                description = doc_params.get(name, None)
                member_spec = TypeAttributeSpec(
                    name=name,
                    type=type_,
                    description=description,
                    required=required,
                    default=default,
                )
                attributes.append(member_spec)
        description = parsed_doc.description if parsed_doc else None
        inherits = []
        if cls is not DataModel:
            for base in cls.__bases__:
                if base is object:
                    continue
                if base.__module__ in ("builtins", "enum"):
                    continue
                inherits.append(SpecBuilder._get_type(base))
        return TypeSpec(
            name=cls.__name__,
            description=description,
            attributes=attributes,
            inherits=inherits,
        )

    @staticmethod
    def _get_parameters(cls) -> list[ParameterSpec]:
        parameters = []
        init_signature = inspect.signature(cls.__init__)
        doc = inspect.getdoc(cls.__init__)
        doc_parsed = SpecBuilder._parse_docstring(doc) if doc else None
        doc_params = (
            {p.arg_name: p.description for p in doc_parsed.params}
            if doc_parsed
            else {}
        )
        hints = get_type_hints(cls.__init__)
        for param_name, param in init_signature.parameters.items():
            if param_name == "self" or param_name == "kwargs":
                continue
            param_type = SpecBuilder._get_type_info(
                hints.get(param_name, None)
            )
            options = []
            if param.annotation is not inspect.Parameter.empty:
                origin = get_origin(param.annotation)
                args = get_args(param.annotation)
                if origin is Literal:
                    options = [
                        ParameterOptionSpec(label=str(a), value=a)
                        for a in args
                    ]
                elif inspect.isclass(param.annotation) and issubclass(
                    param.annotation, Enum
                ):
                    options = [
                        ParameterOptionSpec(label=e.name, value=e.value)
                        for e in param.annotation
                    ]
            required = param.default == inspect.Parameter.empty
            default = SpecBuilder._get_default_value(param)
            param_spec = ParameterSpec(
                name=param_name,
                type=param_type,
                description=doc_params.get(param_name, None),
                required=required,
                default=default,
                options=options,
            )
            parameters.append(param_spec)
        return parameters

    @staticmethod
    def _parse_docstring(docstring) -> Any:
        try:
            import docstring_parser

            return docstring_parser.parse(docstring)
        except ImportError:
            return None

    @staticmethod
    def _get_type_info(annotation) -> TypeInfo:
        if annotation == inspect.Parameter.empty:
            return TypeInfo(name="Any")
        origin = get_origin(annotation)
        args = get_args(annotation)
        if origin and args:
            if origin.__name__ == "UnionType" or origin.__name__ == "Union":
                return TypeInfo(
                    name="Union",
                    args=[SpecBuilder._get_type_info(arg) for arg in args],
                )
            else:
                return TypeInfo(
                    name=origin.__name__,
                    args=[SpecBuilder._get_type_info(arg) for arg in args],
                )
        if hasattr(annotation, "__name__"):
            if annotation.__name__ == "NoneType":
                return TypeInfo(name="None")
            return TypeInfo(name=annotation.__name__)
        return TypeInfo(name=str(annotation))

    @staticmethod
    def _get_operations_custom_types(cls) -> list[Any]:
        types: list = []
        for _, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not SpecBuilder._is_operation(method):
                continue
            custom_types = SpecBuilder._get_args_custom_types(method)
            types = SpecBuilder._combine_types_list(types, custom_types)
        return types

    @staticmethod
    def _get_args_custom_types(method) -> list[Any]:
        types: list = []
        sig = inspect.signature(method)
        hints = get_type_hints(method)
        for param_name, _ in sig.parameters.items():
            custom_types = SpecBuilder._get_custom_types(
                hints.get(param_name, None)
            )
            types = SpecBuilder._combine_types_list(types, custom_types)
        return types

    @staticmethod
    def _get_attributes_custom_types(cls) -> list[Any]:
        types: list = []
        type_hints = get_type_hints(cls)
        for _, type_hint in type_hints.items():
            custom_types = SpecBuilder._get_custom_types(type_hint)
            types = SpecBuilder._combine_types_list(types, custom_types)
        return types

    @staticmethod
    def _get_parameters_custom_types(cls) -> list[Any]:
        types: list = []
        init_signature = inspect.signature(cls.__init__)
        hints = get_type_hints(cls.__init__)
        for param_name, param in init_signature.parameters.items():
            custom_types = SpecBuilder._get_custom_types(
                hints.get(param_name, None)
            )
            types = SpecBuilder._combine_types_list(types, custom_types)
        return types

    @staticmethod
    def _get_provider_parameters_custom_types(
        component_type: str, provider_type: str
    ) -> list[Any]:
        py_path = Loader.get_provider_path(component_type, provider_type)
        try:
            provider = Loader.load_class(py_path, Provider)
            return SpecBuilder._get_parameters_custom_types(provider)
        except Exception:
            return []

    @staticmethod
    def _get_custom_types(annotation) -> list[Any]:
        if annotation == inspect.Parameter.empty:
            return []
        types: list = []
        origin = get_origin(annotation)
        args = get_args(annotation)
        if origin and args:
            for arg in args:
                custom_types = SpecBuilder._get_custom_types(arg)
                types = SpecBuilder._combine_types_list(types, custom_types)
        if hasattr(annotation, "__name__"):
            if (
                annotation.__module__ != "builtins"
                and annotation.__module__ != "typing"
            ):
                types.append(annotation)
        return types

    @staticmethod
    def _combine_types_list(types1: list[Any], types2: list[Any]) -> list[Any]:
        for type in types2:
            if type not in types1:
                types1.append(type)
        return types1

    @staticmethod
    def _get_default_value(param) -> Any:
        default = (
            None if param.default == inspect.Parameter.empty else param.default
        )
        if isinstance(default, Enum):
            return default.value
        return default

    @staticmethod
    def _is_operation(method) -> bool:
        return callable(method) and hasattr(method, "__operation__")

    @staticmethod
    def _add_dependent_types(types: list[Any]) -> list[Any]:
        all_types = list(types)
        while True:
            new_types = []
            for type in all_types:
                attribute_types = SpecBuilder._get_attributes_custom_types(
                    type
                )
                for attr_type in attribute_types:
                    if (
                        attr_type not in all_types
                        and attr_type not in new_types
                    ):
                        new_types.append(attr_type)
            if not new_types:
                break
            all_types.extend(new_types)
        return all_types
