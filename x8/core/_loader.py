from __future__ import annotations

import copy
import importlib
import importlib.util
import inspect
import os
import re
import sys
from enum import Enum
from typing import Any

from ._component import Component, Provider
from ._log_helper import warn
from ._operation import Operation
from ._type_converter import TypeConverter
from .constants import ROOT_PACKAGE_NAME
from .dependency import (
    ComponentEdge,
    ComponentNode,
    ProviderEdge,
    ProviderNode,
)
from .exceptions import LoadError
from .manifest import MANIFEST_FILE, Manifest


class Loader:
    path: str
    manifest_path: str

    manifest: Manifest

    _init = False
    _component_dependencies: dict[str, ComponentDep] = dict()
    _provider_dependencies: dict[str, ProviderDep] = dict()

    def __init__(
        self,
        path: str = ".",
        manifest: str = MANIFEST_FILE,
    ):
        self.path = path
        self.manifest_path = os.path.join(
            path,
            manifest,
        )
        self.manifest = Manifest.parse(path=self.manifest_path)
        abs_project_folder = os.path.abspath(path)
        if abs_project_folder not in sys.path:
            sys.path.append(abs_project_folder)

    def get_component_type(self, handle: str) -> str:
        if handle in self.manifest.components:
            return self.manifest.components[handle].type
        raise LoadError("Unresolved handle")

    def load_component(
        self,
        handle: str,
        tag: str | None = None,
    ) -> Component:
        component = self._resolve_component(
            handle=handle,
            tag=tag,
        ).component
        return component

    def _resolve_component(
        self,
        handle: str,
        tag: str | None,
    ) -> ComponentDep:
        if handle not in self.manifest.components:
            raise LoadError(f"Component {handle} not found in manifest")
        cconfig = self.manifest.components[handle]
        if handle in self._component_dependencies:
            if self._component_dependencies[handle].resolved:
                return self._component_dependencies[handle]
            cdep = self._component_dependencies[handle]
        else:
            cdep = self._prepare_component_dependency(
                handle,
                cconfig.type,
                cconfig.parameters,
                cconfig.requirements,
            )
            self._component_dependencies[handle] = cdep
        cdep.parameters = self._resolve_param(
            cdep.parameters,
            tag,
        )
        if cconfig.providers:
            phandle = self._resolve_provider_handle(
                chandle=handle,
                tag=tag,
            )
            pdep = self._resolve_provider(
                chandle=handle,
                phandle=phandle,
                tag=tag,
            )
            cdep.provider = pdep
            provider = pdep.provider
        else:
            provider = None
        cdep.component = self._init_component(cdep, provider)
        cdep.component.__handle__ = handle
        cdep.component.__type__ = cdep.type
        cdep.resolved = True
        return cdep

    def _resolve_provider_handle(
        self,
        chandle: str,
        tag: str | None = None,
    ) -> str:
        if chandle not in self.manifest.components:
            raise LoadError(f"Component {chandle} not found in manifest")
        cconfig = self.manifest.components[chandle]
        if tag and tag in self.manifest.bindings:
            bindings = self.manifest.bindings[tag]
            if chandle in bindings:
                phandle = bindings[chandle]
                if phandle not in cconfig.providers:
                    raise LoadError(
                        (
                            f"Provider handle {phandle} not found in "
                            f"providers for {chandle}"
                        )
                    )
                return phandle
        return next(iter(cconfig.providers.keys()))

    def _resolve_provider(
        self,
        chandle: str,
        phandle: str,
        tag: str | None = None,
    ) -> ProviderDep:
        if chandle not in self.manifest.components:
            raise LoadError(f"Component {chandle} not found in manifest")
        if phandle not in self.manifest.components[chandle].providers:
            raise LoadError(
                f"Provider {phandle} not found in component " f"{chandle}"
            )
        handle = f"{chandle}:{phandle}"
        if handle in self._provider_dependencies:
            if self._provider_dependencies[handle].resolved:
                return self._provider_dependencies[handle]
            pdep = self._provider_dependencies[handle]
        else:
            cconfig = self.manifest.components[chandle]
            pconfig = cconfig.providers[phandle]
            pdep = self._prepare_provider_dependency(
                handle=phandle,
                type=pconfig.type,
                parameters=pconfig.parameters,
                requirements=pconfig.requirements,
                component_type=cconfig.type,
            )
            self._provider_dependencies[handle] = pdep
        pdep.parameters = self._resolve_param(
            pdep.parameters,
            tag,
        )
        pdep.provider = self._init_provider(pdep)
        pdep.provider.__handle__ = pdep.handle
        pdep.provider.__type__ = pdep.type
        pdep.resolved = True
        return pdep

    def _resolve_param(self, value: Any, tag: str | None) -> Any:
        if isinstance(value, dict):
            for k, v in value.items():
                value[k] = self._resolve_param(value=v, tag=tag)
            return value
        elif isinstance(value, list):
            for i, item in enumerate(value):
                value[i] = self._resolve_param(value=item, tag=tag)
            return value
        elif isinstance(value, str) and self._is_ref(value):
            ref_info = self._get_ref_info(value, tag)
            return self._resolve_ref(ref_info=ref_info, tag=tag)
        return value

    def _init_component(
        self,
        cdep: ComponentDep,
        provider: Provider | None,
    ) -> Component:
        path = cdep.py_path
        component = Loader.load_component_instance(
            path=path,
            parameters=cdep.parameters,
            provider=provider,
        )
        return component

    def _init_provider(self, pdep: ProviderDep) -> Provider:
        path = pdep.py_path
        provider = Loader.load_provider_instance(
            path=path,
            parameters=pdep.parameters,
        )
        return provider

    def _prepare_component_dependency(
        self,
        handle: str,
        type: str,
        parameters: dict[str, Any],
        requirements: list[str],
    ) -> ComponentDep:
        cdep = ComponentDep()
        cdep.handle = handle
        cdep.type = type
        cdep.parameters = copy.deepcopy(parameters)
        cdep.requirements = requirements
        cdep.py_path = Loader.get_component_path(component_type=type)
        cdep.kind = Loader.get_component_kind(component_type=type)
        cdep.resolved = False
        return cdep

    def _prepare_provider_dependency(
        self,
        handle: str,
        type: str,
        parameters: dict[str, Any],
        requirements: list[str],
        component_type: str,
    ) -> ProviderDep:
        pdep = ProviderDep()
        pdep.handle = handle
        pdep.component_type = type
        pdep.type = type
        pdep.parameters = copy.deepcopy(parameters)
        pdep.requirements = requirements
        pdep.py_path = Loader.get_provider_path(
            component_type=component_type,
            provider_type=type,
        )
        pdep.kind = Loader.get_provider_kind(
            component_type=component_type,
            provider_type=type,
        )
        pdep.resolved = False
        return pdep

    def _resolve_ref(self, ref_info: RefInfo, tag: str | None) -> Any:
        if ref_info.type == RefType.CONTEXT:
            return self._resolve_context(
                param=ref_info.param,
                tag=tag,
            )
        if ref_info.type == RefType.ENV:
            return self._resolve_param(
                value=self._resolve_env(ref_info.param),
                tag=tag,
            )
        if ref_info.type == RefType.VARIABLE:
            return self._resolve_param(
                value=self._resolve_variable(ref_info.param),
                tag=tag,
            )
        if ref_info.type == RefType.METADATA:
            return self._resolve_metadata(
                param=ref_info.param,
            )
        if ref_info.type == RefType.COMPONENT:
            return self._resolve_component(
                handle=ref_info.component_handle,
                tag=tag,
            ).component
        elif ref_info.type == RefType.COMPONENT_GET:
            return self._resolve_component_get(
                self._resolve_component(
                    handle=ref_info.component_handle,
                    tag=tag,
                ).component,
                ref_info.get_args,
            )
        elif ref_info.type == RefType.COMPONENT_PARAM:
            return self._resolve_component_param(
                self._resolve_component(
                    handle=ref_info.component_handle,
                    tag=tag,
                ).component,
                ref_info.param,
            )
        elif ref_info.type == RefType.PROVIDER:
            return self._resolve_provider(
                chandle=ref_info.component_handle,
                phandle=ref_info.provider_handle,
                tag=tag,
            ).provider
        elif ref_info.type == RefType.PROVIDER_GET:
            return self._resolve_provider_get(
                self._resolve_provider(
                    chandle=ref_info.component_handle,
                    phandle=ref_info.provider_handle,
                    tag=tag,
                ).provider,
                ref_info.get_args,
            )
        elif ref_info.type == RefType.PROVIDER_PARAM:
            return self._resolve_provider_param(
                self._resolve_provider(
                    chandle=ref_info.component_handle,
                    phandle=ref_info.provider_handle,
                    tag=tag,
                ).provider,
                ref_info.param,
            )

    def _resolve_context(self, param: str, tag: str | None) -> Any:
        context = {"tag": tag}
        if param in context:
            return context[param]
        return None

    def _resolve_env(self, param: str) -> Any:
        value = os.getenv(param)
        return value

    def _resolve_variable(self, param: str) -> Any:
        if param in self.manifest.variables:
            return self.manifest.variables[param]
        return None

    def _resolve_metadata(self, param: str) -> Any:
        if param in self.manifest.metadata.__dict__:
            return self.manifest.metadata.__dict__[param]
        return None

    def _resolve_component_param(
        self, component: Component, param: str
    ) -> Any:
        return getattr(component, param, None)

    def _resolve_provider_param(self, provider: Provider, param: str) -> Any:
        return getattr(provider, param, None)

    def _resolve_component_get(self, component: Component, get_args: dict):
        res = component.__run__(operation=Operation(name="get", args=get_args))
        return res.result.value

    def _resolve_provider_get(self, provider: Provider, get_args: dict):
        res = provider.__run__(operation=Operation(name="get", args=get_args))
        return res.result.value

    def _resolve_selector(
        self,
        component_handle: str,
        selector: str,
        tag: str | None,
    ) -> str:
        if component_handle not in self.manifest.components:
            raise LoadError(f"Component handle {component_handle} not found")
        binding_key = f"{component_handle}.{selector}"
        if self.manifest.bindings:
            if tag:
                if tag not in self.manifest.bindings:
                    raise LoadError(f"Tag {tag} not found in bindings")
                bindings = self.manifest.bindings[tag]
            else:
                for key, value in self.manifest.bindings.items():
                    bindings = value
                    warn(
                        "Warning: No tag provided. "
                        f"Selecting the first set of bindings {key}."
                    )
                    break
            if binding_key in bindings:
                return bindings[binding_key]
        selected_provider = next(
            iter(self.manifest.components[component_handle].providers.keys())
        )
        warn(
            f"Warning: Selector binding {binding_key} not found for "
            f"{component_handle}. Using provider {selected_provider}."
        )
        return selected_provider

    def _get_ref_info(self, string: str, tag: str | None) -> RefInfo:
        def parse_get_args(key: str) -> dict:
            parts = key.split(".")
            return (
                {"key": {"label": parts[0], "id": parts[1]}}
                if len(parts) == 2
                else {"key": {"id": key}}
            )

        def split_and_clean(value: str) -> tuple[str, str]:
            return value.split("[")[0], value.split("[")[1].split("]")[
                0
            ].replace("'", "")

        match = re.match(r"^\$\{(.+)\}$", string)
        if not match:
            raise LoadError(f"{string} not a ref")

        value = match.group(1)
        ref_info = RefInfo()
        ref_info.value = value
        prefix_map = {
            "env.": RefType.ENV,
            "context.": RefType.CONTEXT,
            "metadata.": RefType.METADATA,
            "variables.": RefType.VARIABLE,
        }

        for prefix, ref_type in prefix_map.items():
            if value.startswith(prefix):
                ref_info.type = ref_type
                start = len(prefix)
                ref_info.param = value[start:]
                return ref_info

        if "[" in value and "]" in value:
            base, key = split_and_clean(value)
            ref_info.get_args = parse_get_args(key)

            if "::" in base:
                ref_info.type = RefType.PROVIDER_GET
                comp, sel = base.split("::")
                ref_info.component_handle = comp
                ref_info.provider_handle = self._resolve_selector(
                    comp, sel, tag
                )
            elif ":" in base:
                ref_info.type = RefType.PROVIDER_GET
                ref_info.component_handle, ref_info.provider_handle = (
                    base.split(":")
                )
            else:
                ref_info.type = RefType.COMPONENT_GET
                ref_info.component_handle = base
            return ref_info

        if "::" in value:
            comp, rest = value.split("::")
            ref_info.component_handle = comp
            if "." in rest:
                ref_info.type = RefType.PROVIDER_PARAM
                sel, param = rest.split(".")
                ref_info.provider_handle = self._resolve_selector(
                    comp, sel, tag
                )
                ref_info.param = param
            else:
                ref_info.type = RefType.PROVIDER
                ref_info.provider_handle = self._resolve_selector(
                    comp, rest, tag
                )
            return ref_info

        if ":" in value:
            comp, rest = value.split(":")
            ref_info.component_handle = comp
            if "." in rest:
                ref_info.type = RefType.PROVIDER_PARAM
                ref_info.provider_handle, ref_info.param = rest.split(".")
            else:
                ref_info.type = RefType.PROVIDER
                ref_info.provider_handle = rest
            return ref_info

        if "." in value:
            ref_info.type = RefType.COMPONENT_PARAM
            ref_info.component_handle, ref_info.param = value.split(".")
            return ref_info

        ref_info.type = RefType.COMPONENT
        ref_info.component_handle = value
        return ref_info

    def _is_ref(self, str: str) -> bool:
        pattern = r"^\$\{(.+)\}$"
        return bool(re.match(pattern, str))

    def _get_composite_component_provider(
        self,
        type: str,
    ) -> tuple[str, str]:
        _, package = type.split(".", 1)
        if "." in package:
            component, provider = package.split(".", 1)
        else:
            component = package
            provider = "default"
        return component, provider

    def generate_requirements(
        self,
        handle: str,
        tag: str | None = None,
        out: str | None = None,
    ) -> list[str]:
        base_requirements: dict[str, list[str]] = {
            ROOT_PACKAGE_NAME: [],
        }
        custom_requirements: set[str] = set()

        def traverse_provider(
            cnode: ComponentNode, pnode: ProviderNode | None
        ):
            if pnode is None:
                return
            if pnode.requirements:
                custom_requirements.update(pnode.requirements)
            else:
                if pnode.kind == "composite":
                    package = ROOT_PACKAGE_NAME
                    namespace = "composite"
                    component, provider = (
                        self._get_composite_component_provider(pnode.type)
                    )
                elif pnode.kind == "base":
                    type = cnode.type.replace(f"{ROOT_PACKAGE_NAME}.", "")
                    namespace, cpackage = type.split(".", 1)
                    package = ROOT_PACKAGE_NAME
                    component = cpackage
                    provider = pnode.type
                else:
                    package = None
                if package:
                    extra = (
                        namespace.replace("_", "-")
                        + "-"
                        + component.replace("_", "-")
                        + "-"
                        + provider.replace("_", "-")
                    )
                    if package not in base_requirements:
                        base_requirements[package] = []
                    if extra not in base_requirements[package]:
                        base_requirements[package].append(extra)

            for cchild in pnode.depends:
                traverse_component(cchild.component)
            for pchild in pnode.composes:
                traverse_provider(cnode=cnode, pnode=pchild.provider)

        def traverse_component(cnode: ComponentNode):
            custom_requirements.update(cnode.requirements)
            traverse_provider(cnode=cnode, pnode=cnode.provider)
            for child in cnode.depends:
                traverse_component(child.component)

        custom_requirements.update(self.manifest.requirements)
        graph = self.get_dependency_graph(handle=handle, tag=tag)
        traverse_component(graph)

        reqs: list[str] = []
        for package in base_requirements:
            if len(base_requirements[package]) > 0:
                reqs.append(
                    package
                    + "["
                    + str.join(",", base_requirements[package])
                    + "]"
                )
            else:
                reqs.append(package)
        if len(custom_requirements) > 0:
            for req in custom_requirements:
                reqs.append(req)
        if out is not None:
            with open(out, "w") as file:
                file.write("\n".join(reqs))
        return reqs

    def get_dependency_graph(
        self,
        handle: str,
        tag: str | None = None,
    ) -> ComponentNode:
        component_node_cache: dict[str, ComponentNode] = {}
        provider_node_cache: dict[str, ProviderNode] = {}

        def _get_component_node(cdep: ComponentDep) -> ComponentNode:
            if cdep.handle in component_node_cache:
                return component_node_cache[cdep.handle]
            node = ComponentNode(
                handle=cdep.handle,
                type=cdep.type,
                kind=cdep.kind,
                requirements=cdep.requirements,
            )
            if cdep.provider:
                node.provider = _get_provider_node(
                    cdep=cdep, pdep=cdep.provider
                )
            component_dependencies = _get_component_dependencies_from_param(
                value=cdep.parameters, tag=tag
            )
            for cdep, ref_info in component_dependencies:
                node.depends.append(
                    ComponentEdge(
                        component=_get_component_node(cdep),
                        ref=ref_info.value,
                    )
                )
            component_node_cache[cdep.handle] = node
            return node

        def _get_provider_node(
            cdep: ComponentDep, pdep: ProviderDep
        ) -> ProviderNode:
            handle = f"{cdep.handle}:{pdep.handle}"
            if handle in provider_node_cache:
                return provider_node_cache[handle]
            node = ProviderNode(
                handle=pdep.handle,
                type=pdep.type,
                kind=pdep.kind,
                requirements=pdep.requirements,
            )
            component_dependencies = _get_component_dependencies_from_param(
                value=pdep.parameters, tag=tag
            )
            for component_dependency, ref_info in component_dependencies:
                node.depends.append(
                    ComponentEdge(
                        component=_get_component_node(component_dependency),
                        ref=ref_info.value,
                    )
                )
            provider_dependencies = _get_provider_dependencies_from_param(
                value=pdep.parameters, tag=tag
            )
            for pdep, ref_info in provider_dependencies:
                node.composes.append(
                    ProviderEdge(
                        provider=_get_provider_node(cdep=cdep, pdep=pdep),
                        ref=ref_info.value,
                    )
                )
            provider_node_cache[handle] = node
            return node

        def _get_provider_dependencies_from_param(
            value: Any,
            tag: str | None,
        ) -> list[tuple[ProviderDep, RefInfo]]:
            dependencies: list[tuple[ProviderDep, RefInfo]] = []
            if isinstance(value, dict):
                for k, v in value.items():
                    dependencies.extend(
                        _get_provider_dependencies_from_param(value=v, tag=tag)
                    )
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    dependencies.extend(
                        _get_provider_dependencies_from_param(
                            value=item, tag=tag
                        )
                    )
            elif isinstance(value, str) and self._is_ref(value):
                ref_info = self._get_ref_info(value, tag)
                if ref_info.type in (
                    RefType.PROVIDER,
                    RefType.PROVIDER_PARAM,
                    RefType.PROVIDER_GET,
                ):
                    handle = (
                        f"{ref_info.component_handle}:"
                        f"{ref_info.provider_handle}"
                    )
                    if handle in self._provider_dependencies:
                        dependencies.append(
                            (self._provider_dependencies[handle], ref_info)
                        )
                    else:
                        raise LoadError(f"Unresolved provider {handle}")
            return dependencies

        def _get_component_dependencies_from_param(
            value: Any,
            tag: str | None,
        ) -> list[tuple[ComponentDep, RefInfo]]:
            dependencies: list = []
            if isinstance(value, dict):
                for k, v in value.items():
                    dependencies.extend(
                        _get_component_dependencies_from_param(
                            value=v, tag=tag
                        )
                    )
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    dependencies.extend(
                        _get_component_dependencies_from_param(
                            value=item, tag=tag
                        )
                    )
            elif isinstance(value, str) and self._is_ref(value):
                ref_info = self._get_ref_info(value, tag)
                if ref_info.type in (
                    RefType.COMPONENT,
                    RefType.COMPONENT_PARAM,
                    RefType.COMPONENT_GET,
                ):
                    handle = ref_info.component_handle
                    if handle in self._component_dependencies:
                        dependencies.append(
                            (self._component_dependencies[handle], ref_info)
                        )
                    else:
                        raise LoadError(f"Unresolved component {handle}")
            return dependencies

        def _prepare_unresolved(
            tag: str | None = None,
        ):
            for chandle in self.manifest.components:
                cconfig = self.manifest.components[chandle]
                cdep = self._prepare_component_dependency(
                    chandle,
                    cconfig.type,
                    cconfig.parameters,
                    cconfig.requirements,
                )
                self._component_dependencies[chandle] = cdep
                if cconfig.providers:
                    phandle = self._resolve_provider_handle(
                        chandle=chandle,
                        tag=tag,
                    )
                    pconfig = cconfig.providers[phandle]
                    pdep = self._prepare_provider_dependency(
                        handle=phandle,
                        type=pconfig.type,
                        parameters=pconfig.parameters,
                        requirements=pconfig.requirements,
                        component_type=cconfig.type,
                    )
                    cdep.provider = pdep
                    self._provider_dependencies[f"{chandle}:{phandle}"] = pdep
                else:
                    cdep.provider = None

        _prepare_unresolved(tag=tag)
        node = _get_component_node(self._component_dependencies[handle])
        return node

    @staticmethod
    def load_provider_instance(
        path: str | None = None,
        parameters: dict[str, Any] = dict(),
    ) -> Provider:
        if path is None:
            return Provider(**parameters)
        provider = Loader.load_class(path, Provider)
        converted_parameters = TypeConverter.convert_args(
            provider.__init__, parameters
        )
        return provider(**converted_parameters)

    @staticmethod
    def get_component_kind(
        component_type: str,
    ) -> str:
        if component_type.startswith(f"{ROOT_PACKAGE_NAME}."):
            return "base"
        return "custom"

    @staticmethod
    def get_provider_kind(
        component_type: str,
        provider_type: str,
    ) -> str:
        if provider_type.startswith(f"{ROOT_PACKAGE_NAME}.composite."):
            return "composite"
        elif component_type.startswith(f"{ROOT_PACKAGE_NAME}."):
            return "base"
        else:
            return "custom"

    @staticmethod
    def get_component_path(
        component_type: str,
    ) -> str:
        if ":" in component_type:
            return component_type
        else:
            return f"{component_type}.component"

    @staticmethod
    def get_provider_path(
        component_type: str,
        provider_type: str,
    ) -> str:
        if ":" in provider_type or ":" in component_type:
            return provider_type
        elif provider_type.startswith(f"{ROOT_PACKAGE_NAME}."):
            if ".providers." in provider_type:
                return provider_type
            else:
                return f"{provider_type}.providers.default"
        else:
            return f"{component_type}.providers.{provider_type}"

    @staticmethod
    def load_component_instance(
        path: str | None,
        parameters: dict,
        provider: Provider | None,
    ) -> Component:
        if path is None:
            return Component(__provider__=provider, **parameters)
        component = Loader.load_class(path, Component)
        converted_parameters = TypeConverter.convert_args(
            component.__init__, parameters
        )
        return component(__provider__=provider, **converted_parameters)

    @staticmethod
    def load_class(path: str, type: Any) -> Any:
        class_name = None
        if ":" in path:
            splits = path.split(":")
            module_name = splits[0]
            class_name = splits[-1]
        else:
            module_name = path
        if module_name.endswith(".py"):
            import sys

            normalized_module_name = (
                os.path.normpath(module_name)
                .replace("\\", "/")
                .split(".py")[0]
                .replace("/", ".")
                .lstrip(".")
            )
            spec = importlib.util.spec_from_file_location(
                normalized_module_name, os.path.abspath(path)
            )
            if spec is not None:
                module = importlib.util.module_from_spec(spec)
                sys.modules[normalized_module_name] = module
                if spec.loader is not None:
                    spec.loader.exec_module(module)
            module_name = normalized_module_name
        else:
            module = importlib.import_module(module_name)
        if class_name is not None:
            return getattr(module, class_name)
        else:
            for name, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, type) and cls.__module__ == module_name:
                    return cls
        raise LoadError(f"{type.__name__} not found at {module}")


class RefInfo:
    type: RefType
    component_handle: str
    provider_handle: str
    param: str
    get_args: dict
    value: str


class RefType(str, Enum):
    ENV = "env"
    CONTEXT = "context"
    METADATA = "metadata"
    VARIABLE = "variable"
    COMPONENT = "component"
    COMPONENT_PARAM = "component_param"
    COMPONENT_GET = "component_get"
    PROVIDER = "provider"
    PROVIDER_PARAM = "provider_param"
    PROVIDER_GET = "provider_get"


class ComponentDep:
    handle: str
    type: str
    parameters: dict[str, Any]
    requirements: list[str]
    kind: str
    py_path: str
    provider: ProviderDep | None
    component: Component
    resolved: bool

    def __init__(self):
        self.parameters = dict()


class ProviderDep:
    handle: str
    component_type: str
    type: str
    parameters: dict[str, Any]
    requirements: list[str]
    kind: str
    py_path: str
    provider: Provider
    resolved: bool

    def __init__(self):
        self.parameters = dict()
