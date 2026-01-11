"""
Azure kubernetes deployment.
"""

import os
import re
import time
from pathlib import Path
from typing import Any, Iterable, Literal

import yaml
from kubernetes import client, config  # type: ignore
from x8.compute._common._image_helper import get_images
from x8.compute._common._models import ImageMap
from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Context, Operation, Provider, Response, RunContext

from .._models import ManifestsType


class BaseKubernetes(Provider):
    kubeconfig: str | dict[str, Any] | None
    context: str | None
    manifests: ManifestsType
    overlays: ManifestsType
    namespace: str | None
    images: list[ImageMap] | None
    containerizer: Containerizer | None
    container_registry: ContainerRegistry | None

    def __init__(
        self,
        kubeconfig: str | dict[str, Any] | None = None,
        context: str | None = None,
        manifests: ManifestsType = None,
        overlays: ManifestsType = None,
        namespace: str | None = None,
        images: list[ImageMap] | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        **kwargs: Any,
    ):
        self.kubeconfig = kubeconfig
        self.context = context
        self.manifests = manifests
        self.overlays = overlays
        self.namespace = namespace
        self.images = images
        self.containerizer = containerizer
        self.container_registry = container_registry
        super().__init__(**kwargs)

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        if not operation:
            self.__setup__(context)
            result = self.apply(run_context=self._get_run_context(context))
            return Response(result=result)
        return super().__run__(operation=operation, context=context, **kwargs)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        if not operation:
            await self.__asetup__(context)
            result = self.apply(run_context=self._get_run_context(context))
            return Response(result=result)
        return await super().__arun__(
            operation=operation, context=context, **kwargs
        )

    def _apply_provider_settings(
        self,
        objects: list[dict[str, Any]],
        image_map: dict[str, str],
    ):
        # Apply provider-specific settings to the object
        pass

    def _get_provider_kubeconfig(self) -> str | dict[str, Any] | None:
        return None

    def apply(
        self,
        manifests: ManifestsType = None,
        overlays: ManifestsType = None,
        namespace: str | None = None,
        server_side: bool = False,
        force_conflicts: bool = False,
        field_manager: str | None = None,
        dry_run: Literal["client", "server"] | None = None,
        validate: bool | Literal["strict"] | None = True,
        prune: bool = False,
        selector: str | None = None,
        prune_all: bool = False,
        prune_allowlist: list[str] | None = None,
        wait: bool = True,
        timeout: str | None = None,
        images: list[ImageMap] | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[None]:
        # Resolve inputs and load manifests/overlays
        base_manifests_input = self._get_manifests(manifests)
        overlay_manifests_input = self._get_overlays(overlays)
        kubeconfig_input = (
            self._get_kubeconfig() or self._get_provider_kubeconfig()
        )
        context_input = self._get_context()
        namespace_input = self._get_namespace(namespace)
        images_input = self._get_images(images)

        objects = self._load_manifests_input(base_manifests_input)
        overlay_objects = self._load_manifests_input(overlay_manifests_input)

        if overlay_objects:
            objects = self._apply_overlays(objects, overlay_objects)

        if not objects:
            return Response(result=None)

        image_map = dict()
        if images_input:
            image_uris = get_images(
                images=images_input,
                containerizer=self._get_containerizer(),
                container_registry=self._get_container_registry(),
                run_context=run_context,
            )
            for i, image in enumerate(images_input):
                image_map[image.name] = image_uris[i]
            self._rewrite_container_images(objects, image_map)

        self._apply_provider_settings(objects, image_map)

        # Initialize Kubernetes API client
        k8s_client = self._init_k8s_client(
            kubeconfig=kubeconfig_input, context=context_input
        )

        # Normalize and apply objects
        dry_run_opt = self._map_dry_run(dry_run)
        field_validation = self._map_field_validation(validate)
        timeout_seconds = self._parse_timeout(timeout) if wait else None

        applied_keys: set[tuple[str, str | None, str]] = set()
        for obj in objects:
            if not isinstance(obj, dict):
                continue
            # apply default namespace if provided and object is namespaced
            if namespace_input and self._is_namespaced_kind(obj):
                obj.setdefault("metadata", {}).setdefault(
                    "namespace", namespace_input
                )

            key = self._build_object_key(obj)
            applied_keys.add(key)

            if server_side:
                self._server_side_apply(
                    k8s_client,
                    obj,
                    field_manager=field_manager,
                    force=force_conflicts,
                    dry_run=dry_run_opt,
                    field_validation=field_validation,
                    default_namespace=namespace_input,
                )
            else:
                self._create_or_patch(
                    k8s_client,
                    obj,
                    dry_run=dry_run_opt,
                    field_validation=field_validation,
                    default_namespace=namespace_input,
                )

        # Optionally wait for readiness
        if wait:
            self._wait_for_objects(
                k8s_client,
                objects,
                namespace=namespace_input,
                timeout_seconds=timeout_seconds or 300,
            )

        # Optional prune based on selector
        if prune and selector:
            self._prune(
                k8s_client,
                desired_keys=applied_keys,
                selector=selector,
                namespace=namespace_input,
                prune_all=prune_all,
                prune_allowlist=prune_allowlist,
                dry_run=dry_run_opt,
            )
        return Response(result=None)

    def _get_run_context(self, context: Context | None = None) -> RunContext:
        if context and context.data:
            return context.data.pop("__run__", RunContext())
        return RunContext()

    def _get_kubeconfig(self) -> str | dict[str, Any] | None:
        if self.kubeconfig:
            return self.kubeconfig
        if self.__component__ and self.__component__.kubeconfig:
            return self.__component__.kubeconfig
        return None

    def _get_context(self) -> str | None:
        if self.context:
            return self.context
        if self.__component__ and self.__component__.context:
            return self.__component__.context
        return None

    def _get_manifests(
        self,
        manifests: ManifestsType,
    ) -> ManifestsType:
        if manifests:
            return manifests
        if self.manifests:
            return self.manifests
        if self.__component__ and self.__component__.manifests:
            return self.__component__.manifests
        return None

    def _get_overlays(
        self,
        overlays: ManifestsType,
    ) -> ManifestsType:
        if overlays:
            return overlays
        if self.overlays:
            return self.overlays
        if self.__component__ and self.__component__.overlays:
            return self.__component__.overlays
        return None

    def _get_namespace(self, namespace: str | None) -> str:
        if namespace:
            return namespace
        if self.namespace:
            return self.namespace
        if self.__component__ and self.__component__.namespace:
            return self.__component__.namespace
        return "default"

    def _get_images(self, images: list[ImageMap] | None) -> list[ImageMap]:
        if images:
            return images
        if self.images:
            return self.images
        if self.__component__ and self.__component__.images:
            return self.__component__.images
        return []

    def _get_containerizer(self) -> Containerizer | None:
        if self.containerizer:
            return self.containerizer
        if self.__component__ and self.__component__.containerizer:
            return self.__component__.containerizer
        return Containerizer(__provider__="default")

    def _get_container_registry(self) -> ContainerRegistry | None:
        if self.container_registry:
            return self.container_registry
        if self.__component__ and self.__component__.container_registry:
            return self.__component__.container_registry
        return ContainerRegistry(__provider__="default")

    def _init_k8s_client(
        self, kubeconfig: str | dict[str, Any] | None, context: str | None
    ) -> Any:
        """
        Initialize Kubernetes ApiClient using kubeconfig path or dict.
        Falls back to default config if none provided.
        """
        if isinstance(kubeconfig, dict):
            # load from dict if available
            try:
                # load_kube_config_from_dict is available in kubernetes>=18
                config.load_kube_config_from_dict(
                    config_dict=kubeconfig, context=context
                )
            except Exception:
                # fallback: may not support context arg
                config.load_kube_config_from_dict(kubeconfig)
        elif isinstance(kubeconfig, str) and kubeconfig:
            config.load_kube_config(config_file=kubeconfig, context=context)
        else:
            # default kubeconfig (e.g., ~/.kube/config) or in-cluster
            try:
                config.load_kube_config(context=context)
            except Exception:
                config.load_incluster_config()

        return client.ApiClient()

    def _load_manifests_input(
        self, manifests: ManifestsType
    ) -> list[dict[str, Any]]:
        """
        Load manifests from str path (file or dir), list of paths,
        dict, or list of dicts.
        """

        if manifests is None:
            return []

        def load_file(path: str) -> list[dict[str, Any]]:
            objs: list[dict[str, Any]] = []
            with open(path, "r", encoding="utf-8") as f:
                for doc in yaml.safe_load_all(f):
                    if isinstance(doc, dict):
                        objs.append(doc)
            return objs

        def load_dir(dir_path: str) -> list[dict[str, Any]]:
            objs: list[dict[str, Any]] = []
            for root, _, files in os.walk(dir_path):
                for name in sorted(files):
                    if name.endswith((".yml", ".yaml")):
                        objs.extend(load_file(os.path.join(root, name)))
            return objs

        if isinstance(manifests, str):
            p = Path(manifests)
            if p.is_dir():
                return load_dir(str(p))
            elif p.is_file():
                return load_file(str(p))
            else:
                # Not a valid path; treat as nothing
                return []
        if isinstance(manifests, list):
            if not manifests:
                return []
            if all(isinstance(x, str) for x in manifests):
                objs: list[dict[str, Any]] = []
                for s in manifests:
                    objs.extend(self._load_manifests_input(s))
                return objs
            if all(isinstance(x, dict) for x in manifests):
                return [x for x in manifests if isinstance(x, dict)]
            # Mixed or unsupported list
            return []
        if isinstance(manifests, dict):
            return [manifests]

        return []

    def _build_object_key(
        self, obj: dict[str, Any]
    ) -> tuple[str, str | None, str]:
        """Key to identify a K8s object: (gvk, namespace, name)."""
        api_version = obj.get("apiVersion", "")
        kind = obj.get("kind", "")
        meta = obj.get("metadata", {}) or {}
        name = meta.get("name", "")
        namespace = meta.get("namespace")
        return (f"{api_version}|{kind}", namespace, name)

    def _deep_merge(
        self, base: dict[str, Any], overlay: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Deep-merge overlay into base. Lists are replaced;
        dicts merged recursively.
        """
        for k, v in overlay.items():
            if v is None:
                # mimic kustomize behavior: explicit null removes field
                base.pop(k, None)
                continue
            if k not in base:
                base[k] = v
            else:
                if isinstance(base[k], dict) and isinstance(v, dict):
                    base[k] = self._deep_merge(base[k], v)
                else:
                    base[k] = v
        return base

    def _apply_overlays(
        self,
        base_objs: list[dict[str, Any]],
        overlay_objs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not overlay_objs:
            return base_objs
        base_map: dict[tuple[str, str | None, str], dict[str, Any]] = {}
        for o in base_objs:
            if isinstance(o, dict):
                base_map[self._build_object_key(o)] = o
        for ovr in overlay_objs:
            key = self._build_object_key(ovr)
            if key in base_map:
                base_map[key] = self._deep_merge(base_map[key], ovr)
            else:
                base_map[key] = ovr
        # preserve original order as best-effort: base first, then new overlays
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str | None, str]] = set()
        for obj in base_objs:
            k = self._build_object_key(obj)
            merged.append(base_map[k])
            seen.add(k)
        for k, v in base_map.items():
            if k not in seen:
                merged.append(v)
        return merged

    def _is_namespaced_kind(self, obj: dict[str, Any]) -> bool:
        # Heuristic: most core resources are namespaced except a few
        kind = (obj.get("kind") or "").lower()
        non_ns = {
            "namespace",
            "node",
            "persistentvolume",
            "clusterrole",
            "clusterrolebinding",
            "customresourcedefinition",
            "storageclass",
            "priorityclass",
            "mutatingwebhookconfiguration",
            "validatingwebhookconfiguration",
            "apiservice",
            "clusterissuer",
            "clusterpolicy",
        }
        return kind not in non_ns

    def _create_or_patch(
        self,
        api_client: Any,
        obj: dict[str, Any],
        dry_run: str | None,
        field_validation: str | None,
        default_namespace: str | None = None,
    ) -> None:
        # Determine API and method
        k8s_api, operation_kind, kwargs = self._resolve_api_and_kwargs(
            api_client, obj, default_namespace=default_namespace
        )

        # Try create, then patch if exists
        try:
            create_fn = getattr(k8s_api, f"create_{operation_kind}")
            create_kwargs: dict[str, Any] = {}
            if operation_kind.startswith("namespaced_"):
                create_kwargs["namespace"] = kwargs.get("namespace")
            if dry_run:
                create_kwargs["dry_run"] = dry_run
            if field_validation:
                create_kwargs["field_validation"] = field_validation
            create_fn(body=obj, **create_kwargs)
        except client.exceptions.ApiException as e:
            if e.status != 409:
                raise
            # Already exists -> patch
            patch_fn = getattr(k8s_api, f"patch_{operation_kind}")
            patch_kwargs = dict(kwargs)
            if dry_run:
                patch_kwargs["dry_run"] = dry_run
            if field_validation:
                patch_kwargs["field_validation"] = field_validation
            # name required for namespaced/global patch
            patch_fn(body=obj, **patch_kwargs)

    def _server_side_apply(
        self,
        api_client: Any,
        obj: dict[str, Any],
        field_manager: str | None,
        force: bool,
        dry_run: str | None,
        field_validation: str | None,
        default_namespace: str | None = None,
    ) -> None:
        k8s_api, operation_kind, kwargs = self._resolve_api_and_kwargs(
            api_client, obj, default_namespace=default_namespace
        )

        patch_fn = getattr(k8s_api, f"patch_{operation_kind}")
        params = dict(kwargs)
        if dry_run:
            params["dry_run"] = dry_run
        if field_manager:
            params["field_manager"] = field_manager
        if force:
            params["force"] = True
        if field_validation:
            params["field_validation"] = field_validation

        # Content type for Server-Side Apply
        params["_content_type"] = "application/apply-patch+yaml"
        patch_fn(body=obj, **params)

    def _resolve_api_and_kwargs(
        self,
        api_client: Any,
        obj: dict[str, Any],
        default_namespace: str | None = None,
    ) -> tuple[Any, str, dict[str, Any]]:
        """
        Return (api, operation_suffix, kwargs)
        where operation_suffix is like 'namespaced_deployment' or
        'deployment'. kwargs includes namespace/name for namespaced
        operations and name for cluster-scoped.
        """

        kind = obj.get("kind", "")
        group, _, version = (obj.get("apiVersion", "") or "").partition("/")
        if version == "":
            version = group
            group = "core"
        group = "".join(group.rsplit(".k8s.io", 1))
        group = "".join(word.capitalize() for word in group.split("."))
        api_name = f"{group}{version.capitalize()}Api"
        k8s_api = getattr(client, api_name)(api_client)

        # snake_case kind

        def to_snake(s: str) -> str:
            s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
            s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)
            return s.lower()

        skind = to_snake(kind)
        metadata = obj.get("metadata", {}) or {}
        name = metadata.get("name")
        namespace = metadata.get("namespace")

        namespaced_method = f"create_namespaced_{skind}"
        # methods used for probing existence
        # (not referenced directly to avoid flake warnings)

        kwargs: dict[str, Any] = {}
        # Determine if namespaced by probing for method existence
        if hasattr(k8s_api, namespaced_method):
            # namespaced
            kwargs["namespace"] = namespace or default_namespace or "default"
            if name:
                kwargs["name"] = name
            operation_kind = f"namespaced_{skind}"
        else:
            # cluster scoped
            if name:
                kwargs["name"] = name
            operation_kind = skind
        return k8s_api, operation_kind, kwargs

    def _map_dry_run(
        self, dry_run: Literal["client", "server"] | None
    ) -> str | None:
        # Kubernetes API uses dry_run="All" for both client/server intent;
        # it always happens server-side.
        return "All" if dry_run else None

    def _map_field_validation(
        self, validate: bool | Literal["strict"] | None
    ) -> str | None:
        if validate is True or validate == "strict":
            return "Strict"
        if validate is False or validate is None:
            return None
        return None

    def _parse_timeout(self, timeout: str | None) -> int:
        if not timeout:
            return 300
        # parse formats like 30s, 2m, 1h
        m = re.match(r"^(\d+)([smh]?)$", timeout.strip())
        if not m:
            return 300
        value = int(m.group(1))
        unit = m.group(2)
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        return value

    def _wait_for_objects(
        self,
        api_client: Any,
        objects: list[dict[str, Any]],
        namespace: str | None = None,
        timeout_seconds: int = 300,
    ) -> None:
        start = time.time()
        for obj in objects:
            kind = (obj.get("kind") or "").lower()
            meta = obj.get("metadata", {}) or {}
            name = meta.get("name")
            namespace = meta.get("namespace") or namespace

            if kind in {
                "deployment",
                "statefulset",
                "daemonset",
                "job",
                "cronjob",
            }:
                if isinstance(name, str) and name:
                    self._wait_kind(
                        api_client,
                        kind,
                        name,
                        namespace,
                        timeout_seconds,
                        start,
                    )

    def _wait_kind(
        self,
        api_client: Any,
        kind: str,
        name: str,
        namespace: str | None,
        timeout_seconds: int,
        start_time: float,
    ) -> None:
        def elapsed() -> float:
            return time.time() - start_time

        if kind == "deployment":
            api = client.AppsV1Api(api_client)
            while elapsed() < timeout_seconds:
                # type: ignore[arg-type]
                dep = api.read_namespaced_deployment(name, namespace)
                spec_repl = (dep.spec.replicas or 1) if dep and dep.spec else 1
                avail = (
                    dep.status.available_replicas
                    if (
                        dep
                        and dep.status
                        and dep.status.available_replicas is not None
                    )
                    else 0
                )
                if avail >= spec_repl:
                    return
                time.sleep(2)
        elif kind == "statefulset":
            api = client.AppsV1Api(api_client)
            while elapsed() < timeout_seconds:
                # type: ignore[arg-type]
                st = api.read_namespaced_stateful_set(name, namespace)
                spec_repl = (st.spec.replicas or 1) if st and st.spec else 1
                ready = (
                    st.status.ready_replicas
                    if (
                        st
                        and st.status
                        and st.status.ready_replicas is not None
                    )
                    else 0
                )
                if ready >= spec_repl:
                    return
                time.sleep(2)
        elif kind == "daemonset":
            api = client.AppsV1Api(api_client)
            while elapsed() < timeout_seconds:
                # type: ignore[arg-type]
                ds = api.read_namespaced_daemon_set(name, namespace)
                desired = (
                    ds.status.desired_number_scheduled
                    if (
                        ds
                        and ds.status
                        and ds.status.desired_number_scheduled is not None
                    )
                    else 0
                )
                ready = (
                    ds.status.number_ready
                    if ds and ds.status and ds.status.number_ready is not None
                    else 0
                )
                if desired > 0 and ready >= desired:
                    return
                time.sleep(2)
        elif kind == "job":
            batch = client.BatchV1Api(api_client)
            while elapsed() < timeout_seconds:
                jb = batch.read_namespaced_job(name, namespace)
                succeeded = (
                    jb.status.succeeded
                    if jb and jb.status and jb.status.succeeded is not None
                    else 0
                )
                if succeeded and succeeded > 0:
                    return
                time.sleep(2)
        elif kind == "cronjob":
            batch = client.BatchV1Api(api_client)
            while elapsed() < timeout_seconds:
                cj = batch.read_namespaced_cron_job(name, namespace)
                # Consider cronjob "ready" if at least one job
                # was scheduled and none active
                last_schedule = (
                    cj.status.last_schedule_time if cj and cj.status else None
                )
                active = (
                    len(cj.status.active)
                    if cj and cj.status and cj.status.active
                    else 0
                )
                if last_schedule and active == 0:
                    return
                time.sleep(2)

    def _prune(
        self,
        api_client: Any,
        desired_keys: set[tuple[str, str | None, str]],
        selector: str,
        namespace: str | None,
        prune_all: bool,
        prune_allowlist: list[str] | None,
        dry_run: str | None,
    ) -> None:
        # Build list of kinds to consider:
        # either from allowlist or from desired set
        kinds_in_desired = {k.split("|")[1] for (k, _, _) in desired_keys}
        consider_kinds = (
            set(prune_allowlist) if prune_allowlist else kinds_in_desired
        )

        # Map kind to list and delete APIs
        api_map: list[tuple[str, Any, Any]] = []
        core = client.CoreV1Api(api_client)
        apps = client.AppsV1Api(api_client)
        batch = client.BatchV1Api(api_client)
        # Only a subset for safety
        api_map.extend(
            [
                ("Pod", core.list_namespaced_pod, core.delete_namespaced_pod),
                (
                    "Service",
                    core.list_namespaced_service,
                    core.delete_namespaced_service,
                ),
                (
                    "ConfigMap",
                    core.list_namespaced_config_map,
                    core.delete_namespaced_config_map,
                ),
                (
                    "Secret",
                    core.list_namespaced_secret,
                    core.delete_namespaced_secret,
                ),
                (
                    "PersistentVolumeClaim",
                    core.list_namespaced_persistent_volume_claim,
                    core.delete_namespaced_persistent_volume_claim,
                ),
                (
                    "Deployment",
                    apps.list_namespaced_deployment,
                    apps.delete_namespaced_deployment,
                ),
                (
                    "StatefulSet",
                    apps.list_namespaced_stateful_set,
                    apps.delete_namespaced_stateful_set,
                ),
                (
                    "DaemonSet",
                    apps.list_namespaced_daemon_set,
                    apps.delete_namespaced_daemon_set,
                ),
                (
                    "Job",
                    batch.list_namespaced_job,
                    batch.delete_namespaced_job,
                ),
            ]
        )

        for kind, list_fn, delete_fn in api_map:
            if kind not in consider_kinds:
                continue
            if namespace is None:
                # Require namespace for namespaced prune to avoid broad deletes
                continue
            objs = list_fn(namespace=namespace, label_selector=selector)
            for item in objs.items or []:
                gvk = self._infer_gvk_from_obj(item)
                name = item.metadata.name
                key = (gvk, namespace, name)
                if key not in desired_keys:
                    params: dict[str, Any] = {}
                    if dry_run:
                        params["dry_run"] = dry_run
                    try:
                        delete_fn(name=name, namespace=namespace, **params)
                    except client.exceptions.ApiException as e:
                        if e.status == 404:
                            continue
                        else:
                            raise

    def _infer_gvk_from_obj(self, item: Any) -> str:
        api_version = getattr(item, "api_version", None) or getattr(
            item, "apiVersion", ""
        )
        kind = getattr(item, "kind", "")
        return f"{api_version}|{kind}"

    # --- Helpers to rewrite image uris ---
    def _iter_podspecs(self, obj: dict[str, Any]) -> Iterable[dict[str, Any]]:
        """Yield every PodSpec dict contained in a single K8s object."""
        kind = (obj.get("kind") or "").lower()
        spec = obj.get("spec") or {}

        if kind == "pod":
            if spec:
                yield spec
            return

        if kind in {
            "deployment",
            "replicaset",
            "replicationcontroller",
            "statefulset",
            "daemonset",
            "job",
        }:
            tmpl = spec.get("template") or {}
            ps = tmpl.get("spec") or {}
            if ps:
                yield ps
            return

        if kind == "cronjob":
            jt = spec.get("jobTemplate") or {}
            jts = jt.get("spec") or {}
            tmpl = jts.get("template") or {}
            ps = tmpl.get("spec") or {}
            if ps:
                yield ps
            return
        # Other kinds: ignore

    def _rewrite_container_images_on_obj(
        self, obj: dict[str, Any], images_map: dict[str, str]
    ) -> None:
        """Rewrite images on a single object in place."""
        for ps in self._iter_podspecs(obj):
            for section in (
                "containers",
                "initContainers",
                "ephemeralContainers",
            ):
                arr = ps.get(section)
                if not isinstance(arr, list):
                    continue
                for c in arr:
                    if not isinstance(c, dict):
                        continue
                    cname = c.get("name")
                    cur = c.get("image")
                    if not isinstance(cname, str) or not isinstance(cur, str):
                        continue
                    if cname in images_map:
                        new_img = images_map[cname]
                        c["image"] = new_img
                        c["imagePullPolicy"] = "IfNotPresent"

    def _rewrite_container_images(
        self, objects: list[dict[str, Any]], images_map: dict[str, str]
    ) -> None:
        """
        Rewrite images across a list of objects.
        """
        for obj in objects:
            if (
                isinstance(obj, dict)
                and obj.get("kind") == "List"
                and isinstance(obj.get("items"), list)
            ):
                for item in obj["items"]:
                    if isinstance(item, dict):
                        self._rewrite_container_images_on_obj(item, images_map)
            elif isinstance(obj, dict):
                self._rewrite_container_images_on_obj(obj, images_map)
