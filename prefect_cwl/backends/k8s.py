"""Kubernetes execution backend for Prefect CWL steps.

This module implements a Backend that materializes a step plan and runs it as
Kubernetes Jobs, managing a shared PVC for staging files and mounts.
"""

import base64
import os
import re
import shlex
import uuid
from logging import Logger
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional

from prefect_kubernetes import KubernetesJob
from prefect.context import get_run_context
from prefect.exceptions import MissingContextError

from prefect_cwl.planner.templates import (
    StepPlan,
    ListingMaterialization,
    StepTemplate,
    StepResources,
)
from prefect_cwl.planner.templates import ArtifactPath
from prefect_cwl.backends.base import Backend
from prefect_cwl.io import build_command_and_listing
from prefect_cwl.logger import get_logger


class K8sBackend(Backend):
    """Execute StepTemplate using Kubernetes Jobs with a shared PVC mounted at JOBROOT.

    Materializes listings to the PVC and create all the required directories via dedicated jobs.
    """

    def __init__(
        self,
        namespace: str = os.environ.get("PREFECT_CWL_K8S_NAMESPACE", "prefect"),
        pvc_name: str = os.environ.get(
            "PREFECT_CWL_K8S_PVC_NAME", "prefect-shared-pvc"
        ),
        pvc_mount_path: str = os.environ.get("PREFECT_CWL_K8S_PVC_MOUNT_PATH", "/data"),
        service_account_name: str = os.environ.get(
            "PREFECT_CWL_K8S_SERVICE_ACCOUNT_NAME", "prefect-flow-runner"
        ),
        image_pull_secrets: List[str] = os.environ.get(
            "PREFECT_CWL_K8S_PULL_SECRETS", None
        ),
        ttl_seconds_after_finished: int = 3600,
    ) -> None:
        """Initialize the backend.

        Args:
            namespace (str, optional): Kubernetes namespace to use. Defaults to "prefect".
            pvc_name (str, optional): Name of the PVC to use. Defaults to "prefect-shared-pvc".
            pvc_mount_path (str, optional): Path to mount the PVC at. Defaults to "/data".
            service_account_name (str, optional): Name of the service account to use. Defaults to "prefect-flow-runner".
            image_pull_secrets (List[str], optional): List of image pull secrets to use. Defaults to None.
            ttl_seconds_after_finished (int, optional): TTL for finished jobs. Defaults to 3600.
        """
        self.namespace = namespace
        self.pvc_name = pvc_name
        self.pvc_mount_path = pvc_mount_path
        self.service_account_name = service_account_name
        self.image_pull_secrets = image_pull_secrets or []
        self.ttl_seconds_after_finished = ttl_seconds_after_finished

    # ------------------------
    # Helpers
    # ------------------------
    def _parse_container_spec(self, spec: str) -> tuple[str, bool]:
        """Parse a volume spec into a container path, setting up the read-only flag.

        spec: "/out:ro" | "/out:rw" | "/out"
        returns: (container_path, read_only)

        Args:
            spec (str): Volume specification

        Returns:
            tuple[str, bool]: Container path and read-only flag
        """
        if not isinstance(spec, str) or not spec.strip():
            raise ValueError(f"Invalid volume spec: {spec!r}")

        spec = spec.strip()
        if spec.endswith(":ro"):
            return spec[:-3], True
        if spec.endswith(":rw"):
            return spec[:-3], False
        return spec, False

    def _to_subpath(self, host_path: str) -> str:
        """Convert PVC-absolute host path (e.g. '/data/tmp/out') to subPath ('tmp/out').

        Args:
            host_path (str): Absolute path on PVC.
        """
        hp = str(host_path).replace("\\", "/")
        root = self.pvc_mount_path.rstrip("/")
        if not hp.startswith(root + "/"):
            raise ValueError(f"Host path must be under PVC mount {root!r}: {hp!r}")
        return hp[len(root) + 1 :]

    def _map_stepplan_to_pvc(self, step: StepPlan) -> StepPlan:
        """Ensure all paths are under PVC mount."""
        mapped_listings = [
            ListingMaterialization(host_path=Path(x.host_path), content=x.content)
            for x in (step.listings or [])
        ]

        mapped_volumes: Dict[str, str] = {}
        for host, spec in (step.volumes or {}).items():
            mapped_volumes[host] = spec

        mapped_out = {k: Path(v) for k, v in (step.out_artifacts or {}).items()}

        return StepPlan(
            step_name=step.step_name,
            tool_id=step.tool_id,
            image=step.image,
            argv=list(step.argv),
            outdir_container=step.outdir_container,
            volumes=mapped_volumes,
            listings=mapped_listings,
            out_artifacts=mapped_out,
            envs=step.envs,
            resources=step.resources,
        )

    def _k8s_name(self, s: str, max_len: int = 63) -> str:
        """RFC1123 subdomain naming."""
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9.-]+", "-", s)
        s = re.sub(r"[-.]{2,}", "-", s)
        s = re.sub(r"^[^a-z0-9]+", "", s)
        s = re.sub(r"[^a-z0-9]+$", "", s)
        if not s:
            s = "job"
        s = s[:max_len]
        s = re.sub(r"[^a-z0-9]+$", "", s)
        return s or "job"

    def _runtime_job_variables(self) -> Dict[str, Any]:
        """Read flow-run job variables provided by Prefect worker infrastructure.

        On deployed runs, this may include values rendered from the work pool base
        job template (for example namespace, service account, env, volumes).
        """
        try:
            ctx = get_run_context()
        except MissingContextError:
            return {}

        flow_run = getattr(ctx, "flow_run", None)
        raw = getattr(flow_run, "job_variables", None)
        return raw if isinstance(raw, dict) else {}

    def _effective_namespace(self) -> str:
        runtime_vars = self._runtime_job_variables()
        namespace = runtime_vars.get("namespace")
        return str(namespace).strip() if namespace else self.namespace

    def _effective_service_account_name(self) -> Optional[str]:
        runtime_vars = self._runtime_job_variables()
        service_account_name = runtime_vars.get("service_account_name")
        if service_account_name:
            return str(service_account_name).strip()
        return self.service_account_name

    def _effective_image_pull_secrets(self) -> List[dict]:
        runtime_vars = self._runtime_job_variables()
        runtime_pull_secrets = runtime_vars.get("image_pull_secrets", [])

        merged: Dict[str, dict] = {}
        for secret_name in self.image_pull_secrets:
            if not secret_name:
                continue
            merged[str(secret_name)] = {"name": str(secret_name)}

        if isinstance(runtime_pull_secrets, list):
            for item in runtime_pull_secrets:
                if isinstance(item, str) and item:
                    merged[item] = {"name": item}
                elif isinstance(item, dict):
                    name = item.get("name")
                    if isinstance(name, str) and name:
                        merged[name] = {"name": name}
        return list(merged.values())

    def _effective_runtime_env(self) -> List[dict]:
        runtime_vars = self._runtime_job_variables()
        runtime_env = runtime_vars.get("env", {})

        env_entries: List[dict] = []
        if isinstance(runtime_env, dict):
            for key, value in runtime_env.items():
                if value is None:
                    continue
                env_entries.append({"name": str(key), "value": str(value)})
        elif isinstance(runtime_env, list):
            for item in runtime_env:
                if isinstance(item, dict) and "name" in item:
                    env_entries.append(dict(item))
        return env_entries

    def _effective_runtime_volume_mounts(self) -> List[dict]:
        runtime_vars = self._runtime_job_variables()
        mounts = runtime_vars.get("volume_mounts", runtime_vars.get("volumeMounts", []))
        if not isinstance(mounts, list):
            return []
        out: List[dict] = []
        for item in mounts:
            if isinstance(item, dict) and item.get("name") and item.get("mountPath"):
                out.append(dict(item))
        return out

    def _effective_runtime_volumes(self) -> List[dict]:
        runtime_vars = self._runtime_job_variables()
        volumes = runtime_vars.get("volumes", [])
        if not isinstance(volumes, list):
            return []
        out: List[dict] = []
        for item in volumes:
            if isinstance(item, dict) and item.get("name"):
                out.append(dict(item))
        return out

    def _merge_env(
        self, runtime_env: List[dict], container_env: List[dict]
    ) -> List[dict]:
        """Merge env vars by name, letting container values override runtime ones."""
        merged: List[dict] = []
        by_name: Dict[str, int] = {}

        for item in runtime_env + container_env:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if isinstance(name, str) and name:
                if name in by_name:
                    merged[by_name[name]] = dict(item)
                else:
                    by_name[name] = len(merged)
                    merged.append(dict(item))
            else:
                merged.append(dict(item))
        return merged

    def _merge_named(self, first: List[dict], second: List[dict]) -> List[dict]:
        """Merge lists of dicts keyed by 'name', with second list taking precedence."""
        merged: Dict[str, dict] = {}
        for item in first:
            if isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str) and name:
                    merged[name] = dict(item)
        for item in second:
            if isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str) and name:
                    merged[name] = dict(item)
        return list(merged.values())

    # ------------------------
    # Job builders
    # ------------------------
    def _base_job_manifest(self, job_name: str, container_spec: dict) -> dict:
        """Generate base Job manifest with PVC mounted. Set service account and image pull secrets as well, if provided."""
        volume_name = "work"

        runtime_mounts = self._effective_runtime_volume_mounts()
        mounts = runtime_mounts + list(container_spec.get("volumeMounts", []))
        # Ensure only our PVC-backed mount is used for the pvc root mountPath.
        mounts = [
            m
            for m in mounts
            if not (isinstance(m, dict) and m.get("mountPath") == self.pvc_mount_path)
        ]
        mounts.append({"name": volume_name, "mountPath": self.pvc_mount_path})

        runtime_env = self._effective_runtime_env()
        merged_env = self._merge_env(runtime_env, list(container_spec.get("env", [])))
        container = {
            "name": "main",
            **container_spec,
            "volumeMounts": mounts,
            "env": merged_env,
        }

        required_work_volume = {
            "name": volume_name,
            "persistentVolumeClaim": {"claimName": self.pvc_name},
        }
        runtime_volumes = self._effective_runtime_volumes()
        volumes = self._merge_named(runtime_volumes, [required_work_volume])

        pod_spec = {
            "restartPolicy": "Never",
            "containers": [container],
            "volumes": volumes,
        }

        service_account_name = self._effective_service_account_name()
        if service_account_name:
            pod_spec["serviceAccountName"] = service_account_name

        image_pull_secrets = self._effective_image_pull_secrets()
        if image_pull_secrets:
            pod_spec["imagePullSecrets"] = image_pull_secrets

        namespace = self._effective_namespace()
        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name, "namespace": namespace},
            "spec": {
                "ttlSecondsAfterFinished": self.ttl_seconds_after_finished,
                "backoffLimit": 0,
                "template": {"spec": pod_spec},
            },
        }

    def _mkdir_job(self, job_name: str, dirs: List[str]) -> dict:
        """Job that creates dirs inside the PVC."""
        cmd = [
            "sh",
            "-lc",
            "set -euo pipefail\n"
            + "\n".join(f"mkdir -p {shlex.quote(d)}" for d in dirs),
        ]
        return self._base_job_manifest(
            job_name,
            container_spec={
                "image": "busybox:1.36",
                "command": cmd,
            },
        )

    def _listings_job(
        self, job_name: str, listings: List[ListingMaterialization]
    ) -> dict:
        """Job that writes listing files into the PVC."""
        lines = ["set -euo pipefail"]
        for item in listings or []:
            dst = str(item.host_path).replace("\\", "/")
            parent = str(Path(dst).parent)
            b64 = base64.b64encode(item.content.encode("utf-8")).decode("ascii")
            lines.append(f"mkdir -p {shlex.quote(parent)}")
            lines.append(f"echo {shlex.quote(b64)} | base64 -d > {shlex.quote(dst)}")

        cmd = ["sh", "-lc", "\n".join(lines) + "\n"]
        return self._base_job_manifest(
            job_name,
            container_spec={
                "image": "busybox:1.36",
                "command": cmd,
            },
        )

    def _step_job(self, job_name: str, step: StepPlan) -> dict:
        """Generate the Kubernetes Job manifest for a step, setting up volumes, environment variables, etc.

        Args:
            job_name (str): Name for the Kubernetes Job.
            step (StepPlan): Step to run.
        """
        volume_name = "work"
        extra_mounts: List[dict] = []

        for host_path, spec in (step.volumes or {}).items():
            container_path, read_only = self._parse_container_spec(spec)

            if not container_path.startswith("/"):
                raise ValueError(
                    f"Container mount path must be absolute: {container_path!r}"
                )

            sub_path = self._to_subpath(host_path)

            extra_mounts.append(
                {
                    "name": volume_name,
                    "mountPath": container_path,
                    "subPath": sub_path,
                    "readOnly": read_only,
                }
            )

        container_spec = {
            "image": step.image,
            "command": step.argv,
            "volumeMounts": extra_mounts,
            "env": [{"name": k, "value": v} for k, v in (step.envs or {}).items()],
            "workingDir": str(step.outdir_container),
        }
        resources = self._k8s_container_resources(step.resources)
        if resources:
            container_spec["resources"] = resources

        return self._base_job_manifest(job_name, container_spec=container_spec)

    @staticmethod
    def _k8s_container_resources(
        step_resources: StepResources,
    ) -> Dict[str, Dict[str, str]]:
        """Map normalized resources into Kubernetes container resources."""
        requests: Dict[str, str] = {}
        limits: Dict[str, str] = {}

        if step_resources.cpu_request is not None:
            requests["cpu"] = str(step_resources.cpu_request)
        if step_resources.cpu_limit is not None:
            limits["cpu"] = str(step_resources.cpu_limit)
        if step_resources.memory_request_mb is not None:
            requests["memory"] = f"{int(step_resources.memory_request_mb)}Mi"
        if step_resources.memory_limit_mb is not None:
            limits["memory"] = f"{int(step_resources.memory_limit_mb)}Mi"

        out: Dict[str, Dict[str, str]] = {}
        if requests:
            out["requests"] = requests
        if limits:
            out["limits"] = limits
        return out

    async def _run_job(
        self, k8s_job: KubernetesJob, step_job_name: str, logger: Logger
    ):
        """Run a Kubernetes Job and wait for completion.

        Args:
            k8s_job (KubernetesJob): KubernetesJob to run.
            step_job_name (str): Name of the step job.
            logger (Logger): Logger to use for printing logs.
        """
        job_run = await k8s_job.trigger()
        await job_run.wait_for_completion(
            print_func=lambda line: logger.info(
                "[%s] %s", step_job_name, line.rstrip()
            ),
        )
        return await job_run.fetch_result()

    # ------------------------
    # Backend API
    # ------------------------
    async def call_single_step(
        self,
        step_template: StepTemplate,
        workflow_inputs: Dict[str, Any],
        produced: Dict[Tuple[str, str], ArtifactPath],
        workspace: Path,
        job_suffix: Optional[str] = None,
        input_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Execute step on K8s."""
        logger = get_logger("prefect-k8s")

        if step_template is None:
            raise ValueError("step_template is required")

        # MATERIALIZE the step with runtime values
        step_plan = step_template.materialize_step(
            workflow_inputs=workflow_inputs,
            produced=produced,
            workspace=workspace,
            render_io=build_command_and_listing,
            job_suffix=job_suffix,
            input_overrides=input_overrides,
        )

        # Map to PVC paths
        step = self._map_stepplan_to_pvc(step_plan)

        # Compute dirs to create
        dirs: set[str] = set()
        for host_pvc_path in (step.volumes or {}).keys():
            dirs.add(str(host_pvc_path))
        for item in step.listings or []:
            dirs.add(str(Path(item.host_path).parent))
        dirs = {d.rstrip("/") for d in dirs if d and d.startswith(self.pvc_mount_path)}

        prefix_raw = f"{step.step_name}-{uuid.uuid4().hex[:8]}"
        prefix = self._k8s_name(prefix_raw, max_len=50)

        mkdir_job_name = self._k8s_name(f"{prefix}-mkdir")
        listings_job_name = self._k8s_name(f"{prefix}-listings")
        step_job_name = self._k8s_name(f"{prefix}-run")

        logger.info("K8s step: %s image=%s", step_job_name, step.image)
        logger.info("Command: %s", shlex.join(step.argv))
        logger.info("PVC: %s mounted at %s", self.pvc_name, self.pvc_mount_path)
        logger.info("Volumes: %s", sorted(step.volumes or {}))

        # Run mkdir job
        if dirs:
            mkdir_manifest = self._mkdir_job(mkdir_job_name, sorted(dirs))
            k8s_job = KubernetesJob(
                namespace=mkdir_manifest["metadata"]["namespace"], v1_job=mkdir_manifest
            )
            await self._run_job(k8s_job, mkdir_job_name, logger)

        # Run listings job
        if step.listings:
            listings_manifest = self._listings_job(listings_job_name, step.listings)
            k8s_job = KubernetesJob(
                namespace=listings_manifest["metadata"]["namespace"],
                v1_job=listings_manifest,
            )
            await self._run_job(k8s_job, listings_job_name, logger)

        # Run main step job
        step_manifest = self._step_job(step_job_name, step)
        k8s_job = KubernetesJob(
            namespace=step_manifest["metadata"]["namespace"], v1_job=step_manifest
        )
        await self._run_job(k8s_job, step_job_name, logger)

        # Track produced outputs for downstream steps
        for output_port, host_path in step.out_artifacts.items():
            produced[(step.step_name, output_port)] = host_path
