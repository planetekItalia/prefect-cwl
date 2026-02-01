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
from typing import Dict, Any, Tuple, List

from prefect_kubernetes import KubernetesJob

from prefect_cwl.planner.templates import StepPlan, ListingMaterialization, StepTemplate
from prefect_cwl.backends.base import Backend
from prefect_cwl.io import build_command_and_listing
from prefect_cwl.logger import get_logger


class K8sBackend(Backend):
    """
    Execute StepTemplate using Kubernetes Jobs with a shared PVC mounted at /data.
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
        """
        spec: "/out:ro" | "/out:rw" | "/out"
        returns: (container_path, read_only)
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
        """
        Convert PVC-absolute host path (e.g. '/data/tmp/out') to subPath ('tmp/out').
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

    # ------------------------
    # Job builders
    # ------------------------
    def _base_job_manifest(self, job_name: str, container_spec: dict) -> dict:
        """Base Job manifest with PVC mounted."""
        volume_name = "work"

        mounts = list(container_spec.get("volumeMounts", []))

        if not any(
            m.get("name") == volume_name and m.get("mountPath") == self.pvc_mount_path
            for m in mounts
        ):
            mounts.append({"name": volume_name, "mountPath": self.pvc_mount_path})

        container = {
            "name": "main",
            **container_spec,
            "volumeMounts": mounts,
        }

        pod_spec = {
            "restartPolicy": "Never",
            "containers": [container],
            "volumes": [
                {
                    "name": volume_name,
                    "persistentVolumeClaim": {"claimName": self.pvc_name},
                },
            ],
        }

        if self.service_account_name:
            pod_spec["serviceAccountName"] = self.service_account_name

        if self.image_pull_secrets:
            pod_spec["imagePullSecrets"] = [
                {"name": s} for s in self.image_pull_secrets
            ]

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name, "namespace": self.namespace},
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
        """Main execution job."""
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

        return self._base_job_manifest(job_name, container_spec=container_spec)

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
        produced: Dict[Tuple[str, str], Path],
        workspace: Path,
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
            k8s_job = KubernetesJob(namespace=self.namespace, v1_job=mkdir_manifest)
            await self._run_job(k8s_job, mkdir_job_name, logger)

        # Run listings job
        if step.listings:
            listings_manifest = self._listings_job(listings_job_name, step.listings)
            k8s_job = KubernetesJob(namespace=self.namespace, v1_job=listings_manifest)
            await self._run_job(k8s_job, mkdir_job_name, logger)

        # Run main step job
        step_manifest = self._step_job(step_job_name, step)
        k8s_job = KubernetesJob(namespace=self.namespace, v1_job=step_manifest)
        await self._run_job(k8s_job, mkdir_job_name, logger)

        # Track produced outputs for downstream steps
        for output_port, host_path in step.out_artifacts.items():
            produced[(step.step_name, output_port)] = host_path
