"""Docker execution backend for Prefect CWL steps.

This module implements a Backend that materializes a step plan and runs it in
Docker, handling volume mounts, environment variables, and streamed logs.
"""
import asyncio
import os
import shlex
import uuid
from pathlib import Path
from typing import List, Dict, Any, Tuple

from prefect_docker.containers import create_docker_container, start_docker_container
from prefect_docker.images import pull_docker_image

from prefect_cwl.planner.templates import StepTemplate, ListingMaterialization
from prefect_cwl.backends.base import Backend
from prefect_cwl.exceptions import ValidationError
from prefect_cwl.io import build_command_and_listing
from prefect_cwl.logger import get_logger



class DockerBackend(Backend):
    """
    Docker backend that materializes and executes CWL steps.
    """

    # ------------------------
    # FS helpers
    # ------------------------
    @staticmethod
    def _write_listings(listings: List[ListingMaterialization]) -> None:
        """Materialize InitialWorkDir listings on the host (under step jobdir)."""
        for item in listings or []:
            p = Path(item.host_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(item.content, encoding="utf-8")

    @staticmethod
    def _ensure_mount_sources(volumes: Dict[str, str]) -> None:
        """
        Ensure host mount sources exist.
        - For directory mounts: create dir
        - For file mounts: create parent dir
        """
        for host_path in (volumes or {}).keys():
            hp = Path(host_path)
            hp.mkdir(parents=True, exist_ok=True)

    # ------------------------
    # Mount formatting
    # ------------------------
    @staticmethod
    def _volumes_to_prefect_docker(volumes: Dict[str, str]) -> List[str]:
        """
        Convert host->"container:mode" into ["host:container:mode", ...].

        Example:
          {"/tmp/out": "/out:rw", "/tmp/in": "/cwl_job/inputs/x:ro"}
        """
        out: List[str] = []
        for host, container_spec in (volumes or {}).items():
            if not isinstance(container_spec, str):
                raise ValidationError(f"Invalid volume spec for {host!r}: {container_spec!r}")

            if container_spec.endswith(":ro"):
                container_path = container_spec[:-3]
                mode = "ro"
            elif container_spec.endswith(":rw"):
                container_path = container_spec[:-3]
                mode = "rw"
            else:
                container_path = container_spec
                mode = "rw"

            if not container_path.startswith("/"):
                raise ValidationError(f"Container mount path must be absolute: {container_path!r}")

            out.append(f"{host}:{container_path}:{mode}")
        return out

    # ------------------------
    # Log streaming
    # ------------------------
    @staticmethod
    def _stream_logs(container, logger, prefix: str) -> None:
        """Stream docker logs. Runs in a background thread."""
        try:
            for chunk in container.logs(stream=True, follow=True):
                if not chunk:
                    continue
                line = chunk.decode("utf-8", errors="replace").rstrip("\n")
                if line:
                    logger.info("%s %s", prefix, line)
        except Exception as e:
            logger.debug("Log streaming stopped: %r", e)

    @staticmethod
    def _tail_logs(container, tail: int = 200) -> str:
        """Return the last lines of container logs as a string."""
        try:
            data = container.logs(tail=tail)
            if not data:
                return ""
            return data.decode("utf-8", errors="replace")
        except Exception:
            return ""

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
        """Execute a single CWL step."""

        logger = get_logger("prefect-cwl")

        if step_template is None:
            raise ValidationError("step_template is required")

        # MATERIALIZE the step with runtime values
        step_plan = step_template.materialize_step(
            workflow_inputs=workflow_inputs,
            produced=produced,
            workspace=workspace,
            render_io=build_command_and_listing,
        )

        # Execute the materialized plan
        self._write_listings(step_plan.listings)
        self._ensure_mount_sources(step_plan.volumes)

        await pull_docker_image(repository=step_plan.image)

        job_name = f"{step_plan.step_name}-{uuid.uuid4().hex[:12]}"
        volume_args = self._volumes_to_prefect_docker(step_plan.volumes)

        logger.info("Docker step: %s image=%s", job_name, step_plan.image)
        logger.info("Command: %s", shlex.join(step_plan.argv))
        logger.info("Volumes: %s", volume_args)

        user = None
        if hasattr(os, 'getuid'):
            user = f"{os.getuid()}:{os.getgid()}"

        container = await create_docker_container(
            name=job_name,
            image=step_plan.image,
            command=step_plan.argv,  # Pass as list
            auto_remove=False,
            volumes=volume_args,
            environment=step_plan.envs,
            user=user,
        )

        container = await start_docker_container(container_id=container.id)

        log_task = asyncio.create_task(
            asyncio.to_thread(self._stream_logs, container, logger, f"[{job_name}]")
        )

        try:
            result = await asyncio.to_thread(container.wait)
            status = int(result.get("StatusCode", 1))
        finally:
            try:
                await log_task
            except Exception as e:
                logger.warning("Log streaming failed: %s", e)
                status = -1

        if status != 0:
            tail = self._tail_logs(container)
            raise RuntimeError(
                f"Step {step_plan.step_name} failed with exit code {status}\n\n{tail}"
            )

        # Track produced outputs for downstream steps
        for output_port, host_path in step_plan.out_artifacts.items():
            produced[(step_plan.step_name, output_port)] = host_path
