"""Factory functions for creating Prefect Flows and executors."""

from __future__ import annotations

from pathlib import Path
from typing import Awaitable, Callable, Dict, Any

from prefect import Flow, flow

from prefect_cwl.backends.base import Backend
from prefect_cwl.flow_builder import PrefectFlowBuilder
from prefect_cwl.planner.planner import Planner


# --------------------------
# Internal helpers
# --------------------------

from importlib.util import find_spec


def _require_backend(module: str, extra: str) -> None:
    if find_spec(module) is None:
        raise ImportError(
            f"{extra} backend is not installed.\n"
            "Install it with:\n\n"
            f"  pip install prefect-cwl[{extra}]\n"
            "  # or\n"
            f"  uv add prefect-cwl[{extra}]"
        )


def _prepare_plan(workflow_text: str, host_work_dir: Path, workflow_id: str):
    """Parse + plan CWL into a WorkflowTemplate/plan object."""
    return Planner(
        workflow_text,
        workspace_root=host_work_dir,
    ).prepare(workflow_ref=workflow_id)


def _create_executor_with_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
    backend: Backend,
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Create a deploy-safe executor callable using a custom backend.

    Returns an async callable that can be awaited inside a top-level @flow.
    """
    workflow_plan = _prepare_plan(workflow_text, host_work_dir, workflow_id)

    f_builder = PrefectFlowBuilder()
    return f_builder.build_executor(workflow_plan, backend)


def _wrap_executor_as_flow(
    workflow_id: str,
    executor: Callable[..., Awaitable[Dict[str, Any]]],
) -> Flow:
    """Wrap an executor into a Flow for convenience local usage."""

    @flow(name=workflow_id)
    async def _dynamic_flow(**kwargs: Any):
        return await executor(**kwargs)

    # If builder set execute.__signature__, copy it so the UI signature stays nice
    sig = getattr(executor, "__signature__", None)
    if sig is not None:
        _dynamic_flow.fn.__signature__ = sig  # type: ignore[attr-defined]

    return _dynamic_flow


# --------------------------
# Public API: Docker
# --------------------------


def create_executor_with_docker_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Create a deploy-safe executor that executes steps on Docker."""
    try:
        _require_backend("prefect_cwl.backends.docker", "DockerBackend")
        from prefect_cwl.backends.docker import DockerBackend
    except ImportError as e:
        raise ImportError(
            "Docker backend is not installed.\n"
            "Install it with:\n\n"
            "  pip install prefect-cwl[docker]\n"
            "  # or\n"
            "  uv add prefect-cwl[docker]"
        ) from e

    return _create_executor_with_backend(
        workflow_text,
        host_work_dir,
        workflow_id,
        DockerBackend(),
    )


def create_flow_with_docker_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
) -> Flow:
    """Create a Prefect Flow that executes steps on Docker (dynamic flow).

    Good for local runs; for deployments prefer create_executor_with_docker_backend
    inside a stable top-level @flow.
    """
    try:
        _require_backend("prefect_cwl.backends.docker", "DockerBackend")
    except ImportError as e:
        raise ImportError(
            "Docker backend is not installed.\n"
            "Install it with:\n\n"
            "  pip install prefect-cwl[docker]\n"
            "  # or\n"
            "  uv add prefect-cwl[docker]"
        ) from e

    executor = create_executor_with_docker_backend(
        workflow_text, host_work_dir, workflow_id
    )
    return _wrap_executor_as_flow(workflow_id, executor)


# --------------------------
# Public API: Kubernetes
# --------------------------


def create_executor_with_k8s_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Create a deploy-safe executor that executes steps on Kubernetes."""
    try:
        _require_backend("prefect_cwl.backends.k8s", "K8sBackend")
        from prefect_cwl.backends.k8s import K8sBackend
    except ImportError as e:
        raise ImportError(
            "Kubernetes backend is not installed.\n"
            "Install it with:\n\n"
            "  pip install prefect-cwl[k8s]\n"
            "  # or\n"
            "  uv add prefect-cwl[k8s]"
        ) from e

    return _create_executor_with_backend(
        workflow_text,
        host_work_dir,
        workflow_id,
        K8sBackend(),
    )


def create_flow_with_k8s_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
) -> Flow:
    """Create a Prefect Flow that executes steps on Kubernetes (dynamic flow).

    Good for local runs; for deployments prefer create_executor_with_k8s_backend
    inside a stable top-level @flow.
    """
    try:
        _require_backend("prefect_cwl.backends.k8s", "K8sBackend")
    except ImportError as e:
        raise ImportError(
            "Kubernetes backend is not installed.\n"
            "Install it with:\n\n"
            "  pip install prefect-cwl[k8s]\n"
            "  # or\n"
            "  uv add prefect-cwl[k8s]"
        ) from e

    executor = create_executor_with_k8s_backend(
        workflow_text, host_work_dir, workflow_id
    )
    return _wrap_executor_as_flow(workflow_id, executor)
