"""Utilities to build and run Prefect flows from CWL definitions.

This package provides parsers, models, and execution backends (e.g., Docker,
Kubernetes) to translate CWL documents into executable Prefect flows.
"""
from pathlib import Path

from prefect import Flow

from prefect_cwl.backends.base import Backend

from prefect_cwl.flow_builder import PrefectFlowBuilder
from prefect_cwl.planner.planner import Planner

def _create_flow_with_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
    backend: Backend,
) -> Flow:
    """Create flow with custom backend.

    Args:
        workflow_text: CWL document content.
        host_work_dir: Host workspace directory used for planning and mounts.
        workflow_id: CWL workflow reference (e.g., "#main").
        backend: Execution backend (Docker, Kubernetes, etc.).

    Returns:
        A Prefect Flow object ready to run.
    """
    workflow_plan = Planner(
        workflow_text,
        workspace_root=host_work_dir,
    ).prepare(workflow_ref=workflow_id)

    flow_builder = PrefectFlowBuilder()
    return flow_builder.build(workflow_plan, backend)


def create_flow_with_docker_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
) -> Flow:
    """Create a Prefect flow that executes steps on Docker.

    See _create_flow_with_backend for parameter details.
    """
    try:
        from prefect_cwl.backends.docker import DockerBackend
    except ImportError as e:
        raise ImportError(
            "Docker backend is not installed.\n"
            "Install it with:\n\n"
            "  pip install prefect-cwl[docker]\n"
            "  # or\n"
            "  uv add prefect-cwl[docker]"
        ) from e

    return _create_flow_with_backend(
        workflow_text,
        host_work_dir,
        workflow_id,
        DockerBackend(),
    )


def create_flow_with_k8s_backend(
    workflow_text: str,
    host_work_dir: Path,
    workflow_id: str,
) -> Flow:
    try:
        from prefect_cwl.backends.k8s import K8sBackend
    except ImportError as e:
        raise ImportError(
            "Kubernetes backend is not installed.\n"
            "Install it with:\n\n"
            "  pip install prefect-cwl[k8s]\n"
            "  # or\n"
            "  uv add prefect-cwl[k8s]"
        ) from e

    return _create_flow_with_backend(
        workflow_text,
        host_work_dir,
        workflow_id,
        K8sBackend(),
    )