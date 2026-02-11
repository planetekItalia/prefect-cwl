"""prefect-cwl: Orchestrate CWL with Prefect.

This module provides a factory for creating Prefect flows and wrappable executors from CWL documents.
"""

from prefect_cwl.factory import (
    create_executor_with_docker_backend,
    create_executor_with_k8s_backend,
    create_flow_with_docker_backend,
    create_flow_with_k8s_backend,
)

__all__ = [
    "create_executor_with_docker_backend",
    "create_executor_with_k8s_backend",
    "create_flow_with_docker_backend",
    "create_flow_with_k8s_backend",
]

from importlib.metadata import version as _version

__version__ = _version("prefect-cwl")
