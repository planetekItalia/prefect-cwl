"""Plan object definitions for execution backends.

This module defines small, immutable dataclasses used to describe how a
single CWL step should be executed by a backend (Docker, Kubernetes, etc.).
"""

# ----------------------------
# Pure “plan” objects
# ----------------------------
from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass(frozen=True)
class StageFile:
    """A file or directory to be staged before step execution.

    Attributes:
        abs_path: Absolute path on the target filesystem (e.g., PVC or host).
        content: File content to write; if None, create the directory only.
        mount_point: Container path where the file/dir will be mounted.
    """
    abs_path: str
    content: Optional[str]  # None => mkdir dir only
    mount_point: str


@dataclass(frozen=True)
class StepPlan:
    """Backend-agnostic execution plan for a single CWL step.

    Attributes:
        job_name: Unique name used for the container/job.
        image: Container image reference.
        command: Command and arguments to execute.
        env: Environment variables for the container/job.
        mounts: Volume mounts. For Kubernetes, each dict contains mountPath and subPath.
        stage_files: Files/directories to materialize before execution.
        mkdir_dirs_abs: Absolute directories to create on the target filesystem.
        user: Optional user spec for the container runtime (backend-specific).
        workdir: Optional working directory in the container.
    """
    job_name: str
    image: str
    command: List[str]
    env: Dict[str, str]

    # K8s mounts (each is a dict with mountPath/subPath)
    mounts: List[Dict[str, str]]

    # PVC ops
    stage_files: List[StageFile]

    mkdir_dirs_abs: List[str] = field(default_factory=list)

    user: str = None
    workdir: str = None