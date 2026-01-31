"""Map CWL workflow to Python classes using Pydantic."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any, Dict, List, Literal, Optional, Union, Annotated

from pydantic import BaseModel, Field, ConfigDict, model_validator, field_validator

from prefect_cwl.exceptions import ValidationError
from prefect_cwl.constants import JOBROOT


# ---------------- Requirements (subset) ----------------

class DockerRequirement(BaseModel):
    """Docker requirement, mandatory."""
    dockerPull: str
    dockerOutputDirectory: PurePosixPath = JOBROOT

    @field_validator("dockerOutputDirectory")
    @classmethod
    def must_be_absolute_posix_path(cls, docker_output_directory: PurePosixPath) -> PurePosixPath:
        if not docker_output_directory.is_absolute():
            raise ValidationError(f"dockerOutputDirectory must be absolute; got {docker_output_directory!r}")
        return docker_output_directory

class EnvVarRequirement(BaseModel):
    """Environment variable requirement, optional."""
    envDef: Dict[str, str] = Field(default_factory=dict)

class Listing(BaseModel):
    """Listing requirement, optional."""
    entryname: PurePosixPath
    entry: str

    @field_validator("entryname")
    @classmethod
    def must_be_absolute_posix_path(cls, entryname: PurePosixPath) -> PurePosixPath:
        # Check if under JOBROOT
        try:
            entryname.relative_to(JOBROOT)
        except ValueError:
            raise ValueError(
                f"Listing entryname must be under {JOBROOT}: got {entryname!r}"
            )
        return entryname


class InitialWorkDirRequirement(BaseModel):
    """Initial work directory requirement, optional."""
    listing: List[Listing] = Field(default_factory=list)

class Requirements(BaseModel):
    """CWL Requirements."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    docker_requirement: DockerRequirement = Field(
        alias="DockerRequirement"
    )
    env_var_requirement: Optional[EnvVarRequirement] = Field(
        default=EnvVarRequirement(), alias="EnvVarRequirement"
    )
    initial_workdir_requirement: Optional[InitialWorkDirRequirement] = Field(
        default=InitialWorkDirRequirement(), alias ="InitialWorkDirRequirement"
    )


# ---------------- Bindings ----------------

class InputBinding(BaseModel):
    """Input binding. Position to -1 means no explicit position set (ie: CWL default)."""
    position: Optional[int] = -1
    prefix: Optional[str] = None
    separator: Optional[str] = None


class ToolInput(BaseModel):
    """Tool input."""
    model_config = ConfigDict(extra="allow")
    type: str
    inputBinding: Optional[InputBinding] = None

_GLOB_CHARS = re.compile(r"[*?\[\]{}]")
class OutputBinding(BaseModel):
    """Output binding."""
    glob: str

    @field_validator("glob")
    @classmethod
    def must_be_exact_relative_path(cls, v: str) -> str:
        """Validate that glob is an exact relative path without wildcards or traversal."""
        v = v.strip()

        if not v:
            raise ValueError("glob must be a non-empty path")

        # Must be relative (not absolute)
        if v.startswith("/"):
            raise ValueError(f"glob must be a relative path, not absolute: {v!r}")

        # Disallow glob metacharacters
        if _GLOB_CHARS.search(v) or "**" in v:
            raise ValueError(f"glob must be an exact path without wildcards: {v!r}")

        # Disallow parent traversal - simpler check using Path
        path = PurePosixPath(v)
        if ".." in path.parts:
            raise ValueError(f"glob must not contain parent traversal '..': {v!r}")

        return v


class ToolOutput(BaseModel):
    """Tool output."""
    model_config = ConfigDict(extra="allow")
    type: str
    outputBinding: OutputBinding


# ---------------- Workflow structures ----------------

class WorkflowInput(BaseModel):
    """Workflow input."""
    model_config = ConfigDict(extra="allow")
    type: str


class WorkflowOutput(BaseModel):
    """Workflow output."""
    model_config = ConfigDict(extra="allow")
    type: str
    outputSource: str


class WorkflowStep(BaseModel):
    """Workflow step."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    run: str
    in_: Dict[str, str] = Field(default_factory=dict, alias="in")
    out: List[str] = Field(default_factory=list)
    definition: Optional[CommandLineToolNode] = None
    volumes: Dict[str, str] = Field(default_factory=dict)




# ---------------- Graph nodes ----------------

class WorkflowNode(BaseModel):
    """Workflow node."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    kind: Literal["Workflow"] = Field(alias="class")
    id: str
    label: Optional[str] = None
    doc: Optional[str] = None
    inputs: Dict[str, WorkflowInput] = Field(default_factory=dict)
    outputs: Dict[str, WorkflowOutput] = Field(default_factory=dict)
    steps: Dict[str, WorkflowStep] = Field(default_factory=dict)

    @model_validator(mode="after")
    def enforce_step_run_id_matches_name(self) -> "WorkflowNode":
        """
        Enforce that if step.run is an in-document reference like '#X',
        then X must equal the step key name.
        Example:
          downloader: { run: "#downloader" }  ✅
          downloader: { run: "#other" }       ❌
        """
        for step_name, step in self.steps.items():
            run = step.run
            if isinstance(run, str) and run.startswith("#"):
                target = run[1:]
                if target != step_name:
                    raise ValidationError(
                        f"steps.{step_name}.run must be '#{step_name}' when using fragment refs; got {run!r}"
                    )
        return self


class CommandLineToolNode(BaseModel):
    """Command line tool node."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    kind: Literal["CommandLineTool"] = Field(alias="class")
    id: str
    requirements: Requirements
    baseCommand: Union[str, List[str]]
    arguments: List[Any] = Field(default_factory=list)
    inputs: Dict[str, ToolInput] = Field(default_factory=dict)
    outputs: Dict[str, ToolOutput] = Field(default_factory=dict)


GraphNode = Annotated[
    Union[WorkflowNode, CommandLineToolNode],
    Field(discriminator="kind"),
]


# ---------------- Document root ----------------

class CWLDocument(BaseModel):
    """CWL document."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    cwlVersion: str
    namespaces: Dict[str, str] = Field(default_factory=dict, alias="$namespaces")
    softwareVersion: Optional[str] = Field(default=None, alias="s:softwareVersion")
    graph: List[GraphNode] = Field(default_factory=list, alias="$graph")
