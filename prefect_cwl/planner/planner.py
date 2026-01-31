"""Plan CWL workflows into executable Prefect steps.

This module parses a CWL document, builds a dependency graph, and produces
workflow and step templates. At runtime, templates are materialized into
concrete execution plans by the selected backend.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

from prefect_cwl.planner.templates import StepTemplate, WorkflowTemplate
from prefect_cwl.exceptions import ValidationError
from prefect_cwl.models import CWLDocument, WorkflowNode, CommandLineToolNode


def index_graph(doc: CWLDocument) -> tuple[Dict[str, WorkflowNode], Dict[str, CommandLineToolNode]]:
    """Index document graph into workflow and tool maps by id."""
    workflows: Dict[str, WorkflowNode] = {}
    tools: Dict[str, CommandLineToolNode] = {}

    for node in doc.graph:
        if node.kind == "Workflow":
            workflows[node.id] = node
        else:
            tools[node.id] = node

    return workflows, tools


def select_workflow(workflows: Dict[str, WorkflowNode], workflow_ref: Optional[str]) -> WorkflowNode:
    """Select a workflow by reference or return the sole workflow.

    Raises ValidationError if the reference is provided but not found, or if no
    workflows exist in the document.
    """
    if not workflows:
        raise ValidationError("No Workflow node found")

    if workflow_ref:
        wf = workflows.get(workflow_ref)
        if wf is None:
            raise ValidationError(f"Workflow {workflow_ref!r} not found")
        return wf

    return next(iter(workflows.values()))





# -------------------------
# Planner class
# -------------------------

@dataclass
class Planner:
    """
    Parses CWL and creates a workflow template (structure only, no runtime values).

    In tests, inject custom render_io; in production use build_command_and_listing.
    """

    def __init__(self, text: str, workspace_root: Path):
        """Initialize a CWL parser and create the CWL document.

        Args:
            text: the CWL document to parse
            workspace_root: root directory for workflow execution
        """
        raw = yaml.safe_load(text)
        self.doc = CWLDocument.model_validate(raw)
        self.workspace_root: Path = workspace_root

    def _resolve_run_id(self, step_run: str) -> str:
        return step_run[1:] if step_run.startswith("#") else step_run

    def _topo_waves(self, downstream: Dict[str, Set[str]], indegree: Dict[str, int]) -> List[List[str]]:
        """Compute topological waves for a DAG.

        Nodes with indegree 0 form the first wave; after removing them, repeat.
        Raises ValueError when cycles are detected or dependencies cannot be
        resolved.
        """
        downstream = {k: set(v) for k, v in downstream.items()}
        indegree = dict(indegree)

        remaining: Set[str] = set(indegree.keys())
        waves: List[List[str]] = []

        while remaining:
            wave = sorted([s for s in remaining if indegree.get(s, 0) == 0])
            if not wave:
                stuck = sorted(remaining)
                raise ValueError(f"Cycle detected or unresolved deps among: {stuck}")

            waves.append(wave)

            for s in wave:
                remaining.remove(s)
                for dep in downstream.get(s, set()):
                    indegree[dep] -= 1
                downstream.pop(s, None)

        return waves

    def _build_dependency_graph(self, wf: WorkflowNode) -> tuple[Dict[str, Set[str]], Dict[str, int]]:
        """Build a dependency graph from workflow step inputs.

        Returns a tuple of (downstream, indegree) where:
          - downstream maps a step to the set of steps that depend on it.
          - indegree maps a step to the number of unresolved upstream deps.
        """
        steps = set(wf.steps.keys())
        downstream: Dict[str, Set[str]] = {s: set() for s in steps}
        indegree: Dict[str, int] = {s: 0 for s in steps}

        for step_name, step in wf.steps.items():
            for _, src in step.in_.items():
                if isinstance(src, str) and "/" in src:
                    upstream_step = src.split("/", 1)[0]
                    if upstream_step in steps:
                        downstream[upstream_step].add(step_name)
                        indegree[step_name] += 1

        return downstream, indegree


    def prepare(self, workflow_ref: str) -> WorkflowTemplate:
        """
        Build workflow template - just structure, no runtime input values.

        Args:
            workflow_ref: CWL workflow reference (e.g., "#main")

        Returns:
            WorkflowTemplate with execution graph and step templates
        """
        workflows, tools = index_graph(self.doc)
        wf = select_workflow(workflows, workflow_ref)

        downstream, indegree = self._build_dependency_graph(wf)
        waves = self._topo_waves(downstream, indegree)

        step_templates: Dict[str, StepTemplate] = {}

        for wf_step_name, wf_step_value in wf.steps.items():
            tool_id = self._resolve_run_id(wf_step_value.run)
            clt = tools.get(tool_id)
            if clt is None:
                raise ValidationError(f"Step {wf_step_name} refers to unknown tool {tool_id!r}")

            docker = clt.requirements.docker_requirement
            outdir_container = docker.dockerOutputDirectory
            image = docker.dockerPull

            step_templates[wf_step_name] = StepTemplate(
                step_name=wf_step_name,
                tool_id=clt.id,
                tool=clt,
                wf_step=wf_step_value,
                image=image,
                outdir_container=outdir_container,
                envs=clt.requirements.env_var_requirement.envDef,
            )

        # Compute workflow output schema (what outputs exist, not their values)
        wf_output_schema: Dict[str, Any] = {}
        for wf_out_name, wf_out in wf.outputs.items():
            src = wf_out.outputSource
            sname, oport = src.split("/", 1)
            if sname not in step_templates:
                raise ValidationError(f"Workflow output {wf_out_name!r} references unknown step {sname!r}")
            # Store the reference, not the actual path (that comes at runtime)
            wf_output_schema[wf_out_name] = {"source_step": sname, "source_port": oport}

        return WorkflowTemplate(
            workflow_id=wf.id,
            workflow_inputs=wf.inputs,  # Input schemas (types)
            workspace=self.workspace_root,
            step_templates=step_templates,
            workflow_outputs=wf_output_schema,
            waves=waves,
        )