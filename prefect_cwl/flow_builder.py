"""Utilities to construct Prefect flows from workflow templates."""
import inspect
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from prefect import flow, task, Flow

from prefect_cwl.backends.base import Backend
from prefect_cwl.planner.templates import StepTemplate, WorkflowTemplate


class PrefectFlowBuilder:
    """Prefect flow builder."""

    def __init__(self, log_prints: bool = True):
        """Initialize the builder."""
        self.log_prints = log_prints

    def build(self, template: WorkflowTemplate, backend: Backend) -> Flow:
        """Build the Prefect flow given the workflow template and backend.

        The flow signature matches workflow inputs for UI rendering.
        At runtime, inputs are passed to each task for materialization.

        Args:
            template: the workflow template (structure only)
            backend: the backend to use

        Returns:
            the Prefect flow
        """
        type_mapping = {
            "string": str,
            "string[]": List[str],
            "float": float,
            "float[]": List[float],
            "int": int,
            "int[]": List[int],
            "string?": Optional[str],
            "string[]?": Optional[List[str]],
            "float?": Optional[float],
            "float[]?": Optional[List[float]],
            "int?": Optional[int],
            "int[]?": Optional[List[int]],
        }

        params = [
            (name, type_mapping.get(tp.type, Any))
            for name, tp in template.workflow_inputs.items()
        ]

        @task
        async def run_step(
                step_template: StepTemplate,
                workflow_inputs: Dict[str, Any],
                produced: Dict[Tuple[str, str], Path],
        ):
            """
            Materialize and execute a single step.

            Args:
                step_template: The step template (no values)
                workflow_inputs: Runtime workflow input values
                produced: Upstream outputs produced so far

            Returns:
                None
            """
            # Backend materializes the template with runtime values and executes
            await backend.call_single_step(
                step_template=step_template,
                workflow_inputs=workflow_inputs,
                produced=produced,
                workspace=template.workspace,
            )

        async def process_flow(**kwargs: Any):
            """
            Execute all steps in topologically sorted waves.

            Steps in the same wave run in parallel. We track produced outputs
            across waves for dependency resolution.
            """
            # Validate inputs
            workflow_inputs = dict(kwargs)
            for name in template.workflow_inputs.keys():
                if name not in workflow_inputs:
                    raise ValueError(f"Missing required workflow input: {name}")

            # Track produced artifacts across all steps
            produced: Dict[Tuple[str, str], Path] = {}

            # Execute waves
            for wave_idx, wave in enumerate(template.iter_steps()):
                futures = []

                for step_template in wave:
                    if step_template is None:
                        continue

                    fut = run_step.with_options(
                        name=f"wave:{wave_idx} step:{step_template.step_name}"
                    ).submit(
                        step_template=step_template,
                        workflow_inputs=workflow_inputs,
                        produced=produced,  # Shared state
                    )
                    futures.append(fut)

                # Barrier: wait for all steps in wave to complete
                [future.result() for future in futures]

            # Optionally return workflow outputs
            workflow_outputs = {}
            for output_name, output_spec in template.workflow_outputs.items():
                source_step = output_spec["source_step"]
                source_port = output_spec["source_port"]
                output_path = produced.get((source_step, source_port))
                if output_path:
                    workflow_outputs[output_name] = output_path

            return workflow_outputs

        # Set signature for Prefect UI
        process_flow.__signature__ = inspect.Signature([
            inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=tp)
            for name, tp in params
        ])

        return flow(name=template.workflow_id, log_prints=self.log_prints)(process_flow)