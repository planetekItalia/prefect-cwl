"""Utilities to construct Prefect flows from workflow templates."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from prefect import Flow, flow, task

from prefect_cwl.backends.base import Backend
from prefect_cwl.planner.templates import StepTemplate, WorkflowTemplate


class PrefectFlowBuilder:
    """Prefect flow builder."""

    def __init__(self, log_prints: bool = True):
        """Initialize the builder."""
        self.log_prints = log_prints

    def build_executor(
        self,
        template: WorkflowTemplate,
        backend: Backend,
    ) -> Callable[..., Awaitable[Dict[str, Any]]]:
        """Build a deploy-safe executor callable.

        The returned callable can be awaited inside a *top-level* @flow that is
        importable (deployable). This avoids returning a closure Flow object as
        the deployment entrypoint.

        The callable signature matches workflow inputs for UI rendering.
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
        ) -> None:
            """Materialize and execute a single step."""
            await backend.call_single_step(
                step_template=step_template,
                workflow_inputs=workflow_inputs,
                produced=produced,
                workspace=template.workspace,
            )

        async def execute(**kwargs: Any) -> Dict[str, Any]:
            """Execute all steps in topologically sorted waves."""
            # Validate inputs (required inputs only)
            workflow_inputs = dict(kwargs)
            for name in template.workflow_inputs.keys():
                if name not in workflow_inputs:
                    raise ValueError(f"Missing required workflow input: {name}")

            produced: Dict[Tuple[str, str], Path] = {}

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
                        produced=produced,  # shared state
                    )
                    futures.append(fut)

                # Barrier
                for fut in futures:
                    fut.result()

            workflow_outputs: Dict[str, Any] = {}
            for output_name, output_spec in template.workflow_outputs.items():
                source_step = output_spec["source_step"]
                source_port = output_spec["source_port"]
                output_path = produced.get((source_step, source_port))
                if output_path:
                    workflow_outputs[output_name] = output_path

            return workflow_outputs

        # Set signature for Prefect UI (keyword-only)
        execute.__signature__ = inspect.Signature(
            [
                inspect.Parameter(
                    name,
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=tp,
                )
                for name, tp in params
            ]
        )

        return execute

    def build(self, template: WorkflowTemplate, backend: Backend) -> Flow:
        """Build the Prefect flow given the workflow template and backend.

        Backward-compatible convenience: returns a Flow object.
        Note: this Flow is dynamically constructed (closure), so it is not a
        stable deployment entrypoint. For deployments, prefer build_executor()
        inside a top-level importable @flow.
        """
        executor = self.build_executor(template=template, backend=backend)

        @flow(name=template.workflow_id, log_prints=self.log_prints)
        async def _process_flow(**kwargs: Any):
            return await executor(**kwargs)

        # Preserve the UI signature on the flow callable
        sig = getattr(executor, "__signature__", None)
        if sig is not None:
            _process_flow.fn.__signature__ = sig  # type: ignore[attr-defined]

        return _process_flow
