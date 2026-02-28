"""Utilities to construct Prefect flows from workflow templates."""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from prefect import task

from prefect_cwl.backends.base import Backend
from prefect_cwl.exceptions import ValidationError
from prefect_cwl.planner.templates import ArtifactPath, StepTemplate, WorkflowTemplate


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
            produced_snapshot: Dict[Tuple[str, str], ArtifactPath],
            job_suffix: Optional[str] = None,
            input_overrides: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, ArtifactPath]:
            """Materialize and execute a single step task."""
            local_produced = dict(produced_snapshot)
            await backend.call_single_step(
                step_template=step_template,
                workflow_inputs=workflow_inputs,
                produced=local_produced,
                workspace=template.workspace,
                job_suffix=job_suffix,
                input_overrides=input_overrides,
            )
            outputs: Dict[str, ArtifactPath] = {}
            for (step_name, output_port), host_path in local_produced.items():
                if step_name != step_template.step_name:
                    continue
                outputs[output_port] = host_path
            return outputs

        def _resolve_scatter_values(
            step_template: StepTemplate,
            workflow_inputs: Dict[str, Any],
            produced: Dict[Tuple[str, str], ArtifactPath],
        ) -> Tuple[Optional[str], Optional[List[Any]]]:
            """Resolve scatter values from workflow inputs or upstream step outputs."""
            raw_scatter = step_template.wf_step.scatter
            if not raw_scatter:
                return None, None

            if isinstance(raw_scatter, list):
                if len(raw_scatter) == 1:
                    scatter_inport = raw_scatter[0]
                else:
                    raise ValidationError(
                        "Multi-input scatter is not supported yet by this executor; "
                        f"got scatter={raw_scatter!r}"
                    )
            else:
                scatter_inport = raw_scatter

            source = step_template.wf_step.in_.get(scatter_inport, scatter_inport)

            if isinstance(source, str) and "/" in source:
                upstream_step, upstream_port = source.split("/", 1)
                scatter_values = produced.get((upstream_step, upstream_port))
                if not isinstance(scatter_values, list):
                    raise ValidationError(
                        f"Scatter source {source!r} must be a list of produced artifacts"
                    )
            else:
                wf_input_name = str(source)
                wf_input = template.workflow_inputs.get(wf_input_name)
                if wf_input is None:
                    raise ValidationError(
                        f"Scatter input source {wf_input_name!r} not found in workflow inputs"
                    )

                def _is_array_type(t: str) -> bool:
                    t = t.strip()
                    if t.endswith("?"):
                        t = t[:-1]
                    return t.endswith("[]")

                if not _is_array_type(wf_input.type):
                    raise ValidationError(
                        f"Scatter input source {wf_input_name!r} must be an array type; got {wf_input.type!r}"
                    )
                scatter_values = workflow_inputs.get(wf_input_name)

            if not isinstance(scatter_values, (list, tuple)):
                raise ValidationError(
                    f"Scatter source for {scatter_inport!r} must be a list; got {type(scatter_values).__name__}"
                )
            if not scatter_values:
                raise ValidationError(
                    f"Scatter source for {scatter_inport!r} must not be empty"
                )
            if any(item is None for item in scatter_values):
                raise ValidationError(
                    f"Scatter source for {scatter_inport!r} must not contain null items"
                )

            return scatter_inport, list(scatter_values)

        def _merge_step_outputs(
            produced: Dict[Tuple[str, str], ArtifactPath],
            wave_runs: List[Tuple[str, Optional[int], Any]],
        ) -> None:
            """Merge per-run step outputs into shared produced mapping."""
            single_outputs: Dict[Tuple[str, str], ArtifactPath] = {}
            scattered_outputs: Dict[
                Tuple[str, str], List[Tuple[int, ArtifactPath]]
            ] = {}

            for step_name, scatter_idx, fut in wave_runs:
                run_outputs = fut.result()
                for output_port, host_path in run_outputs.items():
                    key = (step_name, output_port)
                    if scatter_idx is None:
                        single_outputs[key] = host_path
                    else:
                        scattered_outputs.setdefault(key, []).append(
                            (scatter_idx, host_path)
                        )

            for key, path in single_outputs.items():
                produced[key] = path
            for key, indexed_outputs in scattered_outputs.items():
                ordered = [
                    p for _, p in sorted(indexed_outputs, key=lambda item: item[0])
                ]
                flattened: List[Path] = []
                for item in ordered:
                    if isinstance(item, list):
                        flattened.extend(item)
                    else:
                        flattened.append(item)
                produced[key] = flattened

        async def execute(**kwargs: Any) -> Dict[str, Any]:
            """Execute all steps in topologically sorted waves, checking for scattered-steps."""
            # Validate inputs (required inputs only)
            workflow_inputs = dict(kwargs)
            for name in template.workflow_inputs.keys():
                if name not in workflow_inputs:
                    raise ValueError(f"Missing required workflow input: {name}")

            produced: Dict[Tuple[str, str], ArtifactPath] = {}

            scatter_limit = None
            scatter_limit_raw = os.getenv("PREFECT_CWL_SCATTER_CONCURRENCY", 4)
            if scatter_limit_raw:
                try:
                    scatter_limit = int(scatter_limit_raw)
                except ValueError as exc:
                    raise ValidationError(
                        "PREFECT_CWL_SCATTER_CONCURRENCY must be an integer"
                    ) from exc
                if scatter_limit <= 0:
                    scatter_limit = None
            scatter_tag = os.getenv("PREFECT_CWL_SCATTER_TAG", "prefect-cwl-scatter")
            scatter_tag = scatter_tag.strip() or None

            for wave_idx, wave in enumerate(template.iter_steps()):
                wave_runs: List[Tuple[str, Optional[int], Any]] = []
                active_scatter = []
                for step_template in wave:
                    if step_template is None:
                        continue

                    scatter_inport, scatter_values = _resolve_scatter_values(
                        step_template, workflow_inputs, produced
                    )
                    if scatter_values is None:
                        run_step_options: Dict[str, Any] = {
                            "name": f"wave:{wave_idx} step:{step_template.step_name}"
                        }
                        if scatter_tag:
                            run_step_options["tags"] = {scatter_tag}

                        fut = run_step.with_options(**run_step_options).submit(
                            step_template=step_template,
                            workflow_inputs=workflow_inputs,
                            produced_snapshot=produced,
                        )
                        wave_runs.append((step_template.step_name, None, fut))
                    else:
                        for idx, item in enumerate(scatter_values):
                            run_step_options: Dict[str, Any] = {
                                "name": (
                                    f"wave:{wave_idx} step:{step_template.step_name} "
                                    f"scatter:{scatter_inport}[{idx}]"
                                )
                            }
                            if scatter_tag:
                                run_step_options["tags"] = {scatter_tag}

                            fut = run_step.with_options(**run_step_options).submit(
                                step_template=step_template,
                                workflow_inputs=workflow_inputs,
                                produced_snapshot=produced,
                                job_suffix=f"{scatter_inport}-{idx}",
                                input_overrides={scatter_inport: item},
                            )
                            wave_runs.append((step_template.step_name, idx, fut))
                            if scatter_limit:
                                active_scatter.append(fut)
                                if len(active_scatter) >= scatter_limit:
                                    for sfut in active_scatter:
                                        sfut.result()
                                    active_scatter = []

                for sfut in active_scatter:
                    sfut.result()
                _merge_step_outputs(produced, wave_runs)

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
