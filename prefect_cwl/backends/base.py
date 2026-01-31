"""Base interface for running single CWL steps under Prefect."""
from pathlib import Path
from typing import Any, Dict, Tuple

from prefect_cwl.planner.templates import StepTemplate


class Backend:
    """Base class for implementing custom workflow execution backends."""

    async def call_single_step(
        self,
        step_template: StepTemplate,
        workflow_inputs: Dict[str, Any],
        produced: Dict[Tuple[str, str], Path],
        workspace: Path,
    ) -> None:
        """Materialize and execute a single step.

        Implementations must materialize the provided StepTemplate using the
        given runtime inputs and shared produced outputs, then execute it in the
        target environment (e.g., Docker, Kubernetes).

        Args:
            step_template: The step to materialize and run.
            workflow_inputs: Runtime workflow input values.
            produced: Mapping of (step_name, output_port) to host Path of
                artifacts produced so far; implementations should update it with
                this step's outputs upon success.
            workspace: Host workspace root used for planning/materialization.
        """
        raise NotImplementedError