from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Dict, List, Tuple, Any, Callable

from prefect_cwl.constants import JOBROOT, INROOT
from prefect_cwl.exceptions import ValidationError
from prefect_cwl.models import WorkflowStep, CommandLineToolNode

def step_host_dirs(workspace: Path, step_name: str) -> Tuple[Path, Path]:
    host_outdir = workspace / "steps" / step_name / "out"
    host_jobdir = workspace / "steps" / step_name / "job"
    return host_outdir, host_jobdir





@dataclass(frozen=True)
class ResolvedInputs:
    values: Dict[str, Dict[str, Any]]
    volumes: Dict[str, str]

@dataclass(frozen=True)
class ListingMaterialization:
    """Materialized listing entry to be written on the host filesystem.

    Attributes:
        host_path: Absolute host path where the content will be written.
        content: Text content to write at host_path.
    """
    host_path: Path
    content: str


@dataclass
class StepPlan:
    """Fully materialized step ready for execution."""
    step_name: str
    tool_id: str
    image: str
    argv: List[str]
    outdir_container: PurePosixPath
    volumes: Dict[str, str]  # host -> "container:ro|rw"
    listings: List[ListingMaterialization]
    out_artifacts: Dict[str, Path]  # tool output name -> host path
    envs: Dict[str, str]


@dataclass
class StepTemplate:
    """Template for a step - no actual input values yet.

    Holds static information parsed from CWL. At runtime, call materialize_step
    to resolve inputs, mounts, argv, and listings.
    """
    step_name: str
    tool_id: str
    tool: CommandLineToolNode
    wf_step: WorkflowStep
    image: str
    outdir_container: PurePosixPath
    envs: Dict[str, str]

    def _compute_base_step_volumes(
            self,
            *,
            host_outdir: Path,
            outdir_container: PurePosixPath,
            host_jobdir: Path,
    ) -> Dict[str, str]:
        return {
            str(host_outdir): f"{outdir_container}:rw",
            str(host_jobdir): f"{JOBROOT}:rw",
        }

    def _resolve_step_inputs_and_mounts(self,
            *,
            wf_step: WorkflowStep,
            clt: CommandLineToolNode,
            workflow_inputs: Dict[str, Any],
            produced: Dict[Tuple[str, str], Path],
            base_volumes: Dict[str, str],
    ) -> ResolvedInputs:
        values: Dict[str, Dict[str, Any]] = {"workflow": dict(workflow_inputs), "inputs": {}}
        volumes = dict(base_volumes)

        def as_cwl_directory(container_path: PurePosixPath) -> Dict[str, Any]:
            return {"class": "Directory", "path": str(container_path)}

        def as_cwl_file(container_path: PurePosixPath) -> Dict[str, Any]:
            return {
                "class": "File",
                "path": str(container_path),
                "basename": container_path.name,
            }

        for inport, src in wf_step.in_.items():
            if isinstance(src, str) and "/" in src:
                upstream_step, upstream_outport = src.split("/", 1)
                host_art = produced.get((upstream_step, upstream_outport))
                if host_art is None:
                    raise ValidationError(f"Step {self.step_name!r} depends on {src!r} but it wasn't produced")

                tool_input = clt.inputs.get(inport)
                if tool_input is None:
                    raise ValidationError(f"Tool {clt.id!r} has no input {inport!r}")

                if tool_input.type == "Directory":
                    mount_target = INROOT / inport
                    volumes[str(host_art)] = f"{mount_target}:ro"
                    values["inputs"][inport] = as_cwl_directory(mount_target)

                elif tool_input.type == "File":
                    mount_target_dir = INROOT / inport
                    volumes[str(host_art.parent)] = f"{mount_target_dir}:ro"
                    file_in_container = mount_target_dir / host_art.name
                    values["inputs"][inport] = as_cwl_file(file_in_container)

                else:
                    raise ValidationError(
                        f"Input {inport!r} has unsupported type {tool_input.type!r} for upstream wiring"
                    )

            else:
                if src not in workflow_inputs:
                    raise ValidationError(
                        f"Step {self.step_name!r} input {inport!r} references unknown workflow input {src!r}"
                    )
                values["inputs"][inport] = workflow_inputs[src]

        return ResolvedInputs(values=values, volumes=volumes)

    def materialize_step(
            self,
            *,
            workflow_inputs: Dict[str, Any],
            produced: Dict[Tuple[str, str], Path],
            workspace: Path,
            render_io,
    ) -> StepPlan:
        """
        Convert a StepTemplate into a StepPlan by resolving actual runtime values.

        This happens at Prefect task execution time, not at plan time.

        Args:
            workflow_inputs: Actual runtime input values
            produced: Map of (step_name, output_port) -> Path for upstream outputs
            workspace: Workspace root directory
            render_io: Function to render command and listings

        Returns:
            Fully materialized StepPlan ready for execution
        """
        host_outdir, host_jobdir = step_host_dirs(workspace, self.step_name)

        base_volumes = self._compute_base_step_volumes(
            host_outdir=host_outdir,
            outdir_container=self.outdir_container,
            host_jobdir=host_jobdir,
        )

        resolved = self._resolve_step_inputs_and_mounts(
            wf_step=self.wf_step,
            clt=self.tool,
            workflow_inputs=workflow_inputs,
            produced=produced,
            base_volumes=base_volumes,
        )

        argv, rendered_listing = render_io(self.tool, resolved.values)

        mats = validate_and_materialize_listings(
            rendered_listing=rendered_listing,
            host_jobdir=host_jobdir,
        )

        out_artifacts = compute_out_artifacts(clt=self.tool, host_outdir=host_outdir)

        return StepPlan(
            step_name=self.step_name,
            tool_id=self.tool_id,
            image=self.image,
            argv=argv,
            outdir_container=self.outdir_container,
            volumes=resolved.volumes,
            listings=mats,
            out_artifacts=out_artifacts,
            envs=self.envs,
        )




@dataclass
class WorkflowTemplate:
    """Workflow structure without materialized values.

    Attributes:
        workflow_id: ID of the workflow.
        workflow_inputs: Input schemas (types), not runtime values.
        workspace: Workspace root on the host filesystem.
        step_templates: Templates for each workflow step.
        workflow_outputs: Output schemas describing sources.
        waves: Topological waves of step names.
    """
    workflow_id: str
    workflow_inputs: Dict[str, Any]  # Input schemas (types), not values
    workspace: Path
    step_templates: Dict[str, StepTemplate]
    workflow_outputs: Dict[str, Any]  # Output schemas
    waves: List[List[str]]

    def iter_steps(self):
        """Iterate over StepTemplate waves according to topological order."""
        for wave in self.waves:
            yield [self.step_templates.get(step) for step in wave]

def validate_and_materialize_listings(
        *,
        rendered_listing: List[Dict[str, str]],
        host_jobdir: Path,
) -> List[ListingMaterialization]:
    mats: List[ListingMaterialization] = []

    for item in rendered_listing:
        entryname = item["entryname"]
        entry = item["entry"]

        container_path = PurePosixPath(entryname)

        # Validate under JOBROOT
        try:
            rel = container_path.relative_to(JOBROOT)
        except ValueError:
            raise ValidationError(
                f"Listing entryname must be under {JOBROOT}: got {entryname!r}"
            )

        host_path = host_jobdir / rel
        mats.append(ListingMaterialization(host_path=host_path, content=entry))

    return mats


def compute_out_artifacts(*, clt: CommandLineToolNode, host_outdir: Path) -> Dict[str, Path]:
    out_artifacts: Dict[str, Path] = {}
    for outport, outspec in clt.outputs.items():
        glob = outspec.outputBinding.glob
        out_artifacts[outport] = host_outdir / glob
    return out_artifacts


RenderIOFn = Callable[
    [CommandLineToolNode, Dict[str, Dict[str, Any]]],
    Tuple[List[str], List[Dict[str, str]]],
]