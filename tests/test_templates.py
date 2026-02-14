from pathlib import Path
from pathlib import PurePosixPath

import pytest

from prefect_cwl.constants import JOBROOT, INROOT
from models import CWLDocument
from models import (
    CommandLineToolNode,
    Requirements,
    DockerRequirement,
    EnvVarRequirement,
    ToolInput,
    ToolOutput,
    OutputBinding,
    WorkflowStep,
)
from prefect_cwl.planner.planner import index_graph
from prefect_cwl.exceptions import ValidationError
from prefect_cwl.planner.templates import (
    StepTemplate,
    compute_out_artifacts,
    validate_and_materialize_listings,
    step_host_dirs,
)


def test_validate_and_materialize_listings_ok(tmp_path: Path):
    host_jobdir = tmp_path / "job"
    rendered = [
        {"entryname": str(JOBROOT / "inputs" / "hello.txt"), "entry": "hi"},
        {"entryname": str(JOBROOT / "subdir" / "a.txt"), "entry": "aaa"},
    ]

    mats = validate_and_materialize_listings(
        rendered_listing=rendered, host_jobdir=host_jobdir
    )
    assert len(mats) == 2

    assert mats[0].host_path == host_jobdir / "inputs" / "hello.txt"
    assert mats[0].content == "hi"


def test_validate_and_materialize_listings_rejects_outside_jobroot(tmp_path: Path):
    host_jobdir = tmp_path / "job"
    rendered = [{"entryname": "/not_jobroot/x.txt", "entry": "nope"}]

    with pytest.raises(ValidationError, match="must be under"):
        validate_and_materialize_listings(
            rendered_listing=rendered, host_jobdir=host_jobdir
        )


def test_compute_out_artifacts_maps_globs(tmp_path: Path, linear_doc: CWLDocument):
    _, tools = index_graph(linear_doc)
    clt = tools["tool1"]

    host_outdir = tmp_path / "out"
    out = compute_out_artifacts(clt=clt, host_outdir=host_outdir)

    assert out["out_file"] == host_outdir / "hello.txt"


def _make_step_template(
    *,
    tool_id: str,
    step_name: str,
    step_in: dict[str, str],
    tool_inputs: dict[str, str],
    output_glob: str = "out_dir",
) -> StepTemplate:
    tool = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": tool_id,
            "requirements": Requirements(
                DockerRequirement=DockerRequirement(
                    dockerPull="python:3.11",
                    dockerOutputDirectory=PurePosixPath("/out"),
                ),
                EnvVarRequirement=EnvVarRequirement(envDef={}),
            ),
            "baseCommand": ["echo", "ok"],
            "inputs": {k: ToolInput(type=v) for k, v in tool_inputs.items()},
            "outputs": {
                "o": ToolOutput(
                    type="Directory", outputBinding=OutputBinding(glob=output_glob)
                )
            },
        }
    )
    wf_step = WorkflowStep(run=f"#{step_name}", **{"in": step_in}, out=["o"])
    return StepTemplate(
        step_name=step_name,
        tool_id=tool_id,
        tool=tool,
        wf_step=wf_step,
        image="python:3.11",
        outdir_container=PurePosixPath("/out"),
        envs={},
    )


def test_step_host_dirs_supports_job_suffix(tmp_path: Path):
    outdir, jobdir = step_host_dirs(tmp_path, "grepper", job_suffix="grep_string-1")
    assert outdir == tmp_path / "steps" / "grepper-grep_string-1" / "out"
    assert jobdir == tmp_path / "steps" / "grepper-grep_string-1" / "job"


def test_materialize_step_wires_directory_array_from_upstream(tmp_path: Path):
    step = _make_step_template(
        tool_id="counter",
        step_name="counter",
        step_in={"counter_input": "augmenter/augmenter_output"},
        tool_inputs={"counter_input": "Directory[]"},
        output_glob="counter_output",
    )

    produced = {
        ("augmenter", "augmenter_output"): [
            tmp_path / "steps" / "augmenter-a" / "out" / "augmenter_output",
            tmp_path / "steps" / "augmenter-b" / "out" / "augmenter_output",
        ]
    }

    seen_values = {}

    def fake_render_io(clt, values):  # type: ignore[no-untyped-def]
        seen_values.update(values)
        return ["echo", "ok"], []

    plan = step.materialize_step(
        workflow_inputs={},
        produced=produced,
        workspace=tmp_path,
        render_io=fake_render_io,
    )

    dirs = seen_values["inputs"]["counter_input"]
    assert isinstance(dirs, list)
    assert len(dirs) == 2
    assert dirs[0]["class"] == "Directory"
    assert dirs[0]["path"] == str(INROOT / "counter_input" / "0")
    assert dirs[1]["path"] == str(INROOT / "counter_input" / "1")

    assert str(produced[("augmenter", "augmenter_output")][0]) in plan.volumes
    assert str(produced[("augmenter", "augmenter_output")][1]) in plan.volumes


def test_materialize_step_rejects_directory_array_into_scalar_directory(tmp_path: Path):
    step = _make_step_template(
        tool_id="augmenter",
        step_name="augmenter",
        step_in={"augmenter_input": "grepper/grepper_output"},
        tool_inputs={"augmenter_input": "Directory"},
    )
    produced = {
        ("grepper", "grepper_output"): [
            tmp_path / "steps" / "g0" / "out" / "grepper_output",
            tmp_path / "steps" / "g1" / "out" / "grepper_output",
        ]
    }

    with pytest.raises(ValidationError, match="expects Directory but got array"):
        step.materialize_step(
            workflow_inputs={},
            produced=produced,
            workspace=tmp_path,
            render_io=lambda *_: (["echo"], []),
        )


def test_materialize_step_directory_override_mounts_path(tmp_path: Path):
    step = _make_step_template(
        tool_id="augmenter",
        step_name="augmenter",
        step_in={"augmenter_input": "unused"},
        tool_inputs={"augmenter_input": "Directory"},
        output_glob="augmenter_output",
    )
    override_dir = tmp_path / "steps" / "grepper-0" / "out" / "grepper_output"
    seen_values = {}

    def fake_render_io(clt, values):  # type: ignore[no-untyped-def]
        seen_values.update(values)
        return ["echo", "ok"], []

    plan = step.materialize_step(
        workflow_inputs={},
        produced={},
        workspace=tmp_path,
        render_io=fake_render_io,
        input_overrides={"augmenter_input": override_dir},
    )

    assert seen_values["inputs"]["augmenter_input"]["class"] == "Directory"
    assert seen_values["inputs"]["augmenter_input"]["path"] == str(
        INROOT / "augmenter_input"
    )
    assert str(override_dir) in plan.volumes
