from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest

from prefect_cwl.exceptions import ValidationError

# Adjust these imports to your actual module paths
from prefect_cwl.planner.planner import (
    Planner,
    index_graph,
    select_workflow,
)

# Import your Pydantic models (the ones you pasted)
from prefect_cwl.models import (
    CWLDocument,
    WorkflowNode,
    WorkflowStep,
    WorkflowInput,
    WorkflowOutput,
    CommandLineToolNode,
    Requirements,
    DockerRequirement,
    EnvVarRequirement,
    ToolInput,
    InputBinding,
    ToolOutput,
    OutputBinding,
)


def make_planner(doc: CWLDocument, workspace_root: Path) -> Planner:
    """Create Planner without going through __init__ (no YAML parsing)."""
    p = Planner.__new__(Planner)
    p.doc = doc
    p.workspace_root = workspace_root
    return p


def make_tool(
    *,
    tool_id: str,
    glob: str,
    image: str = "python:3.11",
    outdir: PurePosixPath = PurePosixPath("/cwl_job/out"),
) -> CommandLineToolNode:
    reqs = Requirements(
        DockerRequirement=DockerRequirement(dockerPull=image, dockerOutputDirectory=outdir),
        EnvVarRequirement=EnvVarRequirement(envDef={}),
    )
    return CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": tool_id,
            "requirements": reqs,
            "baseCommand": "python",
            "inputs": {
                "x": ToolInput(type="string", inputBinding=InputBinding(position=1)),
            },
            "outputs": {
                "out_file": ToolOutput(type="File", outputBinding=OutputBinding(glob=glob)),
            },
        }
    )


def make_cycle_doc() -> CWLDocument:
    wf = WorkflowNode(
        **{
            "class": "Workflow",
            "id": "#main",
            "inputs": {},
            "outputs": {},
            "steps": {
                "a": WorkflowStep(run="#a", **{"in": {"x": "b/out_file"}}, out=["out_file"]),
                "b": WorkflowStep(run="#b", **{"in": {"x": "a/out_file"}}, out=["out_file"]),
            },
        }
    )
    tool_a = make_tool(tool_id="a", glob="a.txt")
    tool_b = make_tool(tool_id="b", glob="b.txt")
    return CWLDocument(cwlVersion="v1.2", **{"$graph": [wf, tool_a, tool_b]})


def test_index_graph_splits_workflows_and_tools(linear_doc):
    workflows, tools = index_graph(linear_doc)

    assert "#main" in workflows
    assert "tool1" in tools
    assert "tool2" in tools


def test_select_workflow_by_ref_and_default(linear_doc):
    workflows, _ = index_graph(linear_doc)

    assert select_workflow(workflows, "#main").id == "#main"
    assert select_workflow(workflows, None).id == "#main"


def test_select_workflow_errors(linear_doc):
    # No workflows case
    tool = make_tool(tool_id="t", glob="x.txt")
    doc = CWLDocument(cwlVersion="v1.2", **{"$graph": [tool]})
    workflows, _ = index_graph(doc)

    with pytest.raises(ValidationError, match="No Workflow node found"):
        select_workflow(workflows, None)

    # Missing ref
    workflows2, _ = index_graph(linear_doc)
    with pytest.raises(ValidationError, match="not found"):
        select_workflow(workflows2, "#does_not_exist")


def test_workflow_step_run_id_validator_enforced():
    # step name "tool1" but run "#other" should fail
    with pytest.raises(ValidationError, match=r"steps\.tool1\.run must be '#tool1'"):
        WorkflowNode(
            **{
                "class": "Workflow",
                "id": "#main",
                "inputs": {},
                "outputs": {},
                "steps": {
                    "tool1": WorkflowStep(run="#other", **{"in": {}}, out=[]),
                },
            }
        )


def test_prepare_builds_templates_and_waves(tmp_path: Path, linear_doc):
    p = make_planner(linear_doc, tmp_path)

    tpl = p.prepare("#main")

    assert set(tpl.step_templates.keys()) == {"tool1", "tool2"}
    assert tpl.waves == [["tool1"], ["tool2"]]

    assert tpl.workflow_outputs["final_out"]["source_step"] == "tool2"
    assert tpl.workflow_outputs["final_out"]["source_port"] == "out_file"

    assert tpl.step_templates["tool1"].image == "python:3.11"
    assert str(tpl.step_templates["tool1"].outdir_container) == "/cwl_job/out"


def test_prepare_errors_on_missing_tool(tmp_path: Path):
    # Workflow references "#missing", but no corresponding tool id "missing"
    wf = WorkflowNode(
        **{
            "class": "Workflow",
            "id": "#main",
            "inputs": {},
            "outputs": {},
            "steps": {
                "missing": WorkflowStep(run="#missing", **{"in": {}}, out=[]),
            },
        }
    )
    doc = CWLDocument(cwlVersion="v1.2", **{"$graph": [wf]})

    p = make_planner(doc, tmp_path)
    with pytest.raises(ValidationError, match="unknown tool"):
        p.prepare("#main")


def test_prepare_cycle_detection(tmp_path: Path):
    doc = make_cycle_doc()
    p = make_planner(doc, tmp_path)

    with pytest.raises(ValueError, match="Cycle detected|unresolved deps"):
        p.prepare("#main")

def test_prepare_waves_multiple_steps_same_wave(tmp_path: Path):
    wf = WorkflowNode(
        **{
            "class": "Workflow",
            "id": "#main",
            "inputs": {"x": WorkflowInput(type="string")},
            "outputs": {
                "final": WorkflowOutput(type="File", outputSource="c/out_file")
            },
            "steps": {
                # No step-to-step deps (they only consume workflow input)
                "a": WorkflowStep(run="#a", **{"in": {"msg": "x"}}, out=["out_file"]),
                "b": WorkflowStep(run="#b", **{"in": {"msg": "x"}}, out=["out_file"]),
                # Depends on both a and b
                "c": WorkflowStep(
                    run="#c",
                    **{"in": {"from_a": "a/out_file", "from_b": "b/out_file"}},
                    out=["out_file"],
                ),
            },
        }
    )

    tool_a = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "a",
            "requirements": Requirements(
                DockerRequirement=DockerRequirement(
                    dockerPull="python:3.11",
                    dockerOutputDirectory=PurePosixPath("/cwl_job/out"),
                ),
                EnvVarRequirement=EnvVarRequirement(envDef={}),
            ),
            "baseCommand": "python",
            "inputs": {"msg": ToolInput(type="string", inputBinding=InputBinding(position=1))},
            "outputs": {"out_file": ToolOutput(type="File", outputBinding=OutputBinding(glob="a.txt"))},
        }
    )

    tool_b = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "b",
            "requirements": Requirements(
                DockerRequirement=DockerRequirement(
                    dockerPull="python:3.11",
                    dockerOutputDirectory=PurePosixPath("/cwl_job/out"),
                ),
                EnvVarRequirement=EnvVarRequirement(envDef={}),
            ),
            "baseCommand": "python",
            "inputs": {"msg": ToolInput(type="string", inputBinding=InputBinding(position=1))},
            "outputs": {"out_file": ToolOutput(type="File", outputBinding=OutputBinding(glob="b.txt"))},
        }
    )

    tool_c = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "c",
            "requirements": Requirements(
                DockerRequirement=DockerRequirement(
                    dockerPull="python:3.11",
                    dockerOutputDirectory=PurePosixPath("/cwl_job/out"),
                ),
                EnvVarRequirement=EnvVarRequirement(envDef={}),
            ),
            "baseCommand": "python",
            "inputs": {
                "from_a": ToolInput(type="File", inputBinding=InputBinding(position=1)),
                "from_b": ToolInput(type="File", inputBinding=InputBinding(position=2)),
            },
            "outputs": {"out_file": ToolOutput(type="File", outputBinding=OutputBinding(glob="c.txt"))},
        }
    )

    doc = CWLDocument(cwlVersion="v1.2", **{"$graph": [wf, tool_a, tool_b, tool_c]})
    p = make_planner(doc, tmp_path)

    tpl = p.prepare("#main")

    # topo_waves sorts wave elements, so we expect alphabetical ordering
    assert tpl.waves == [["a", "b"], ["c"]]

