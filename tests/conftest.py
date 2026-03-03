from pathlib import PurePosixPath

import pytest

from prefect_cwl.models import (
    WorkflowNode,
    CWLDocument,
    WorkflowInput,
    WorkflowOutput,
    WorkflowStep,
    CommandLineToolNode,
    Requirements,
    DockerRequirement,
    EnvVarRequirement,
    ToolInput,
    InputBinding,
    ToolOutput,
    OutputBinding,
)


@pytest.fixture
def linear_doc() -> CWLDocument:
    # Important for your WorkflowNode validator:
    # step name must match fragment after '#', so "tool1" must run "#tool1"
    wf = WorkflowNode(
        **{
            "class": "Workflow",
            "id": "#main",
            "inputs": {"greeting": WorkflowInput(type="string")},
            "outputs": {
                "final_out": WorkflowOutput(type="File", outputSource="tool2/out_file")
            },
            "steps": {
                "tool1": WorkflowStep(
                    run="#tool1", **{"in": {"msg": "greeting"}}, out=["out_file"]
                ),
                "tool2": WorkflowStep(
                    run="#tool2",
                    **{"in": {"upstream": "tool1/out_file"}},
                    out=["out_file"],
                ),
            },
        }
    )

    tool1 = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "tool1",
            "requirements": Requirements(
                DockerRequirement=DockerRequirement(
                    dockerPull="python:3.11",
                    dockerOutputDirectory=PurePosixPath("/cwl_job/out"),
                ),
                EnvVarRequirement=EnvVarRequirement(envDef={}),
            ),
            "baseCommand": "python",
            "inputs": {
                "msg": ToolInput(type="string", inputBinding=InputBinding(position=1)),
            },
            "outputs": {
                "out_file": ToolOutput(
                    type="File",
                    outputBinding=OutputBinding(glob="hello.txt"),
                ),
            },
        }
    )

    tool2 = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "tool2",
            "requirements": Requirements(
                DockerRequirement=DockerRequirement(
                    dockerPull="python:3.11",
                    dockerOutputDirectory=PurePosixPath("/cwl_job/out"),
                ),
                EnvVarRequirement=EnvVarRequirement(envDef={}),
            ),
            "baseCommand": "python",
            "inputs": {
                "upstream": ToolInput(
                    type="File", inputBinding=InputBinding(position=1)
                ),
            },
            "outputs": {
                "out_file": ToolOutput(
                    type="File",
                    outputBinding=OutputBinding(glob="result.txt"),
                ),
            },
        }
    )

    return CWLDocument(cwlVersion="v1.2", **{"$graph": [wf, tool1, tool2]})
