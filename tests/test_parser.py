import pytest
from pathlib import PurePosixPath

from prefect_cwl.models import (
    CommandLineToolNode,
    Requirements,
    Listing,
    ToolOutput,
    OutputBinding,
    WorkflowNode,
    CWLDocument,
)
from prefect_cwl.exceptions import ValidationError as CustomValidationError


def minimal_requirements(**overrides) -> Requirements:
    base = {
        "DockerRequirement": {
            "dockerPull": "alpine:3",
            "dockerOutputDirectory": "/cwl_job/out",
        },
        "EnvVarRequirement": {"FOO": "bar"},
        "InitialWorkDirRequirement": {"listing": []},
    }
    base.update(overrides)
    return Requirements(**base)


def test_docker_output_directory_must_be_absolute_raises_custom_error():
    # dockerOutputDirectory must be absolute; validator raises our CustomValidationError
    req = {
        "DockerRequirement": {
            "dockerPull": "alpine:3",
            "dockerOutputDirectory": "relative/path",  # invalid
        }
    }

    with pytest.raises(CustomValidationError) as ei:
        CommandLineToolNode(
            **{
                "class": "CommandLineTool",
                "id": "t",
                "requirements": Requirements(**req),
                "baseCommand": ["echo"],
                "arguments": [],
                "inputs": {},
                "outputs": {},
            }
        )
    assert "dockerOutputDirectory must be absolute" in str(ei.value)


@pytest.mark.parametrize(
    "glob",
    [
        "/absolute.txt",     # absolute not allowed
        "data/*.txt",        # wildcard not allowed
        "**/deep/file.txt",  # double-star not allowed
        "../escape.txt",     # parent traversal not allowed
        "",                   # empty not allowed
    ],
)
def test_output_binding_glob_validation_errors(glob):
    with pytest.raises(ValueError) as ei:
        ToolOutput(
            type="File",
            outputBinding=OutputBinding(glob=glob),
        )
    # Spot-check message mentions "glob" and the reason
    assert "glob" in str(ei.value)


def test_listing_entryname_must_be_under_jobroot():
    # Listing.entryname must be under JOBROOT (default /cwl_job)
    with pytest.raises(ValueError) as ei:
        Listing(entryname=PurePosixPath("/tmp/file.txt"), entry="hello")
    assert "Listing entryname must be under" in str(ei.value)


def test_workflow_step_fragment_mismatch_raises_custom_error():
    # When using fragment refs ("#name"), the name must match the step key.
    with pytest.raises(CustomValidationError) as ei:
        WorkflowNode(
            **{
                "class": "Workflow",
                "id": "wf",
                "inputs": {},
                "outputs": {},
                "steps": {
                    # key is "download" but run points to "#other" -> invalid
                    "download": {"run": "#other", "in": {}, "out": []}
                },
            }
        )
    assert "steps.download.run must be '#download'" in str(ei.value)


def test_successful_cwl_document_marshalling():
    # Positive control: build a minimal but valid CWL document graph
    clt = {
        "class": "CommandLineTool",
        "id": "echo",
        "requirements": {
            "DockerRequirement": {
                "dockerPull": "alpine:3",
                "dockerOutputDirectory": "/cwl_job/out",
            },
            "InitialWorkDirRequirement": {"listing": [
                {"entryname": "/cwl_job/msg.txt", "entry": "hello"}
            ]},
        },
        "baseCommand": ["echo"],
        "arguments": [],
        "inputs": {
            "msg": {
                "type": "string",
                "inputBinding": {"prefix": "--msg", "position": 1},
            }
        },
        "outputs": {
            "o": {"type": "File", "outputBinding": {"glob": "msg.txt"}}
        },
    }

    doc = CWLDocument(
        **{
            "cwlVersion": "v1.2",
            "$graph": [clt],
        }
    )

    assert isinstance(doc, CWLDocument)
    assert len(doc.graph) == 1
    node = doc.graph[0]
    # Discriminator should produce a CommandLineToolNode instance
    assert isinstance(node, CommandLineToolNode)
    assert node.id == "echo"
