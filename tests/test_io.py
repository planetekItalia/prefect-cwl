import datetime as dt
from pathlib import PurePosixPath

from prefect_cwl.io import build_command_and_listing
from prefect_cwl.models import Requirements, EnvVarRequirement, InitialWorkDirRequirement, CommandLineToolNode, \
    ToolInput, \
    InputBinding, Listing, DockerRequirement


def _req(listing=None):
    return Requirements(
        EnvVarRequirement=EnvVarRequirement(envDef={}),
        InitialWorkDirRequirement=listing or InitialWorkDirRequirement(listing=[]),
        DockerRequirement=DockerRequirement(dockerPull="ubuntu:latest", dockerOutputDirectory=PurePosixPath("/tmp")),
    )


def test_basecommand_interpolation():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["$(workflow.bin)", "run"],
            "arguments": [],
            "inputs": {
                "n": ToolInput(type="int", inputBinding=InputBinding(prefix="--n", position=1))
            },
            "outputs": {},
        }
    )
    cmd, listing = build_command_and_listing(clt, {"inputs": {"n": 5}, "workflow": {"bin": "mytool"}})
    assert cmd == ["mytool", "run", "--n", "5"]
    assert listing == []


def test_inputs_position_and_prefix_scalar():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["echo"],
            "arguments": [],
            "inputs": {
                "n": ToolInput(type="int", inputBinding=InputBinding(prefix="--n", position=1))
            },
            "outputs": {},
        }
    )
    cmd, _ = build_command_and_listing(clt, {"inputs": {"n": 3}, "workflow": {}})
    assert cmd == ["echo", "--n", "3"]


def test_inputs_no_prefix_positioned():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "x": ToolInput(type="int", inputBinding=InputBinding(position=1))  # no prefix
            },
            "outputs": {},
        }
    )
    cmd, _ = build_command_and_listing(clt, {"inputs": {"x": 10}, "workflow": {}})
    assert cmd == ["tool", "10"]


def test_datetime_serialization_isoformat():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "when": ToolInput(type="datetime", inputBinding=InputBinding(prefix="--when", position=1))
            },
            "outputs": {},
        }
    )
    when = dt.datetime(2026, 1, 17, 9, 30, 0)
    cmd, _ = build_command_and_listing(clt, {"inputs": {"when": when}, "workflow": {}})
    assert cmd == ["tool", "--when", "2026-01-17T09:30:00"]


# TBV
def test_arguments_string_interpolation_single_token():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["echo"],
            "arguments": ["hello $(inputs.name) from $(workflow.project)"],
            "inputs": {
                "name": ToolInput(type="string", inputBinding=InputBinding(prefix="--name", position=1))
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"name": "alice"}, "workflow": {"project": "prefect-cwl"}},
    )

    # IMPORTANT: the entire interpolated string is one argv token
    assert cmd == ["echo", "--name", "alice", "hello alice from prefect-cwl"]

def test_arguments_string_interpolation_single_token_with_prefix():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["echo"],
            "arguments": ["-d", "$(inputs.name)"],
            "inputs": {
                "name": ToolInput(type="string", inputBinding=InputBinding(prefix="--name", position=1))
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"name": "alice"}, "workflow": {"project": "prefect-cwl"}},
    )

    # IMPORTANT: the entire interpolated string is one argv token
    assert cmd == ["echo", "--name", "alice", "-d", "alice"]

def test_arguments_valueFrom_single_token_interpolated():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["echo"],
            "arguments": [
                {"position": 2, "valueFrom": "hello $(inputs.name) from $(workflow.project)"},
            ],
            "inputs": {
                "name": ToolInput(type="string", inputBinding=InputBinding(prefix="--name", position=1)),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"name": "alice"}, "workflow": {"project": "prefect-cwl"}},
    )

    # single argv token for the whole rendered string
    assert cmd == ["echo", "--name", "alice", "hello alice from prefect-cwl"]


def test_arguments_multiple_entries_each_valueFrom_multiple_tokens():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["echo"],
            "arguments": [
                {"position": 1, "valueFrom": "hello"},
                {"position": 2, "valueFrom": "$(inputs.name)"},
                {"position": 3, "valueFrom": "from"},
                {"position": 4, "valueFrom": "$(workflow.project)"},
            ],
            "inputs": {},
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"name": "alice"}, "workflow": {"project": "prefect-cwl"}},
    )

    # each arguments entry is one argv token
    assert cmd == ["echo", "hello", "alice", "from", "prefect-cwl"]


def test_arguments_value_list_expands_to_multiple_tokens():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["echo"],
            "arguments": [
                {"position": 1, "value": ["hello", "$(inputs.name)", "from", "$(workflow.project)"]},
            ],
            "inputs": {},
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"name": "alice"}, "workflow": {"project": "prefect-cwl"}},
    )

    # list value explicitly expands to multiple argv tokens
    assert cmd == ["echo", "hello", "alice", "from", "prefect-cwl"]




def test_arguments_valueFrom_dict():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [{"position": 1, "valueFrom": "--flag=$(workflow.flag)"}],
            "inputs": {},
            "outputs": {},
        }
    )
    cmd, _ = build_command_and_listing(clt, {"inputs": {}, "workflow": {"flag": "ON"}})
    assert cmd == ["tool", "--flag=ON"]


def test_listing_interpolation():
    listing_req = InitialWorkDirRequirement(
        listing=[
            Listing(entryname=PurePosixPath("/cwl_job/$(inputs.name).txt"), entry="Hello $(inputs.name)!"),
        ]
    )
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(listing=listing_req),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {"name": ToolInput(type="string", inputBinding=InputBinding(prefix="--name", position=1))},
            "outputs": {},
        }
    )
    cmd, listing = build_command_and_listing(clt, {"inputs": {"name": "alice"}, "workflow": {}})
    assert cmd == ["tool", "--name", "alice"]
    assert listing == [{"entryname": "/cwl_job/alice.txt", "entry": "Hello alice!"}]



def test_using_prefix_and_position_orders_correctly():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "b": ToolInput(type="string", inputBinding=InputBinding(prefix="--b", position=2)),
                "a": ToolInput(type="string", inputBinding=InputBinding(prefix="--a", position=1)),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(clt, {"inputs": {"a": "A", "b": "B"}, "workflow": {}})
    assert cmd == ["tool", "--a", "A", "--b", "B"]



def test_list_item_separator_joins_values_single_prefix_with_separator():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "ids": ToolInput(
                    type="int[]",
                    inputBinding=InputBinding(prefix="--ids", position=1, separator=","),
                ),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(clt, {"inputs": {"ids": [1, 2, 3]}, "workflow": {}})
    assert cmd == ["tool", "--ids", "1,2,3"]

def test_list_item_separator_joins_values_single_prefix():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "ids": ToolInput(
                    type="int[]",
                    inputBinding=InputBinding(prefix="--ids", position=1),
                ),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(clt, {"inputs": {"ids": [1, 2, 3]}, "workflow": {}})
    assert cmd == ["tool", "--ids", "1", "2", "3"]

def test_list_item_separator_joins_values_single_prefix_for_str():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "ids": ToolInput(
                    type="string[]",
                    inputBinding=InputBinding(prefix="--ids", position=1),
                ),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(clt, {"inputs": {"ids": [1, 2, 3]}, "workflow": {}})
    assert cmd == ["tool", "--ids", "1", "2", "3"]

def test_list_item_separator_joins_values_single_prefix_for_str_with_multiple_tokens():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["tool"],
            "arguments": [],
            "inputs": {
                "ids": ToolInput(
                    type="string[]",
                    inputBinding=InputBinding(prefix="--ids", position=1),
                ),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(clt, {"inputs": {"ids": ["this", "is a bug"]}, "workflow": {}})
    assert cmd == ["tool", "--ids", "this", "is a bug"]

def test_positional_int_without_prefix_then_list_with_prefix_and_separator():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["command"],
            "arguments": [],
            "inputs": {
                # int with position, but NO prefix -> appears as bare token
                "i": ToolInput(type="int", inputBinding=InputBinding(position=1)),
                # list with prefix + separator -> single joined token after prefix
                "n": ToolInput(type="int[]", inputBinding=InputBinding(prefix="--n", separator=",")),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"i": 1, "n": [3, 3, 4]}, "workflow": {}},
    )
    print(cmd)

    assert cmd == ["command", "1", "--n", "3,3,4"]

def test_fill_array_in_arg():
    clt = CommandLineToolNode(
        **{
            "class": "CommandLineTool",
            "id": "t",
            "requirements": _req(),
            "baseCommand": ["command"],
            "arguments": ["$(inputs.n[0])", "$(inputs.n[1])", "$(inputs.n[2])"],
            "inputs": {
                # int with position, but NO prefix -> appears as bare token
                "i": ToolInput(type="int", inputBinding=InputBinding(position=1)),
                # list with prefix + separator -> single joined token after prefix
                "n": ToolInput(type="int[]", inputBinding=InputBinding(prefix="--n", separator=",")),
            },
            "outputs": {},
        }
    )

    cmd, _ = build_command_and_listing(
        clt,
        {"inputs": {"i": 1, "n": [3, 3, 4]}, "workflow": {}},
    )

    assert cmd == ["command", "1", "3", "3", "4", "--n", "3,3,4" ]
