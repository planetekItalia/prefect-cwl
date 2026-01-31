from pathlib import Path

import pytest

from constants import JOBROOT
from models import CWLDocument
from prefect_cwl.planner.planner import index_graph
from prefect_cwl.exceptions import ValidationError
from prefect_cwl.planner.templates import validate_and_materialize_listings, compute_out_artifacts

def test_validate_and_materialize_listings_ok(tmp_path: Path):
    host_jobdir = tmp_path / "job"
    rendered = [
        {"entryname": str(JOBROOT / "inputs" / "hello.txt"), "entry": "hi"},
        {"entryname": str(JOBROOT / "subdir" / "a.txt"), "entry": "aaa"},
    ]

    mats = validate_and_materialize_listings(rendered_listing=rendered, host_jobdir=host_jobdir)
    assert len(mats) == 2

    assert mats[0].host_path == host_jobdir / "inputs" / "hello.txt"
    assert mats[0].content == "hi"


def test_validate_and_materialize_listings_rejects_outside_jobroot(tmp_path: Path):
    host_jobdir = tmp_path / "job"
    rendered = [{"entryname": "/not_jobroot/x.txt", "entry": "nope"}]

    with pytest.raises(ValidationError, match="must be under"):
        validate_and_materialize_listings(rendered_listing=rendered, host_jobdir=host_jobdir)


def test_compute_out_artifacts_maps_globs(tmp_path: Path, linear_doc: CWLDocument):
    _, tools = index_graph(linear_doc)
    clt = tools["tool1"]

    host_outdir = tmp_path / "out"
    out = compute_out_artifacts(clt=clt, host_outdir=host_outdir)

    assert out["out_file"] == host_outdir / "hello.txt"