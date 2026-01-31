import asyncio
import tempfile
from pathlib import Path
from prefect_cwl import create_flow_with_docker_backend

inputs = dict(
        val_int=1,
        val_int_a=[2, 3, 4],
        val_flt=3.6,
        val_flt_a=[0.3, 0.5, 0.7],
        val_str="Test sentence",
        val_str_a=["String one", "String two", "String three"],
    )

with tempfile.TemporaryDirectory(delete=False) as tmpdir:
    with open(Path(__file__).parent / "multiple_inputs_checker.cwl") as inp:
        runnable_flow = create_flow_with_docker_backend(
            inp.read(), Path(tmpdir), workflow_id="#multiple_inputs_checker"
        )

    asyncio.run(runnable_flow(**inputs))