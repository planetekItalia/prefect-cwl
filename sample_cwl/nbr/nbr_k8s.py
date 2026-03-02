import asyncio
import tempfile
from pathlib import Path
import yaml
from prefect_cwl import create_flow_with_k8s_backend

# Load inputs from the inputs file
with open(Path(__file__).parent / "nbr_inputs.yml") as f:
    inputs = yaml.safe_load(f)

with open(Path(__file__).parent / "nbr.yml") as inp:
    runnable_flow = create_flow_with_k8s_backend(
        inp.read(), Path("/data/hrbucd"), workflow_id="#main"
    )

asyncio.run(runnable_flow(**inputs))
