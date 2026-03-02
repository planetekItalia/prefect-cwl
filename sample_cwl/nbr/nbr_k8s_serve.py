import tempfile
from pathlib import Path
import yaml
from prefect_cwl import create_flow_with_k8s_backend

# Load inputs from the inputs file (for reference)
with open(Path(__file__).parent / "nbr_inputs.yml") as f:
    inputs = yaml.safe_load(f)

with tempfile.TemporaryDirectory(delete=False) as tmpdir:
    with open(Path(__file__).parent / "nbr.yml") as inp:
        runnable_flow = create_flow_with_k8s_backend(
            inp.read(), Path(tmpdir), workflow_id="#main"
        )

    # Serve the flow instead of running it immediately
    runnable_flow.serve()
