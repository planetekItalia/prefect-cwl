import tempfile
from pathlib import Path

from prefect_cwl import create_flow_with_docker_backend

if __name__ == "__main__":
    with tempfile.TemporaryDirectory(delete=False) as tmpdir:
        with open(Path(__file__).parent / "random_grep_count.cwl") as inp:
            runnable_flow = create_flow_with_docker_backend(
                inp.read(), Path(tmpdir), workflow_id="#random_grep_count"
            )
            runnable_flow.serve()
