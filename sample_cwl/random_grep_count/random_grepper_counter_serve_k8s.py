import random
import string
from pathlib import Path

from prefect_cwl import create_flow_with_k8s_backend

if __name__ == "__main__":
    random_string = "".join(random.choice(string.ascii_letters) for _ in range(10))
    with open(Path(__file__).parent / "random_grep_count.cwl") as inp:
        runnable_flow = create_flow_with_k8s_backend(
            inp.read(), Path(f"/data/{random_string}"), workflow_id="#random_grep_count"
        )
        runnable_flow.serve()
