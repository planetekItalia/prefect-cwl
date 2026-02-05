import random
import string
import tempfile
from pathlib import Path
from prefect_cwl import create_flow_with_k8s_backend

inputs = {
    "random_string_number": 100,
    "grep_string": "abc",
}

with tempfile.TemporaryDirectory(delete=False) as tmpdir:
    random_string = "".join(random.choice(string.ascii_letters) for _ in range(10))
    with open(Path(__file__).parent / "random_grep_count.cwl") as inp:
        runnable_flow = create_flow_with_k8s_backend(
            inp.read(), Path(tmpdir), workflow_id="#random_grep_count"
        )

    # asyncio.run(runnable_flow(**inputs))
    runnable_flow.serve()
