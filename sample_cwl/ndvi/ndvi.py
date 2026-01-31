import asyncio
import random
import string
import tempfile
from pathlib import Path

from prefect_cwl import create_flow_with_docker_backend

with tempfile.TemporaryDirectory(delete=False) as tmpdir:
    random_string = "".join(random.choice(string.ascii_letters) for _ in range(10))
    inputs = dict(
        spatial_extent=["16.786594","41.077410", "16.935425", "41.141692"]
    )
    with tempfile.TemporaryDirectory(delete=False) as tmpdir:
        with open(Path(__file__).parent / "ndvi.cwl") as inp:
            runnable_flow = create_flow_with_docker_backend(
                inp.read(), Path(tmpdir), workflow_id="#ndvi"
            )

        asyncio.run(runnable_flow(**inputs))