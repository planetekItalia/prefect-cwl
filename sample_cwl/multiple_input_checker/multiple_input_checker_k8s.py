import asyncio
import random
import string
from pathlib import Path
from prefect_cwl import create_flow_with_k8s_backend

inputs = dict(
        val_int=1,
        val_int_a=[2, 3, 4],
        val_flt=3.6,
        val_flt_a=[0.3, 0.5, 0.7],
        val_str="Test sentence",
        val_str_a=["String one", "String two", "String three"],
    )

with open(Path(__file__).parent / "multiple_inputs_checker.cwl") as inp:
    random_string = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    runnable_flow = create_flow_with_k8s_backend(
        inp.read(), Path(f"/data/{random_string}"), workflow_id="#multiple_inputs_checker"
    )

asyncio.run(runnable_flow(**inputs))