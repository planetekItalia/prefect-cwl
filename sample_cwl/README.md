# Some samples

This directory contains some sample CWLs aimed at demonstrating how this library works.
You can analyze how this CWL has been created to know what this library supports, in practice.
Move to the specific directory, then run the *python* script inside.
If you pick the *K8s* version, be sure you got your *KUBECONFIG* env variable correctly pointing to your cluster.
For Kubernetes runs deployed through a Prefect work pool, `prefect-cwl` supports
runtime merge of worker/job-template variables into spawned CWL step jobs
(`namespace`, `serviceAccountName`, `volumes`, `volumeMounts`, and `env`), with
`prefect-cwl` required PVC constraints preserved. This merge behavior is **K8s only**.
See `DESIGN.md` for the detailed behavior and precedence rules.
`PREFECT_CWL_K8S_PVC_MOUNT_PATH` is the canonical in-container root used by `prefect-cwl` for run workspace creation on the shared PVC.
You can tune backend verbosity per deployment by setting `PREFECT_CWL_K8S_LOG_LEVEL` / `PREFECT_CWL_K8S_STREAM_LOG_LEVEL` in deployment/work-pool env.

At the current time we have:

- *Random Grepper Counter*: Generate a file with *N* random strings, grep using one or more predicates, then count matching lines.
  - CWL (base, no scatter): `random_grep_count/random_grep_count.cwl`
  - Runner (Docker): `random_grep_count/random_grepper_counter.py`
  - Runner (K8s): `random_grep_count/random_grepper_counter_k8s.py`
  - Purpose: baseline 3-step chain `randomizer -> grepper -> counter`.
  - CWL (base + `ResourceRequirement` subset): `random_grep_count/random_grep_count_with_resources.cwl`
  - Runner (Docker): `random_grep_count/random_grepper_counter_with_resources.py`
  - Purpose: same baseline chain with per-step `coresMin/coresMax/ramMin/ramMax`.
  - To run with `cwltool`:
  ```bash
    cwltool random_grep_count/random_grep_count.cwl#random_grep_count --grep_string <grep_string> --random_string_number <random_string_number>
  ```
  - CWL (scatter with augmenter): `random_grep_count/random_grep_count_scatter.cwl`
  - Runner (Docker): `random_grep_count/random_grepper_counter_scatter.py`
  - Inputs file: `random_grep_count/input_scatter.yml`
  - Purpose: scatter `grepper` over `grep_string[]`, then scatter `augmenter` over gathered grep outputs, then count all resulting directories.
  - CWL (scatter + first-item gather at tool level): `random_grep_count/random_grep_count_scatter_gather_grep.cwl`
  - Runner (Docker): `random_grep_count/random_grepper_counter_scatter_gather_grep.py`
  - Inputs file: `random_grep_count/input_scatter_gather_grep.yml`
  - Purpose: scatter `grepper` over `grep_string[]`, then pass gathered `Directory[]` to `counter` and select only index `0` via tool `arguments.valueFrom`.
  - Prefect deploy examples:
    - Local serve: `random_grep_count/random_grepper_counter_serve.py`
    - K8s serve: `random_grep_count/random_grepper_counter_serve_k8s.py`
    - Embeddable image + manual deploy job: `random_grep_count/scatter_k8s_image/`
- *Multiple Input Checker*: Test all the inputs supported by the adapter and persist them to a file
  - To run workflow cwl execute
  ```bash
    cwltool multiple_input_checker/multiple_inputs_checker.cwl#multiple_inputs_checker multiple_input_checker/inputs.yml
  ```
- *Dockerized PySTAC Action:* Two step composed workflow witch use docker images that download and item from STAC catalogue and crop it
  - To run workflow cwl execute
  ```bash
    cwltool dockerized_pystac_actions/dockerized_pystac_actions.cwl#cwl2prefect dockerized_pystac_actions/inputs.yml
  ```
  - To run workflow cwl execute
  ```bash
    cwltool dockerized_pystac_actions/dockerized_pystac_actions_parallel_crop.cwl#cwl2prefect dockerized_pystac_actions/inputs_parallel_crop.yml
  ```
- *NDVI*: Generate a NDVI image from Sentinel-2 data
  - To run workflow cwl execute
  ```bash
    cwltool ndvi/ndvi.cwl#ndvi ndvi/inputs.yml
  ```

## About deploying

In order to deploy flows inside the Prefect server, you can use the following approach:

- create a file `my_flow.py` with a similar content:
```python

import os
import random
import string
from pathlib import Path
from typing import Any
from prefect import flow
from prefect_cwl import create_executor_with_k8s_backend


@flow
async def my_algorithm(my_params: Any):
    """My algorithm via CWL.

    Args:
        my_params (Any): My parameters.
    """
    random_string = "".join(random.choice(string.ascii_letters) for _ in range(10))

    cwl_content = open(Path(__file__).parent / "my.cwl").read()
    executor = create_executor_with_k8s_backend(
        workflow_text = cwl_content,
        host_work_dir = Path(f"{os.environ.get("PREFECT_CWL_K8S_PVC_MOUNT_PATH")}/{random_string}"),
        workflow_id= "#id"
    )
    await executor(my_params=my_params)

if __name__ == "__main__":

    my_algorithm.deploy(
        name="my-algorithm",
        tags=["user"],
        work_pool_name="worker-user-pool",
        build=False,
        image=f"my_image:my_tag",
        version=image_tag,
        description="POC from prefect-cwl",
        concurrency_limit=1
    )

```

And then a simple Dockerfile:

```dockerfile
FROM prefecthq/prefect:3.6.13-python3.12

RUN mkdir /app
WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt
RUN rm /app/requirements.txt

COPY src/ /app/src

ENV PYTHONPATH=/app
ENV PREFECT_CWL_K8S_NAMESPACE=data

# ANY ENV VAR YOU NEED as per docs
```

When you build the Dockerfile and push the image, you can deploy the flow running the code inside the `my_flow.py:__main__` and then you should see it inside your Prefect Server and run it from the GUI.
