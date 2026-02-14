<div align="center">

  ![Logo](./static/logo.png "Logo")
  # Prefect CWL
</div>

A lightweight adapter that bridges the *Common Workflow Language* (CWL) world with *Prefect*.
It not only executes CWL but lets you orchestrate it with Prefect’s scheduling, retries, observability, and deployments (this a WIP, actually).
Execution is pluggable via backends, starting with Docker and with Kubernetes as a forthcoming option.

In this library, the atomic unit is a single CWL step (a `CommandLineTool` or workflow step), not an entire workflow/flow.
Prefect orchestrates those steps according to the CWL-defined dependencies.

## What this achieves
- **Bridge CWL and Prefect**: Parse CWL, build a dependency graph, and run steps under Prefect orchestration.
- **Orchestrate, not just execute**: Use Prefect’s UI, scheduling, retries, mapping, and deployments to operate CWL workloads.
- **Pluggable execution backends**: Run each CWL step via Docker today; Kubernetes support is planned.

## Key concepts
- **Atomic unit = CWL step**: Each CWL step is executed as a Prefect task invocation via a backend. Prefect orchestrates the order and parallelism.
- **Dependency “waves”**: Steps run in parallel when their dependencies are satisfied; no artificial serialization.
- **Typed IR**: CWL is parsed into a typed internal representation that drives orchestration and I/O wiring.

## Features
- Parse a practical subset of CWL v1.2 (tools, workflows, requirements, inputs/outputs).
- Build a dependency graph and infer parallel “waves”.
- Generate a Prefect flow whose signature mirrors CWL workflow inputs.
- Execute steps via a backend that handles containers, arguments, volumes, and exit codes.
- Initial backend: **Docker** and **Kubernetes**.

## Current limitations

- The adapter needs the explicit *WRK_DIR* env variable, set up a *current working directory* when running Docker container/K8s Job
- No glob supported, aside from simple *folder names*
- Data among steps shall be passed with directory. Each step shall then read the previous output saved somehow to those files
- Names for steps and input/output reference shall be the same
A more in-depth list can be checked out inside the *DESIGN* file.

Check *sample_cwl* folder for those limits in practice.

## Backends
- **Docker backend**: Uses Prefect’s Docker primitives to pull images, mount volumes, and execute commands.
- **Kubernetes backend (WIP)**: Same interface; schedule Pods/Jobs to run each CWL step.

## Quick start

After installing all the requirements, start *Prefect Server first*:

```sh
prefect server start
```

Then, create a new project:

```
mkdir this-is-just-the-client-callign
uv init
```

and install the library (with the *uv* CLI and *Docker* or *K8s* backend or both):

```
uv add "prefect-cwl[docker]"
uv add "prefect-cwl[k8s]"
```

from your shell:

```python
from prefect_cwl import create_flow_with_docker_backend
with open("myflow.cwl") as inp:
    runnable_flow = create_flow_with_docker_backend(
        inp.read(), Path("/tmp"), workflow_id="#flow_id"
    )

asyncio.run(runnable_flow(**inputs))

```

The *runnable_flow* is a Prefect flow that can be scheduled, deployed, and run as any other Prefect flow.

Shall you want to use *K8s* backend, special requirements apply:

- a running K8s cluster
- a PVC installed and deployed and usable by Prefect
- the following environment env vars set, if needed:
  - `KUBECONFIG`, for custom configuration
  - `PREFECT_CWL_K8S_NAMESPACE`, for custom namespace (default: `prefect`)
  - `PREFECT_CWL_K8S_PVC_NAME`, for custom PVC name (default: `prefect-shared-pvc`)
  - `PREFECT_CWL_K8S_PVC_MOUNT_PATH`, for custom PVC mount path (default: `/data`)
  - `PREFECT_CWL_K8S_SERVICE_ACCOUNT_NAME`, for custom service account name (default: `prefect-flow-runner`)
  - `PREFECT_CWL_K8S_PULL_SECRETS`, for custom pull secrets (default: `[]`)

For running a local K8s cluster, configured with Prefect and all the above requirements, check the *prefect-k8s-demo* folder.

## Runtime concurrency controls

`prefect-cwl` supports CWL scatter with runtime guardrails:

- `PREFECT_CWL_SCATTER_CONCURRENCY` (default: `4`): local in-flow throttling for submitted scattered runs. Set `0` or negative to disable this local gate.
- `PREFECT_CWL_SCATTER_TAG` (default: `prefect-cwl-scatter`): Prefect tag attached to `run_step` task submissions. Set to empty to disable tag attachment.

To enforce a hard orchestration limit, create a Prefect concurrency limit on that tag:

```bash
prefect concurrency-limit create prefect-cwl-scatter 8
prefect concurrency-limit inspect prefect-cwl-scatter
```

Notes:
- The Prefect tag limit is the hard control across workers/flows using the same API/server.
- The local `PREFECT_CWL_SCATTER_CONCURRENCY` gate is process-local and complements (does not replace) Prefect tag limits.
- With current implementation, `PREFECT_CWL_SCATTER_TAG` is applied to all `run_step` task submissions, including non-scattered runs.

## Install the library locally

Prerequisite: install `uv` (https://github.com/astral-sh/uv). Once `uv` has been installed successfully, move in the project folder and use:

```bash
uv sync --all-extras --group dev
```

Be sure to set the *PYTHONPATH* variable to *prefect_cwl* directory.
Alternatively, use the command `echo PYTHONPATH=$PWD`, to set the path pointing to the current folder.
Otherwise, install it into *editable* mode. Should you run tests, install *dev* dependencies.

Start the Prefect server using the command:

```bash
uv run prefect server start
```

Now we can run the python script using the command:

```bash
uv run <file_path>
```

## Sample CWL (WIP)
See `sample_cwl/` for ready-to-run examples you can use to test the library. These are work-in-progress and may evolve as the adapter expands CWL coverage and features.

## Project status
Early-stage and evolving. Expect changes in models, supported CWL features, and backend interfaces as we harden the adapter.

## Design
The package design is detailed in `DESIGN.md` and reflects the latest codebase, including planning vs execution for Docker and Kubernetes backends.
