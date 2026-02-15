# random_grep_count scatter image

This folder is a minimal, embeddable example to package and deploy the scatter CWL workflow using `prefect-cwl` on a Kubernetes work pool.

## What is inside

- `flow_scatter.py`: stable top-level Prefect flow using `create_executor_with_k8s_backend`
- `deploy.py`: registers a deployment in Prefect
- `Dockerfile`: builds an image that installs `prefect-cwl` from your GitHub repo

## Build image

From repo root:

```bash
docker build \
  -f sample_cwl/random_grep_count/scatter_k8s_image/Dockerfile \
  -t <registry>/random-grep-scatter:0.1.0 \
  --build-arg PREFECT_CWL_GIT_URL="https://github.com/planetekItalia/prefect-cwl.git@develop" \
  --build-arg PREFECT_VERSION="3.6.13" \
  .
```

```bash
docker push <registry>/random-grep-scatter:0.1.0
```

Pin `PREFECT_VERSION` to your server/worker version to avoid websocket/event protocol mismatches.


## Register deployment (manual K8s job)

Run a one-shot Kubernetes Job using that image and command `python deploy.py`.
A ready template is provided at `deploy.job.yaml`.

Required envs in that job container:

- `PREFECT_API_URL` (for example `http://prefect-server.prefect.svc.cluster.local:4200/api`)
- `PREFECT_IMAGE=<registry>/random-grep-scatter:0.1.0`

Optional envs:

- `PREFECT_WORK_POOL` (default `local-k8s`)
- `PREFECT_DEPLOYMENT_NAME` (default `random-grep-count-scatter`)
- `PREFECT_API_KEY` (required only if your Prefect API has authentication enabled)

Example container command/env snippet:

```yaml
command: ["python", "deploy.py"]
env:
  - name: PREFECT_API_URL
    value: "http://prefect-server.prefect.svc.cluster.local:4200/api"
  - name: PREFECT_IMAGE
    value: "<registry>/random-grep-scatter:0.1.0"
  - name: PREFECT_WORK_POOL
    value: "local-k8s"
```

## Run deployment

After `deploy.py` succeeds, trigger from UI or CLI.

Example CLI:

```bash
prefect deployment run "random-grep-count-scatter/random-grep-count-scatter" \
  -p random_string_number=100 \
  -p grep_string='["abc","cde"]' \
  -p augment_lines=100
```

The flow uses the worker/job-template merge behavior implemented in `prefect-cwl` when step jobs are spawned.
