from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from prefect_cwl.backends.k8s import K8sBackend
from prefect_cwl.planner.templates import StepPlan, ListingMaterialization


class DummyStepTemplate:
    def __init__(self, step_name: str, plan: StepPlan):
        self.step_name = step_name
        self._plan = plan

    def materialize_step(
        self, *, workflow_inputs, produced, workspace, render_io, **kwargs
    ):  # type: ignore[no-untyped-def]
        return self._plan


class CapturingStepTemplate:
    def __init__(self, step_name: str, plan: StepPlan):
        self.step_name = step_name
        self._plan = plan
        self.seen_kwargs = None

    def materialize_step(
        self, *, workflow_inputs, produced, workspace, render_io, **kwargs
    ):  # type: ignore[no-untyped-def]
        self.seen_kwargs = kwargs
        return self._plan


@pytest.mark.asyncio
async def test_k8s_backend_success(tmp_path, monkeypatch):
    pvc_root = "/data"
    outdir = Path(pvc_root) / "runs" / "out"
    jobdir = Path(pvc_root) / "runs" / "job"
    listing_path = jobdir / "hello.txt"

    plan = StepPlan(
        step_name="s",
        tool_id="t",
        image="alpine:3.19",
        argv=["sh", "-lc", "echo ok"],
        outdir_container=Path("/out"),
        volumes={
            str(outdir): "/out:rw",
            str(jobdir): "/cwl_job:rw",
        },
        listings=[ListingMaterialization(host_path=listing_path, content="hi")],
        out_artifacts={"o": outdir / "x.txt"},
        envs={"A": "1"},
    )

    step = DummyStepTemplate("s", plan)

    submitted = []

    class FakeJobRun:
        def __init__(self, result=None):
            self.wait_for_completion = AsyncMock()
            self.fetch_result = AsyncMock(return_value=result)

    class FakeK8sJob:
        def __init__(self, namespace, v1_job):
            self.namespace = namespace
            self.v1_job = v1_job
            self._job_run = FakeJobRun(result={"ok": True})

        async def trigger(self):
            submitted.append(self.v1_job)
            return self._job_run

    # Patch the Kubernetes objects
    monkeypatch.setattr("prefect_cwl.backends.k8s.KubernetesJob", FakeK8sJob)

    backend = K8sBackend(namespace="ns", pvc_name="pvc", pvc_mount_path=pvc_root)

    produced = {}
    await backend.call_single_step(
        step, workflow_inputs={}, produced=produced, workspace=tmp_path
    )

    # Expect up to three jobs (mkdir, listings, run). mkdir may be skipped if dirs empty, but here not empty
    assert len(submitted) == 3

    mkdir_job, listings_job, run_job = submitted

    # Basic manifest checks
    assert mkdir_job["kind"] == "Job"
    assert run_job["metadata"]["namespace"] == "ns"

    # Run job container spec
    container = run_job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "alpine:3.19"
    assert container["command"] == ["sh", "-lc", "echo ok"]
    # env mapped
    env_map = {e["name"]: e["value"] for e in container["env"]}
    assert env_map == {"A": "1"}
    # Has base mount and extra mounts with subPath
    mounts = container["volumeMounts"]
    # base mount at pvc root
    assert any(m.get("mountPath") == pvc_root for m in mounts)

    # extra mounts include /out and /cwl_job mapped with subPaths from pvc_root
    # Compute expected subpaths
    def sp(p):
        return str(Path(p).relative_to(pvc_root)).replace("\\", "/")

    expected_subs = {sp(str(outdir)): "/out", sp(str(jobdir)): "/cwl_job"}
    subs = {m["subPath"]: m["mountPath"] for m in mounts if "subPath" in m}
    for sub, mpath in expected_subs.items():
        assert subs[sub] == mpath

    # Produced mapping updated
    assert produced[("s", "o")] == outdir / "x.txt"


@pytest.mark.asyncio
async def test_k8s_backend_failure_raises(tmp_path, monkeypatch):
    pvc_root = "/data"
    outdir = Path(pvc_root) / "o"
    jobdir = Path(pvc_root) / "j"

    plan = StepPlan(
        step_name="s",
        tool_id="t",
        image="alpine",
        argv=["false"],
        outdir_container=Path("/out"),
        volumes={str(outdir): "/out:rw", str(jobdir): "/cwl_job:rw"},
        listings=[],
        out_artifacts={"o": outdir / "x.txt"},
        envs={},
    )
    step = DummyStepTemplate("s", plan)

    class FakeJobRun:
        def __init__(self, result=None):
            self.wait_for_completion = AsyncMock()
            self.fetch_result = AsyncMock(return_value=result)

    class FakeK8sJob:
        def __init__(self, namespace, v1_job):
            self.namespace = namespace
            self.v1_job = v1_job
            self._job_run = FakeJobRun(result={"ok": True})

        async def trigger(self):
            raise RuntimeError("job failed")

    def fake_run_namespaced_job_fn(*, kubernetes_job, print_func):  # type: ignore[no-untyped-def]
        raise RuntimeError("job failed")

    monkeypatch.setattr("prefect_cwl.backends.k8s.KubernetesJob", FakeK8sJob)

    backend = K8sBackend(namespace="ns", pvc_name="pvc", pvc_mount_path=pvc_root)

    with pytest.raises(RuntimeError):
        await backend.call_single_step(
            step, workflow_inputs={}, produced={}, workspace=tmp_path
        )


@pytest.mark.asyncio
async def test_k8s_backend_passes_suffix_and_overrides_to_materialize(
    tmp_path, monkeypatch
):
    pvc_root = "/data"
    outdir = Path(pvc_root) / "o"
    jobdir = Path(pvc_root) / "j"
    override_dir = Path(pvc_root) / "upstream" / "out"

    plan = StepPlan(
        step_name="augmenter",
        tool_id="t",
        image="alpine",
        argv=["true"],
        outdir_container=Path("/out"),
        volumes={str(outdir): "/out:rw", str(jobdir): "/cwl_job:rw"},
        listings=[],
        out_artifacts={"o": outdir / "x.txt"},
        envs={},
    )
    step = CapturingStepTemplate("augmenter", plan)

    class FakeJobRun:
        def __init__(self, result=None):
            self.wait_for_completion = AsyncMock()
            self.fetch_result = AsyncMock(return_value=result)

    class FakeK8sJob:
        def __init__(self, namespace, v1_job):
            self._job_run = FakeJobRun(result={"ok": True})

        async def trigger(self):
            return self._job_run

    monkeypatch.setattr("prefect_cwl.backends.k8s.KubernetesJob", FakeK8sJob)

    backend = K8sBackend(namespace="ns", pvc_name="pvc", pvc_mount_path=pvc_root)
    await backend.call_single_step(
        step_template=step,
        workflow_inputs={},
        produced={},
        workspace=tmp_path,
        job_suffix="augmenter_input-0",
        input_overrides={"augmenter_input": override_dir},
    )

    assert step.seen_kwargs == {
        "job_suffix": "augmenter_input-0",
        "input_overrides": {"augmenter_input": override_dir},
    }
