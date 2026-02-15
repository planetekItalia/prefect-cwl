from pathlib import Path
import logging
from unittest.mock import AsyncMock

import pytest

from prefect_cwl.backends.k8s import K8sBackend
from prefect_cwl.planner.templates import (
    StepPlan,
    ListingMaterialization,
    StepResources,
)


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


@pytest.mark.asyncio
async def test_k8s_backend_merges_runtime_job_variables(tmp_path, monkeypatch):
    pvc_root = "/data"
    outdir = Path(pvc_root) / "runs" / "out"
    jobdir = Path(pvc_root) / "runs" / "job"

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
        listings=[],
        out_artifacts={"o": outdir / "x.txt"},
        envs={"A": "step", "B": "step"},
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
            submitted.append((self.namespace, self.v1_job))
            return self._job_run

    class FakeRunContext:
        class _FlowRun:
            job_variables = {
                "namespace": "pool-ns",
                "service_account_name": "pool-sa",
                "env": {"A": "runtime", "C": "runtime"},
                "volumes": [
                    {"name": "cache", "emptyDir": {}},
                ],
                "volume_mounts": [
                    {"name": "cache", "mountPath": "/cache"},
                    {"name": "override-root", "mountPath": pvc_root},
                ],
            }

        flow_run = _FlowRun()

    monkeypatch.setattr("prefect_cwl.backends.k8s.KubernetesJob", FakeK8sJob)
    monkeypatch.setattr(
        "prefect_cwl.backends.k8s.get_run_context", lambda: FakeRunContext()
    )

    backend = K8sBackend(
        namespace="default-ns",
        pvc_name="pvc-main",
        pvc_mount_path=pvc_root,
        service_account_name="default-sa",
    )

    produced = {}
    await backend.call_single_step(
        step, workflow_inputs={}, produced=produced, workspace=tmp_path
    )

    # mkdir + run (no listings)
    assert len(submitted) == 2
    run_job = submitted[-1][1]
    run_ns = submitted[-1][0]

    assert run_ns == "default-ns"
    assert run_job["metadata"]["namespace"] == "default-ns"

    pod_spec = run_job["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "default-sa"

    # Ensure runtime volumes are included and required work volume is preserved
    volumes = {v["name"]: v for v in pod_spec["volumes"]}
    assert "cache" in volumes
    assert volumes["work"]["persistentVolumeClaim"]["claimName"] == "pvc-main"

    container = pod_spec["containers"][0]
    mounts = container["volumeMounts"]
    assert any(
        m.get("name") == "cache" and m.get("mountPath") == "/cache" for m in mounts
    )
    # Ensure root mount path is owned by required work volume, not runtime override
    assert any(
        m.get("name") == "work" and m.get("mountPath") == pvc_root for m in mounts
    )

    env_map = {e["name"]: e["value"] for e in container["env"]}
    # Step env overrides runtime values on key collision
    assert env_map["A"] == "step"
    assert env_map["B"] == "step"
    assert env_map["C"] == "runtime"


@pytest.mark.asyncio
async def test_k8s_backend_maps_step_resources_to_container_resources(
    tmp_path, monkeypatch
):
    pvc_root = "/data"
    outdir = Path(pvc_root) / "runs" / "out"
    jobdir = Path(pvc_root) / "runs" / "job"

    plan = StepPlan(
        step_name="s",
        tool_id="t",
        image="alpine:3.19",
        argv=["sh", "-lc", "echo ok"],
        outdir_container=Path("/out"),
        volumes={str(outdir): "/out:rw", str(jobdir): "/cwl_job:rw"},
        listings=[],
        out_artifacts={"o": outdir / "x.txt"},
        envs={},
        resources=StepResources(
            cpu_request=1,
            cpu_limit=2,
            memory_request_mb=512,
            memory_limit_mb=1024,
        ),
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

    monkeypatch.setattr("prefect_cwl.backends.k8s.KubernetesJob", FakeK8sJob)

    backend = K8sBackend(namespace="ns", pvc_name="pvc", pvc_mount_path=pvc_root)
    await backend.call_single_step(
        step, workflow_inputs={}, produced={}, workspace=tmp_path
    )

    run_job = submitted[-1]
    container = run_job["spec"]["template"]["spec"]["containers"][0]
    resources = container["resources"]
    assert resources["requests"]["cpu"] == "1"
    assert resources["limits"]["cpu"] == "2"
    assert resources["requests"]["memory"] == "512Mi"
    assert resources["limits"]["memory"] == "1024Mi"


def test_k8s_merge_effective_values_from_runtime_job_variables(monkeypatch):
    class FakeRunContext:
        class _FlowRun:
            job_variables = {
                "namespace": "runtime-ns",
                "service_account_name": "runtime-sa",
                "image_pull_secrets": ["regcred-a", {"name": "regcred-b"}],
            }

        flow_run = _FlowRun()

    monkeypatch.setattr(
        "prefect_cwl.backends.k8s.get_run_context", lambda: FakeRunContext()
    )

    backend = K8sBackend(
        namespace="default-ns",
        service_account_name="default-sa",
        image_pull_secrets=["local-secret"],
    )

    assert backend._effective_namespace() == "default-ns"
    assert backend._effective_service_account_name() == "default-sa"
    pull_secrets = backend._effective_image_pull_secrets()
    names = sorted([x["name"] for x in pull_secrets])
    assert names == ["local-secret"]


def test_k8s_hierarchy_template_defaults_then_runtime_then_local_overrides(monkeypatch):
    job_template = {
        "variables": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string", "default": "tpl-ns"},
                "service_account_name": {"type": "string", "default": "tpl-sa"},
                "finished_job_ttl": {"type": "integer", "default": 30},
                "image_pull_policy": {"type": "string", "default": "IfNotPresent"},
                "labels": {
                    "type": "object",
                    "default": {"layer": "template"},
                },
            },
        },
        "job_configuration": {
            "namespace": "{{ namespace }}",
            "labels": "{{ labels }}",
            "job_manifest": {
                "spec": {
                    "ttlSecondsAfterFinished": "{{ finished_job_ttl }}",
                    "template": {
                        "spec": {
                            "serviceAccountName": "{{ service_account_name }}",
                            "imagePullSecrets": [{"name": "tpl-secret"}],
                            "volumes": [{"name": "tpl-cache", "emptyDir": {}}],
                            "containers": [
                                {
                                    "env": [{"name": "TPL_ENV", "value": "1"}],
                                    "volumeMounts": [
                                        {
                                            "name": "tpl-cache",
                                            "mountPath": "/tpl-cache",
                                        }
                                    ],
                                    "imagePullPolicy": "{{ image_pull_policy }}",
                                }
                            ],
                        }
                    },
                }
            },
        },
    }

    class FakeRunContext:
        class _FlowRun:
            job_variables = {
                "job_template": job_template,
                "job_variables": {
                    "namespace": "runtime-ns",
                    "finished_job_ttl": 60,
                    "labels": {"layer": "runtime"},
                },
            }

        flow_run = _FlowRun()

    monkeypatch.setattr(
        "prefect_cwl.backends.k8s.get_run_context", lambda: FakeRunContext()
    )

    backend = K8sBackend(
        namespace="user-ns",
        job_variables={"labels": {"layer": "user"}},
    )

    assert backend._effective_namespace() == "user-ns"
    assert backend._effective_service_account_name() == "tpl-sa"
    assert backend._effective_finished_job_ttl() == 60
    assert backend._effective_labels() == {"layer": "user"}

    manifest = backend._base_job_manifest(
        "x",
        {
            "image": "alpine:3.19",
            "command": ["sh", "-lc", "true"],
            "env": [],
            "volumeMounts": [],
        },
    )
    pod_spec = manifest["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    assert manifest["metadata"]["namespace"] == "user-ns"
    assert manifest["metadata"]["labels"] == {"layer": "user"}
    assert manifest["spec"]["ttlSecondsAfterFinished"] == 60
    assert pod_spec["serviceAccountName"] == "tpl-sa"
    assert container["imagePullPolicy"] == "IfNotPresent"
    assert any(v.get("name") == "tpl-cache" for v in pod_spec["volumes"])
    assert any(
        m.get("name") == "tpl-cache" and m.get("mountPath") == "/tpl-cache"
        for m in container["volumeMounts"]
    )


def test_k8s_template_env_defaults_apply_when_no_runtime_or_local_env(monkeypatch):
    job_template = {
        "variables": {
            "type": "object",
            "properties": {
                "env": {"type": "object", "default": {"A": "from-template"}}
            },
        },
        "job_configuration": {},
    }

    class FakeRunContext:
        class _FlowRun:
            job_variables = {
                "job_template": job_template,
                "job_variables": {},
            }

        flow_run = _FlowRun()

    monkeypatch.setattr(
        "prefect_cwl.backends.k8s.get_run_context", lambda: FakeRunContext()
    )
    backend = K8sBackend()
    env = backend._effective_runtime_env()
    env_map = {item["name"]: item["value"] for item in env}
    assert env_map["A"] == "from-template"


def test_k8s_merge_env_precedence_step_overrides_runtime():
    backend = K8sBackend()
    runtime_env = [
        {"name": "A", "value": "runtime-a"},
        {"name": "B", "value": "runtime-b"},
    ]
    step_env = [
        {"name": "B", "value": "step-b"},
        {"name": "C", "value": "step-c"},
    ]

    merged = backend._merge_env(runtime_env, step_env)
    env_map = {e["name"]: e["value"] for e in merged}

    assert env_map["A"] == "runtime-a"
    assert env_map["B"] == "step-b"
    assert env_map["C"] == "step-c"


def test_k8s_merge_named_second_list_wins():
    backend = K8sBackend()
    runtime_items = [
        {"name": "work", "emptyDir": {}},
        {"name": "cache", "emptyDir": {}},
    ]
    required_items = [
        {"name": "work", "persistentVolumeClaim": {"claimName": "pvc-main"}},
    ]

    merged = backend._merge_named(runtime_items, required_items)
    merged_by_name = {i["name"]: i for i in merged}

    assert "cache" in merged_by_name
    assert merged_by_name["work"]["persistentVolumeClaim"]["claimName"] == "pvc-main"
    assert "emptyDir" not in merged_by_name["work"]


def test_k8s_base_job_manifest_without_runtime_context_uses_backend_defaults(
    monkeypatch,
):
    def raise_no_context():
        from prefect.exceptions import MissingContextError

        raise MissingContextError("no run context")

    monkeypatch.setattr("prefect_cwl.backends.k8s.get_run_context", raise_no_context)

    backend = K8sBackend(
        namespace="default-ns",
        pvc_name="pvc-main",
        pvc_mount_path="/data",
        service_account_name="default-sa",
    )
    manifest = backend._base_job_manifest(
        "my-job",
        {
            "image": "alpine:3.19",
            "command": ["sh", "-lc", "true"],
            "env": [{"name": "A", "value": "1"}],
            "volumeMounts": [],
        },
    )

    assert manifest["metadata"]["namespace"] == "default-ns"
    pod_spec = manifest["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "default-sa"
    assert any(v["name"] == "work" for v in pod_spec["volumes"])
    container = pod_spec["containers"][0]
    assert any(
        m.get("name") == "work" and m.get("mountPath") == "/data"
        for m in container["volumeMounts"]
    )


def test_k8s_backend_log_level_coercion():
    backend = K8sBackend(log_level="warning", stream_log_level="20")
    assert backend.log_level == logging.WARNING
    assert backend.stream_log_level == logging.INFO
