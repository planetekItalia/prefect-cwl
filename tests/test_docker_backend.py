from pathlib import Path
from types import SimpleNamespace

import pytest

from prefect_cwl.planner.templates import (
    StepPlan,
    ListingMaterialization,
    StepResources,
)
from prefect_cwl.backends.docker import DockerBackend


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
async def test_docker_backend_success(tmp_path, monkeypatch):
    # Arrange a minimal StepPlan
    outdir = tmp_path / "out"
    jobdir = tmp_path / "job"
    listing_path = jobdir / "hello.txt"

    plan = StepPlan(
        step_name="step1",
        tool_id="tool-1",
        image="alpine:3.19",
        argv=["sh", "-lc", "echo hi > /out/hi.txt"],
        outdir_container=Path("/out"),
        volumes={
            str(outdir): "/out:rw",
            str(jobdir): "/cwl_job:rw",
        },
        listings=[
            ListingMaterialization(host_path=listing_path, content="hello world")
        ],
        out_artifacts={"o": outdir / "hi.txt"},
        envs={"A": "1"},
    )

    step = DummyStepTemplate("step1", plan)

    # Mock prefect_docker interactions
    calls = {}

    async def fake_pull(repository):
        calls.setdefault("pull", []).append(repository)

    async def fake_to_thread(fn, *args, **kwargs):  # type: ignore[no-untyped-def]
        return fn(*args, **kwargs)

    created_container = SimpleNamespace(id="cid-123")

    async def fake_create(**kwargs):
        calls.setdefault("create", []).append(kwargs)
        return created_container

    class FakeContainer:
        def __init__(self):
            self.id = created_container.id
            self._wait_status = {"StatusCode": 0}
            self._logs = [b"running...\n", b"done\n"]

        def logs(self, stream=False, follow=False, tail=None):  # type: ignore[no-untyped-def]
            if stream:
                for chunk in self._logs:
                    yield chunk
                return
            return b"".join(self._logs)

        def wait(self):
            return self._wait_status

    started_container = FakeContainer()

    async def fake_start(container_id):
        calls.setdefault("start", []).append(container_id)
        return started_container

    monkeypatch.setattr("prefect_cwl.backends.docker.pull_docker_image", fake_pull)
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.create_docker_container", fake_create
    )
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.start_docker_container", fake_start
    )
    monkeypatch.setattr("prefect_cwl.backends.docker.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr(
        DockerBackend, "_stream_logs", staticmethod(lambda *args, **kwargs: None)
    )

    backend = DockerBackend()

    produced = {}
    # Act
    await backend.call_single_step(
        step, workflow_inputs={}, produced=produced, workspace=tmp_path
    )

    # Assert
    assert calls["pull"] == ["alpine:3.19"]
    assert len(calls["create"]) == 1
    create_kwargs = calls["create"][0]
    assert create_kwargs["image"] == "alpine:3.19"
    assert create_kwargs["command"] == ["sh", "-lc", "echo hi > /out/hi.txt"]
    assert any(
        ":/out:rw" in v for v in create_kwargs["volumes"]
    )  # formatted for prefect-docker
    assert ("step1", "o") in produced
    assert produced[("step1", "o")].name == "hi.txt"
    # Listing file is materialized on host
    assert listing_path.exists() and listing_path.read_text() == "hello world"


@pytest.mark.asyncio
async def test_docker_backend_failure_raises_with_tail_logs(tmp_path, monkeypatch):
    outdir = tmp_path / "out"
    jobdir = tmp_path / "job"

    plan = StepPlan(
        step_name="s",
        tool_id="t",
        image="alpine:3.19",
        argv=["sh", "-lc", "exit 2"],
        outdir_container=Path("/out"),
        volumes={str(outdir): "/out:rw", str(jobdir): "/cwl_job:rw"},
        listings=[],
        out_artifacts={"o": outdir / "x.txt"},
        envs={},
    )
    step = DummyStepTemplate("s", plan)

    async def fake_pull(repository):
        return None

    async def fake_to_thread(fn, *args, **kwargs):  # type: ignore[no-untyped-def]
        return fn(*args, **kwargs)

    async def fake_create(**kwargs):
        return SimpleNamespace(id="cid")

    class FakeContainer:
        def __init__(self):
            self.id = "cid"

        def logs(self, stream=False, follow=False, tail=None):  # type: ignore[no-untyped-def]
            if tail is not None:
                return b"last line\n"
            return iter([b"x\n"]) if stream else b"x\n"

        def wait(self):
            return {"StatusCode": 2}

    async def fake_start(container_id):
        return FakeContainer()

    monkeypatch.setattr("prefect_cwl.backends.docker.pull_docker_image", fake_pull)
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.create_docker_container", fake_create
    )
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.start_docker_container", fake_start
    )
    monkeypatch.setattr("prefect_cwl.backends.docker.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr(
        DockerBackend, "_stream_logs", staticmethod(lambda *args, **kwargs: None)
    )
    monkeypatch.setattr(
        DockerBackend, "_tail_logs", staticmethod(lambda *args, **kwargs: "last line\n")
    )

    backend = DockerBackend()

    with pytest.raises(RuntimeError) as ei:
        await backend.call_single_step(
            step, workflow_inputs={}, produced={}, workspace=tmp_path
        )

    assert "failed with exit code 2" in str(ei.value)
    assert "last line" in str(ei.value)


@pytest.mark.asyncio
async def test_docker_backend_passes_suffix_and_overrides_to_materialize(
    tmp_path, monkeypatch
):
    outdir = tmp_path / "out"
    jobdir = tmp_path / "job"
    override_dir = tmp_path / "upstream" / "out"

    plan = StepPlan(
        step_name="augmenter",
        tool_id="t",
        image="alpine:3.19",
        argv=["sh", "-lc", "true"],
        outdir_container=Path("/out"),
        volumes={str(outdir): "/out:rw", str(jobdir): "/cwl_job:rw"},
        listings=[],
        out_artifacts={"o": outdir / "x.txt"},
        envs={},
    )
    step = CapturingStepTemplate("augmenter", plan)

    async def fake_pull(repository):
        return None

    async def fake_to_thread(fn, *args, **kwargs):  # type: ignore[no-untyped-def]
        return fn(*args, **kwargs)

    async def fake_create(**kwargs):
        return SimpleNamespace(id="cid")

    class FakeContainer:
        id = "cid"

        def logs(self, stream=False, follow=False, tail=None):  # type: ignore[no-untyped-def]
            return iter([]) if stream else b""

        def wait(self):
            return {"StatusCode": 0}

    async def fake_start(container_id):
        return FakeContainer()

    monkeypatch.setattr("prefect_cwl.backends.docker.pull_docker_image", fake_pull)
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.create_docker_container", fake_create
    )
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.start_docker_container", fake_start
    )
    monkeypatch.setattr("prefect_cwl.backends.docker.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr(
        DockerBackend, "_stream_logs", staticmethod(lambda *args, **kwargs: None)
    )

    backend = DockerBackend()
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
async def test_docker_backend_maps_resource_limits_to_create_kwargs(
    tmp_path, monkeypatch
):
    outdir = tmp_path / "out"
    jobdir = tmp_path / "job"
    plan = StepPlan(
        step_name="step1",
        tool_id="tool-1",
        image="alpine:3.19",
        argv=["sh", "-lc", "echo hi"],
        outdir_container=Path("/out"),
        volumes={str(outdir): "/out:rw", str(jobdir): "/cwl_job:rw"},
        listings=[],
        out_artifacts={"o": outdir / "hi.txt"},
        envs={},
        resources=StepResources(cpu_limit=1.5, memory_limit_mb=256),
    )
    step = DummyStepTemplate("step1", plan)

    calls = {}

    async def fake_pull(repository):
        return None

    async def fake_to_thread(fn, *args, **kwargs):  # type: ignore[no-untyped-def]
        return fn(*args, **kwargs)

    async def fake_create(**kwargs):
        calls["create"] = kwargs
        return SimpleNamespace(id="cid")

    class FakeContainer:
        id = "cid"

        def logs(self, stream=False, follow=False, tail=None):  # type: ignore[no-untyped-def]
            return iter([]) if stream else b""

        def wait(self):
            return {"StatusCode": 0}

    async def fake_start(container_id):
        return FakeContainer()

    monkeypatch.setattr("prefect_cwl.backends.docker.pull_docker_image", fake_pull)
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.create_docker_container", fake_create
    )
    monkeypatch.setattr(
        "prefect_cwl.backends.docker.start_docker_container", fake_start
    )
    monkeypatch.setattr("prefect_cwl.backends.docker.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr(
        DockerBackend, "_stream_logs", staticmethod(lambda *args, **kwargs: None)
    )

    await DockerBackend().call_single_step(
        step, workflow_inputs={}, produced={}, workspace=tmp_path
    )

    assert calls["create"]["nano_cpus"] == 1_500_000_000
    assert calls["create"]["mem_limit"] == "256m"
