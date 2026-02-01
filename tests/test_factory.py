import builtins
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import prefect_cwl.factory as factory


def test_prepare_plan_calls_planner(monkeypatch, tmp_path: Path):
    planner_instance = Mock()
    planner_instance.prepare.return_value = "PLAN"

    planner_cls = Mock(return_value=planner_instance)
    monkeypatch.setattr(factory, "Planner", planner_cls)

    out = factory._prepare_plan("cwl-text", tmp_path, "#main")

    assert out == "PLAN"
    planner_cls.assert_called_once_with("cwl-text", workspace_root=tmp_path)
    planner_instance.prepare.assert_called_once_with(workflow_ref="#main")


@pytest.mark.asyncio
async def test_create_executor_with_backend_calls_builder(monkeypatch, tmp_path: Path):
    # Make _prepare_plan return a fake plan
    monkeypatch.setattr(factory, "_prepare_plan", Mock(return_value="PLAN"))

    fake_executor = AsyncMock(return_value={"ok": True})

    builder_instance = Mock()
    builder_instance.build_executor.return_value = fake_executor
    builder_cls = Mock(return_value=builder_instance)
    monkeypatch.setattr(factory, "PrefectFlowBuilder", builder_cls)

    backend = Mock()

    executor = factory._create_executor_with_backend(
        workflow_text="cwl",
        host_work_dir=tmp_path,
        workflow_id="#wf",
        backend=backend,
    )

    # Returned object is the executor
    assert executor is fake_executor

    # Ensure builder called with the plan and backend
    builder_instance.build_executor.assert_called_once_with("PLAN", backend)

    # Ensure executor is awaitable when called
    res = await executor(a=1)
    assert res == {"ok": True}


@pytest.mark.asyncio
async def test_wrap_executor_as_flow_calls_executor_and_copies_signature():
    # Create an executor callable that is async
    executor = AsyncMock(return_value={"out": 123})

    # Add a signature to executor (what your builder does)
    executor.__signature__ = inspect.Signature(
        [
            inspect.Parameter(
                "x",
                inspect.Parameter.KEYWORD_ONLY,
                annotation=int,
            )
        ]
    )

    f = factory._wrap_executor_as_flow("wf-id", executor)

    # Avoid running Prefect engine; call the underlying function directly.
    result = await f.fn(x=5)  # type: ignore[attr-defined]
    assert result == {"out": 123}

    executor.assert_awaited_once_with(x=5)

    # Signature copied to flow.fn
    assert getattr(f.fn, "__signature__", None) == executor.__signature__  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_create_flow_with_docker_backend_wraps_executor(
    monkeypatch, tmp_path: Path
):
    # Fake DockerBackend import
    class FakeDockerBackend:
        pass

    # Provide a fake module prefect_cwl.backends.docker with DockerBackend
    fake_docker_module = SimpleNamespace(DockerBackend=FakeDockerBackend)
    monkeypatch.setitem(
        __import__("sys").modules, "prefect_cwl.backends.docker", fake_docker_module
    )

    # Make executor factory return an executor we control
    executor = AsyncMock(return_value={"done": True})
    monkeypatch.setattr(
        factory, "create_executor_with_docker_backend", Mock(return_value=executor)
    )

    f = factory.create_flow_with_docker_backend("cwl", tmp_path, "#wf")

    # Again, avoid Prefect engine: call f.fn and await
    out = await f.fn(random_string_number=1)  # type: ignore[attr-defined]
    assert out == {"done": True}


def test_create_flow_with_docker_backend_missing_extra_raises(
    monkeypatch, tmp_path: Path
):
    """
    Simulate ImportError from: from prefect_cwl.backends.docker import DockerBackend
    """
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "prefect_cwl.backends.docker":
            raise ImportError("no docker extra")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError) as exc:
        factory.create_flow_with_docker_backend("cwl", tmp_path, "#wf")

    msg = str(exc.value)
    assert "Docker backend is not installed" in msg
    assert "pip install prefect-cwl[docker]" in msg


@pytest.mark.asyncio
async def test_create_flow_with_k8s_backend_wraps_executor(monkeypatch, tmp_path: Path):
    # Fake K8sBackend import
    class FakeK8sBackend:
        pass

    fake_k8s_module = SimpleNamespace(K8sBackend=FakeK8sBackend)
    monkeypatch.setitem(
        __import__("sys").modules, "prefect_cwl.backends.k8s", fake_k8s_module
    )

    executor = AsyncMock(return_value={"done": "k8s"})
    monkeypatch.setattr(
        factory, "create_executor_with_k8s_backend", Mock(return_value=executor)
    )

    f = factory.create_flow_with_k8s_backend("cwl", tmp_path, "#wf")
    out = await f.fn(grep_string="abc")  # type: ignore[attr-defined]
    assert out == {"done": "k8s"}


def test_create_executor_with_k8s_backend_missing_extra_raises(
    monkeypatch, tmp_path: Path
):
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "prefect_cwl.backends.k8s":
            raise ImportError("no k8s extra")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError) as exc:
        factory.create_executor_with_k8s_backend("cwl", tmp_path, "#wf")

    msg = str(exc.value)
    assert "Kubernetes backend is not installed" in msg
    assert "pip install prefect-cwl[k8s]" in msg
