import os
import uuid
from pathlib import Path

from prefect import flow
from prefect_cwl import create_executor_with_k8s_backend


def _workspace_root() -> Path:
    return Path(os.getenv("PREFECT_CWL_K8S_PVC_MOUNT_PATH", "/data"))


@flow(name="random-grep-count-scatter")
async def random_grep_count_scatter(
    random_string_number: int,
    grep_string: list[str],
    augment_lines: int,
) -> dict:
    """Run the scatter random-grep-counter CWL on Kubernetes through prefect-cwl."""
    run_id = uuid.uuid4().hex[:10]
    host_work_dir = _workspace_root() / "prefect-cwl-runs" / run_id

    cwl_path = Path(__file__).resolve().parent.parent / "random_grep_count_scatter.cwl"
    workflow_text = cwl_path.read_text(encoding="utf-8")

    executor = create_executor_with_k8s_backend(
        workflow_text=workflow_text,
        host_work_dir=host_work_dir,
        workflow_id="#random_grep_count",
    )

    return await executor(
        random_string_number=random_string_number,
        grep_string=grep_string,
        augment_lines=augment_lines,
    )
