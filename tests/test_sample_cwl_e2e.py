import asyncio
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest

from prefect_cwl import create_flow_with_docker_backend, create_flow_with_k8s_backend


REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_ROOT = REPO_ROOT / "sample_cwl"
pytestmark = [pytest.mark.e2e]


def _docker_ready() -> bool:
    if subprocess.run(
        ["which", "docker"], capture_output=True, text=True, check=False
    ).returncode != 0:
        return False
    return (
        subprocess.run(
            ["docker", "info"], capture_output=True, text=True, check=False
        ).returncode
        == 0
    )


def _kubectl_ready(namespace: str) -> bool:
    if subprocess.run(
        ["which", "kubectl"], capture_output=True, text=True, check=False
    ).returncode != 0:
        return False
    return (
        subprocess.run(
            ["kubectl", "get", "ns", namespace],
            capture_output=True,
            text=True,
            check=False,
        ).returncode
        == 0
    )


def _assert_file(path_value: Any) -> None:
    assert isinstance(path_value, Path), f"Expected Path, got {type(path_value)!r}"
    assert path_value.exists(), f"Missing file: {path_value}"
    assert path_value.is_file(), f"Expected file, got: {path_value}"
    assert path_value.stat().st_size > 0, f"Empty file: {path_value}"


def _assert_dir(path_value: Any) -> None:
    assert isinstance(path_value, Path), f"Expected Path, got {type(path_value)!r}"
    assert path_value.exists(), f"Missing directory: {path_value}"
    assert path_value.is_dir(), f"Expected directory, got: {path_value}"


def _assert_relative_file(base_dir: Path, relpath: str) -> None:
    target = base_dir / relpath
    assert target.exists(), f"Missing expected output file: {target}"
    assert target.is_file(), f"Expected file, got: {target}"
    assert target.stat().st_size > 0, f"Empty file: {target}"


def _assert_exists_in_k8s(path_value: Path) -> None:
    namespace = os.getenv("PREFECT_CWL_E2E_K8S_NAMESPACE", "prefect")
    check_pod = os.getenv("PREFECT_CWL_E2E_K8S_CHECK_POD", "pvc-debug")
    cmd = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        check_pod,
        "--",
        "sh",
        "-lc",
        f"test -e {str(path_value)!r}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert (
        result.returncode == 0
    ), f"Expected path not found in k8s PVC: {path_value}\n{result.stderr}"


def _skip_if_docker_unavailable() -> None:
    if not _docker_ready():
        pytest.skip("Docker is not available for sample_cwl E2E tests")


def _skip_if_network_disabled() -> None:
    network_flag = os.getenv("PREFECT_CWL_E2E_NETWORK", "").lower()
    heavy_io_flag = os.getenv("PREFECT_CWL_E2E_HEAVY_IO", "").lower()
    if network_flag not in {"1", "true", "yes"} and heavy_io_flag not in {
        "1",
        "true",
        "yes",
    }:
        pytest.skip(
            "Heavy I/O sample_cwl E2E disabled. Set PREFECT_CWL_E2E_NETWORK=1 or PREFECT_CWL_E2E_HEAVY_IO=1"
        )


def _skip_if_k8s_unavailable() -> None:
    if os.getenv("PREFECT_CWL_E2E_K8S", "").lower() not in {"1", "true", "yes"}:
        pytest.skip("K8s sample_cwl E2E disabled. Set PREFECT_CWL_E2E_K8S=1")
    namespace = os.getenv("PREFECT_CWL_E2E_K8S_NAMESPACE", "prefect")
    if not _kubectl_ready(namespace):
        pytest.skip(f"kubectl is not ready for namespace {namespace!r}")


def _run_docker_flow(
    cwl_path: Path, workflow_id: str, inputs: Dict[str, Any], workspace: Path
) -> Dict[str, Any]:
    runnable_flow = create_flow_with_docker_backend(
        cwl_path.read_text(), workspace, workflow_id=workflow_id
    )
    return asyncio.run(runnable_flow(**inputs))


def _run_k8s_flow(
    cwl_path: Path, workflow_id: str, inputs: Dict[str, Any], workspace: Path
) -> Dict[str, Any]:
    runnable_flow = create_flow_with_k8s_backend(
        cwl_path.read_text(), workspace, workflow_id=workflow_id
    )
    return asyncio.run(runnable_flow(**inputs))


@pytest.mark.heavy_io
@pytest.mark.parametrize(
    "name,cwl_rel,workflow_id,inputs,expected_output,expected_rel_file",
    [
        (
            "random_grep_count",
            "random_grep_count/random_grep_count.cwl",
            "#random_grep_count",
            {"random_string_number": 100, "grep_string": "abc"},
            "output_dir",
            "count.txt",
        ),
        (
            "random_grep_count_with_resources",
            "random_grep_count/random_grep_count_with_resources.cwl",
            "#random_grep_count",
            {"random_string_number": 100, "grep_string": "abc"},
            "output_dir",
            "count.txt",
        ),
        (
            "random_grep_count_scatter",
            "random_grep_count/random_grep_count_scatter.cwl",
            "#random_grep_count",
            {
                "random_string_number": 100,
                "grep_string": ["abc", "cde"],
                "augment_lines": 100,
            },
            "output_dir",
            "count.txt",
        ),
        (
            "multiple_inputs_checker",
            "multiple_input_checker/multiple_inputs_checker.cwl",
            "#multiple_inputs_checker",
            {
                "val_int": 1,
                "val_int_a": [2, 3, 4],
                "val_flt": 3.6,
                "val_flt_a": [0.3, 0.5, 0.7],
                "val_str": "Test sentence",
                "val_str_a": ["String one", "String two", "String three"],
            },
            "output_dir",
            "out.txt",
        ),
        (
            "scatter_gather_grep",
            "random_grep_count/random_grep_count_scatter_gather_grep.cwl",
            "#random_grep_count",
            {"random_string_number": 100, "grep_string": ["==", "cde"]},
            "output_dir",
            "count.txt",
        ),
    ],
)
def test_sample_flows_docker_local_e2e(
    tmp_path: Path,
    name: str,
    cwl_rel: str,
    workflow_id: str,
    inputs: Dict[str, Any],
    expected_output: str,
    expected_rel_file: str,
) -> None:
    _skip_if_docker_unavailable()

    outputs = _run_docker_flow(
        SAMPLE_ROOT / cwl_rel, workflow_id, inputs, workspace=tmp_path / name
    )
    assert expected_output in outputs, f"Missing output {expected_output!r}: {outputs}"
    out_dir = outputs[expected_output]
    _assert_dir(out_dir)
    _assert_relative_file(out_dir, expected_rel_file)


@pytest.mark.heavy_io
@pytest.mark.parametrize(
    "name,cwl_rel,workflow_id,inputs,output_checks",
    [
        (
            "dockerized_pystac_actions",
            "dockerized_pystac_actions/dockerized_pystac_actions.cwl",
            "#cwl2prefect",
            {
                "downloader_catalog_url": "https://planetarycomputer.microsoft.com/api/stac/v1",
                "downloader_collection_name": "io-lulc-annual-v02",
                "downloader_date_start": "2023-10-01T00:00:00Z",
                "downloader_date_end": "2023-11-01T00:00:00Z",
                "downloader_bbox": [16.786594, 41.077410, 16.935425, 41.141692],
            },
            ("output_downloader", "output_cropper"),
        ),
        (
            "dockerized_pystac_actions_parallel_crop",
            "dockerized_pystac_actions/dockerized_pystac_actions_parallel_crop.cwl",
            "#cwl2prefect",
            {
                "downloader_catalog_url": "https://planetarycomputer.microsoft.com/api/stac/v1",
                "downloader_collection_name": "io-lulc-annual-v02",
                "downloader_date_start": "2023-10-01T00:00:00Z",
                "downloader_date_end": "2023-11-01T00:00:00Z",
                "downloader_bbox": [16.786594, 41.077410, 16.935425, 41.141692],
                "downloader_bbox_two": [16.786594, 41.077410, 16.935425, 41.141692],
                "downloader_bbox_three": [16.786594, 41.077410, 16.935425, 41.141692],
            },
            (
                "output_downloader",
                "output_cropper",
                "output_cropper_two",
                "output_cropper_three",
            ),
        ),
        (
            "ndvi",
            "ndvi/ndvi.cwl",
            "#ndvi",
            {"spatial_extent": ["16.786594", "41.077410", "16.935425", "41.141692"]},
            ("execution_result_assets",),
        ),
        (
            "nbr",
            "nbr/nbr.yml",
            "#main",
            {
                "aoi": ["136.659", "-35.96", "136.923", "-35.791"],
                "assets": [
                    "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/53/H/PA/2021/7/S2B_53HPA_20210723_0_L2A/B8A.tif",
                    "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/53/H/PA/2021/7/S2B_53HPA_20210723_0_L2A/B12.tif",
                    "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/53/H/PA/2021/7/S2B_53HPA_20210723_0_L2A/SCL.tif",
                ],
                "epsg": "EPSG:4326",
            },
            ("nbr",),
        ),
    ],
)
def test_sample_flows_docker_network_e2e(
    tmp_path: Path,
    name: str,
    cwl_rel: str,
    workflow_id: str,
    inputs: Dict[str, Any],
    output_checks: tuple[str, ...],
) -> None:
    _skip_if_docker_unavailable()
    _skip_if_network_disabled()

    outputs = _run_docker_flow(
        SAMPLE_ROOT / cwl_rel, workflow_id, inputs, workspace=tmp_path / name
    )

    for output_name in output_checks:
        assert output_name in outputs, f"Missing output {output_name!r}: {outputs}"
        artifact = outputs[output_name]
        if output_name == "nbr":
            _assert_file(artifact)
        else:
            _assert_dir(artifact)


@pytest.mark.parametrize(
    "name,cwl_rel,workflow_id,inputs,output_checks",
    [
        (
            "random_grep_count_scatter_gather_grep_k8s",
            "random_grep_count/random_grep_count_scatter_gather_grep.cwl",
            "#random_grep_count",
            {"random_string_number": 100, "grep_string": ["==", "cde"]},
            ("output_dir",),
        ),
        (
            "dockerized_pystac_actions_parallel_crop_k8s",
            "dockerized_pystac_actions/dockerized_pystac_actions_parallel_crop.cwl",
            "#cwl2prefect",
            {
                "downloader_catalog_url": "https://planetarycomputer.microsoft.com/api/stac/v1",
                "downloader_collection_name": "io-lulc-annual-v02",
                "downloader_date_start": "2023-10-01T00:00:00Z",
                "downloader_date_end": "2023-11-01T00:00:00Z",
                "downloader_bbox": [16.786594, 41.077410, 16.935425, 41.141692],
                "downloader_bbox_two": [16.786594, 41.077410, 16.935425, 41.141692],
                "downloader_bbox_three": [16.786594, 41.077410, 16.935425, 41.141692],
            },
            (
                "output_downloader",
                "output_cropper",
                "output_cropper_two",
                "output_cropper_three",
            ),
        ),
        (
            "ndvi_k8s",
            "ndvi/ndvi.cwl",
            "#ndvi",
            {"spatial_extent": ["16.786594", "41.077410", "16.935425", "41.141692"]},
            ("execution_result_assets",),
        ),
        (
            "nbr_k8s",
            "nbr/nbr.yml",
            "#main",
            {
                "aoi": ["136.659", "-35.96", "136.923", "-35.791"],
                "assets": [
                    "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/53/H/PA/2021/7/S2B_53HPA_20210723_0_L2A/B8A.tif",
                    "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/53/H/PA/2021/7/S2B_53HPA_20210723_0_L2A/B12.tif",
                    "/vsicurl/https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/53/H/PA/2021/7/S2B_53HPA_20210723_0_L2A/SCL.tif",
                ],
                "epsg": "EPSG:4326",
            },
            ("nbr",),
        ),
    ],
)
def test_sample_flows_k8s_e2e(
    name: str,
    cwl_rel: str,
    workflow_id: str,
    inputs: Dict[str, Any],
    output_checks: tuple[str, ...],
) -> None:
    _skip_if_k8s_unavailable()
    network_flag = os.getenv("PREFECT_CWL_E2E_NETWORK", "").lower()
    heavy_io_flag = os.getenv("PREFECT_CWL_E2E_HEAVY_IO", "").lower()
    if any(token in name for token in ("pystac", "ndvi", "nbr")) and network_flag not in {
        "1",
        "true",
        "yes",
    } and heavy_io_flag not in {"1", "true", "yes"}:
        pytest.skip(
            "Heavy I/O K8s sample_cwl E2E disabled. Set PREFECT_CWL_E2E_NETWORK=1 or PREFECT_CWL_E2E_HEAVY_IO=1"
        )

    base_workspace = Path(
        os.getenv("PREFECT_CWL_E2E_K8S_WORKDIR", "/data/prefect-cwl-e2e")
    )
    workspace = base_workspace / f"{name}-{uuid.uuid4().hex[:8]}"

    outputs = _run_k8s_flow(SAMPLE_ROOT / cwl_rel, workflow_id, inputs, workspace)
    for output_name in output_checks:
        assert output_name in outputs, f"Missing output {output_name!r}: {outputs}"
        artifact = outputs[output_name]
        assert isinstance(
            artifact, Path
        ), f"K8s sample output must be a Path, got {type(artifact)!r}"
        _assert_exists_in_k8s(artifact)
