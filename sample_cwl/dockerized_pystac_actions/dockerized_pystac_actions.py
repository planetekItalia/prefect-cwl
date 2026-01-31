import asyncio
import tempfile
from pathlib import Path
from prefect_cwl import create_flow_with_docker_backend

inputs = dict(
        downloader_catalog_url='https://planetarycomputer.microsoft.com/api/stac/v1',
        downloader_collection_name='io-lulc-annual-v02',
        downloader_date_start='2023-10-01T00:00:00Z',
        downloader_date_end='2023-11-01T00:00:00Z',
        downloader_bbox=[16.786594,41.077410, 16.935425, 41.141692]
    )

with tempfile.TemporaryDirectory(delete=False) as tmpdir:
    with open(Path(__file__).parent / "dockerized_pystac_actions.cwl") as inp:
        runnable_flow = create_flow_with_docker_backend(
            inp.read(), Path(tmpdir), workflow_id="#cwl2prefect"
        )

    asyncio.run(runnable_flow(**inputs))