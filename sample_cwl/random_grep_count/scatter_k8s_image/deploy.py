import os

from flow_scatter import random_grep_count_scatter


if __name__ == "__main__":
    deployment_name = os.getenv("PREFECT_DEPLOYMENT_NAME", "random-grep-count-scatter")
    work_pool_name = os.getenv("PREFECT_WORK_POOL", "local-k8s")
    image = os.getenv("PREFECT_IMAGE")

    if not image:
        raise RuntimeError(
            "PREFECT_IMAGE is required, e.g. ghcr.io/acme/random-grep-scatter:0.1.0"
        )

    random_grep_count_scatter.deploy(
        name=deployment_name,
        work_pool_name=work_pool_name,
        build=False,
        image=image,
        push=False,
        parameters={
            "random_string_number": 100,
            "grep_string": ["abc", "cde"],
            "augment_lines": 100,
        },
    )
