FROM prefecthq/prefect:3.6.13-python3.11

ARG PREFECT_CWL_GIT_URL="https://github.com/planetekItalia/prefect-cwl.git@develop"
ARG PREFECT_VERSION="3.6.13"
ARG PREFECT_KUBERNETES_VERSION="0.7.3"

WORKDIR /app

RUN pip install --no-cache-dir "prefect-cwl[k8s] @ git+${PREFECT_CWL_GIT_URL}" \
    && pip install --no-cache-dir "prefect==${PREFECT_VERSION}" "prefect-kubernetes==${PREFECT_KUBERNETES_VERSION}"

COPY sample_cwl/random_grep_count /app/sample_cwl/random_grep_count

ENV PYTHONPATH=/app
ENV PREFECT_CWL_K8S_PVC_MOUNT_PATH=/data

WORKDIR /app/sample_cwl/random_grep_count/scatter_k8s_image

CMD ["python", "deploy.py"]
