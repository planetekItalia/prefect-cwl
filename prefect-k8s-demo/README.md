# Running Prefect inside a local K8s Cluster

This little guide will get you deploying Prefect (server and worker) on a local K8s cluster.


## Requirements

- some experience with _k8s and kubectl, helm_
- KIND installed and working (KIND is Kubernetes in Docker)

## Create cluster using Kind (Kubernetes in Docker)

    kind create cluster --name prefect-demo --kubeconfig prefect-demo-kubeconfig
    export KUBECONFIG=./prefect-demo-kubeconfig
    
    helm repo add prefect https://prefecthq.github.io/prefect-helm
    helm repo update
    kubectl create namespace prefect

## Install Prefect Server and Worker:

    helm install prefect-server prefect/prefect-server --namespace prefect
    
    helm install prefect-worker prefect/prefect-worker \
      --namespace prefect \
      -f worker-values.yaml

# Port forward Prefect server

    kubectl --namespace prefect port-forward svc/prefect-server 4200:4200

then point on your browser over *http://localhost:4200*

# Create work pool, after installing the *prefect library*

    uv run prefect work-pool create local-k8s -t kubernetes

# Fixing permissions (JOBS from FLOW)
    
    kubectl apply -f rbac.yaml -n prefect

## Create PVC

    kubectl -n prefect apply -f pvc.yml

## Update the workpool trough UI

    namespace: prefect
    serviceAccountName: prefect-flow-runner
    volumes (under spec): 
        - name: workdir
              persistentVolumeClaim:
                claimName: prefect-shared-pvc
    
    volumeMounts (under *container*):
                - name: workdir
                  mountPath: /data

## Build and load docker image

    docker build -t prefect-k8s-demo:0.1 .
    kind load docker-image prefect-k8s-demo:0.1 --name prefect-demo

## Deploy flow

       uv run python deploy.py