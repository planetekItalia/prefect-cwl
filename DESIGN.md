# prefect-cwl Design Document

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Component Details](#component-details)
4. [Data Flow](#data-flow)
5. [Technical Decisions](#technical-decisions)
6. [Limitations](#limitations)
7. [Future Roadmap](#future-roadmap)

---

## Overview

### Purpose

`prefect-cwl` bridges the Common Workflow Language (CWL) specification with Prefect's orchestration engine. The library focuses on **pragmatic subset of CWL** rather than full specification compliance, targeting real-world bioinformatics and data processing workflows.

### Design Goals

1. **Separation of Concerns**: Clear separation between workflow structure (templates) and runtime execution (materialization)
2. **Prefect-Native**: Leverage Prefect's UI, scheduling, and monitoring capabilities
3. **Backend Flexibility**: Support both Docker and Kubernetes execution environments
4. **Parallel Execution**: Automatic detection and execution of independent workflow steps
5. **Testability**: Pure functions and dependency injection for easy testing

### Non-Goals

- Full CWL specification compliance
- Runtime CWL conformance testing

---

## Architecture

### Two-Phase Design

The library implements a strict separation between planning and execution:

#### Phase 1: Planning (Build/Deploy Time)

```python
Planner.prepare(workflow_ref) → WorkflowTemplate
```

**Responsibilities:**
- Parse and validate CWL YAML using Pydantic models
- Build dependency graph from workflow steps
- Compute topologically sorted execution waves
- Create reusable workflow template (no runtime values)

**Output:** `WorkflowTemplate` - an immutable, reusable structure

**Key Insight:** This phase happens **once** at deployment time. The template can be deployed and executed with different inputs multiple times.

#### Phase 2: Execution (Runtime)

```python
Backend.call_single_step(template, inputs, produced, workspace) → None
```

**Responsibilities:**
- Receive runtime workflow input values from Prefect
- Materialize step plan (resolve inputs, render commands, create listings)
- Execute container with proper mounts and environment
- Track outputs for downstream steps

**Output:** Side effects (container execution, output files created)

**Key Insight:** This phase happens **every time** the flow runs. Each execution materializes fresh plans with different input values.

### Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CWL Document (YAML)                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Pydantic Models Layer                      │
│  (CWLDocument, WorkflowNode, CommandLineToolNode, etc.)     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                        Planner                               │
│  • Dependency graph construction                             │
│  • Topological sort → waves                                  │
│  • Template creation (no values)                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   WorkflowTemplate                           │
│  • workflow_id, workflow_inputs (schemas)                    │
│  • step_templates: Dict[str, StepTemplate]                   │
│  • waves: List[List[str]]                                    │
│  • workflow_outputs (schemas)                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  PrefectFlowBuilder                          │
│  • Creates Prefect flow from template                        │
│  • Generates input signature for UI                          │
│  • Orchestrates wave-based execution                         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Prefect Flow                              │
│  Runtime Execution:                                          │
│    For each wave:                                            │
│      Submit tasks in parallel → barrier                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Backend (Docker/K8s)                        │
│  1. materialize_step(template, inputs, produced)             │
│     → StepPlan with actual values                            │
│  2. Execute container                                        │
│  3. Update produced dict                                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Planner

**File:** `planner.py`

**Responsibility:** Parse CWL and create workflow templates.

**Key Functions:**

```python
def prepare(workflow_ref: str) -> WorkflowTemplate
```
- Parses CWL document
- Validates workflow structure
- Builds dependency graph
- Computes execution waves
- Creates step templates

**Pure Helper Functions:**
- `build_dependency_graph()` - Extracts step dependencies
- `topo_waves()` - Topological sort with parallelism
- `index_graph()` - Separates workflows from tools
- `select_workflow()` - Chooses workflow to execute

**Design Rationale:** All helpers are pure functions for testability. The `Planner` class is thin orchestrator around these helpers.

### 2. WorkflowTemplate & StepTemplate

**File:** `planner.py`

**Data Models:**

```python
@dataclass
class StepTemplate:
    step_name: str
    tool_id: str
    tool: CommandLineToolNode        # Reference to parsed tool
    wf_step: WorkflowStepNode        # Reference to workflow step
    image: str                       # Docker image
    outdir_container: PurePosixPath  # Container output path
    envs: Dict[str, str]             # Environment variables

@dataclass
class WorkflowTemplate:
    workflow_id: str
    workflow_inputs: Dict[str, InputParameter]  # Schemas only
    workspace: Path
    step_templates: Dict[str, StepTemplate]
    workflow_outputs: Dict[str, Any]            # Output references
    waves: List[List[str]]                      # Execution order
```

**Design Rationale:** Templates contain **structure** but no **values**. They're immutable and reusable across multiple executions.

### 3. Runtime Materialization

**File:** `planner.py`

**Key Function:**

```python
def materialize_step(
    template: StepTemplate,
    workflow_inputs: Dict[str, Any],
    produced: Dict[Tuple[str, str], Path],
    workspace: Path,
    render_io: RenderIOFn,
) -> StepPlan
```

**Process:**
1. Compute host directories (outdir, jobdir)
2. Resolve step inputs (from workflow inputs or upstream outputs)
3. Build volume mounts (directories containing inputs/outputs)
4. Render command line and listings via `render_io` callback
5. Validate listings are under `JOBROOT`
6. Compute output artifact paths
7. Return fully materialized `StepPlan`

**Design Rationale:** This function is called **at runtime** by backends, allowing the same template to be materialized with different input values.

### 4. PrefectFlowBuilder

**File:** `prefect_cwl/flow_builder.py`

**Responsibility:** Convert `WorkflowTemplate` into Prefect flow.

**Key Method:**

```python
def build(template: WorkflowTemplate, backend: Backend) -> Flow
```

**Process:**
1. Extract workflow input types and create signature
2. Define async `run_step` task (calls backend)
3. Define async `process_flow` function:
   - Validate runtime inputs
   - Track `produced` dict (shared mutable state)
   - For each wave:
     - Submit tasks in parallel
     - If a step declares CWL `scatter`, submit one task run per array item
     - Barrier: wait for all to complete
   - Return workflow outputs
4. Set function signature for Prefect UI
5. Wrap in `@flow` decorator

**Shared State Management:**

The `produced: Dict[Tuple[str, str], Path]` dictionary is **shared mutable state** across all tasks:

```python
produced[(step_name, output_port)] = host_path
```

**Safety:** This is safe because:
- Waves enforce ordering (upstream writes before downstream reads)
- Within a wave, steps don't depend on each other (no conflicts)
- Python's GIL makes dict mutations atomic for simple operations

### 5. Backend Interface

**File:** `backend.py`

**Abstract Interface:**

```python
class Backend(ABC):
    @abstractmethod
    async def call_single_step(
        self,
        step_template: StepTemplate,
        workflow_inputs: Dict[str, Any],
        produced: Dict[Tuple[str, str], Path],
        workspace: Path,
    ) -> None:
        pass
```

**Contract:**
- Receive template and runtime values
- Call `materialize_step()` to get `StepPlan`
- Execute container
- Mutate `produced` dict with outputs

### 6. Docker Backend

**File:** `backend.py`

**Implementation Strategy:**

```python
async def call_single_step(...):
    # 1. Materialize plan
    step_plan = materialize_step(...)

    # 2. Prepare filesystem
    _write_listings(step_plan.listings)
    _ensure_mount_sources(step_plan.volumes)

    # 3. Pull image
    await pull_docker_image(step_plan.image)

    # 4. Create and start container
    container = await create_docker_container(
        command=step_plan.argv,  # Pass as list!
        volumes=_volumes_to_prefect_docker(step_plan.volumes),
        environment=step_plan.envs,
        user=f"{uid}:{gid}",  # Host user mapping
    )

    # 5. Stream logs concurrently with execution
    log_task = asyncio.create_task(...)
    result = await container.wait()

    # 6. Track outputs
    produced.update(...)
```

**Volume Format:** `host:container:mode` (e.g., `/tmp/out:/out:rw`)

**User Mapping:** Automatically maps host UID/GID to avoid permission issues (Unix only)

**Challenges:**
- Race conditions when multiple steps create same directories (mitigated by `exist_ok=True`)
- No cleanup of workspace (user responsibility)
- Log streaming errors need graceful handling

### 7. Kubernetes Backend

**File:** `backend.py`

**Implementation Strategy:**

Three-phase job execution per step:

```python
async def call_single_step(...):
    # 1. Materialize plan
    step_plan = materialize_step(...)
    step = _map_stepplan_to_pvc(step_plan)  # Ensure PVC paths

    # 2. Mkdir job - create directories in PVC
    if dirs:
        await run_namespaced_job(_mkdir_job(...))

    # 3. Listings job - write InitialWorkDir files
    if step.listings:
        await run_namespaced_job(_listings_job(...))

    # 4. Main job - execute tool
    await run_namespaced_job(_step_job(...))

    # 5. Track outputs
    produced.update(...)
```

**Volume Strategy:** Uses Kubernetes `subPath` to mount subdirectories of shared PVC

**PVC Structure:**
```
/data/
└── <workspace>/
    └── steps/
        └── <step-name>/
            ├── out/  (mounted as dockerOutputDirectory)
            └── job/  (mounted as /cwl_job)
```

**Job Naming:** RFC1123-compliant names with UUID suffix to avoid collisions

**Challenges:**
- Job accumulation until TTL expires (set to 1 hour)
- `subPath` has known limitations (no symlinks, permission issues)
- Sequential job execution adds overhead (could be optimized by merging mkdir + listings)

**Work Pool Template Merge (K8s Only):**
- At runtime, spawned CWL step jobs can merge selected Prefect work-pool/deployment `job_variables`
  when available in `flow_run.job_variables`.
- Supported merged fields: namespace, service account, environment variables, volumes, volume mounts, image pull secrets.
- Precedence:
  - `PREFECT_CWL_K8S_*` backend settings are defaults.
  - Runtime `job_variables` override defaults for supported merged fields.
  - Required `prefect-cwl` PVC binding stays enforced (`pvc_name` + root `pvc_mount_path` mount).
- Safety constraints:
  - The required `prefect-cwl` PVC `work` volume and root mount are always enforced.
  - Step-level env values override runtime/template env on key collision.
  - If no run context/job variables exist (e.g. local runs), backend defaults are used.

### 8. Step Resource Requirements

**Scope:** Step-level CWL `ResourceRequirement` subset.

**Supported CWL fields (per CommandLineTool):**
- `coresMin`
- `coresMax`
- `ramMin`
- `ramMax`

**Normalization:**
- Planner parses `ResourceRequirement` and normalizes it into per-step resource metadata.
- This metadata is carried in `StepTemplate` and `StepPlan`.

**Executor Mapping:**
- **Kubernetes backend**
  - Maps to container `resources.requests` and `resources.limits`
  - CPU: `coresMin` -> request, `coresMax` -> limit
  - Memory: `ramMin`/`ramMax` mapped as `Mi`
- **Docker backend**
  - Applies hard limits at container create time
  - CPU: `coresMax` preferred (fallback to `coresMin`) -> `nano_cpus`
  - Memory: `ramMax` preferred (fallback to `ramMin`) -> `mem_limit`

**Notes:**
- This is intentionally a partial CWL implementation; dynamic/expression-based resource evaluation is not covered.
- Tool-level values are interpreted per-step and applied independently for each run (including scattered runs).

---

## Data Flow

### Build-Time (Once per Workflow)

```
CWL YAML
  ↓ (yaml.safe_load)
Raw Dict
  ↓ (CWLDocument.model_validate)
Pydantic Models
  ↓ (Planner.prepare)
WorkflowTemplate
  ↓ (PrefectFlowBuilder.build)
Prefect Flow (deployed)
```

### Runtime (Every Execution)

```
User provides inputs via Prefect UI/API
  ↓
Prefect Flow starts
  ↓
For each wave:
  ↓
  For each step in wave (parallel):
    ↓
    Backend.call_single_step receives:
      - StepTemplate (structure)
      - workflow_inputs (actual values)
      - produced (upstream outputs)
    ↓
    materialize_step():
      - Resolve inputs from workflow_inputs or produced
      - Build volumes (mount upstream outputs)
      - Render command line with actual values
      - Create listings with actual content
      → StepPlan
    ↓
    Execute container:
      - Docker: docker run with volumes
      - K8s: kubectl create job with PVC mounts
    ↓
    Container writes outputs to host filesystem
    ↓
    Update produced dict:
      produced[(step_name, port)] = output_path
  ↓
  Barrier: wait for all steps in wave
↓
Next wave (can now access previous outputs via produced)
↓
Return workflow outputs
```

---

## Technical Decisions

### 1. Why Two-Phase Design?

**Decision:** Separate template creation from materialization.

**Rationale:**
- **Reusability:** One template, many executions with different inputs
- **Prefect UI:** Signature needs to be known at deploy-time for form generation
- **Validation:** Structure validation at parse-time, value validation at runtime
- **Performance:** Expensive parsing/graph-building happens once

**Alternative Considered:** Single-phase where inputs are required at parse-time.
- **Rejected:** Would require re-parsing CWL for every execution.

### 2. Why Shared Mutable State (`produced` dict)?

**Decision:** Pass mutable dict across all tasks in a flow.

**Rationale:**
- **Simplicity:** No complex state management or external storage
- **Performance:** In-memory lookups are fast
- **Safety:** Waves enforce ordering, GIL provides atomicity

**Alternative Considered:** Prefect artifacts or task return values.
- **Rejected:** Would require complex chaining of task dependencies, losing parallelism.

### 3. Why Waves Instead of Prefect Dependencies?

**Decision:** Manually compute waves and use barriers.

**Rationale:**
- **Explicit Parallelism:** Clear which steps run in parallel
- **Simpler Logic:** No complex Prefect dependency graph construction
- **Control:** Full control over execution order

**Alternative Considered:** Use Prefect's `wait_for` or task dependencies.
- **Accepted Trade-off:** More Prefect-native but less explicit about parallelism.

### 4. Why PVC Required for K8s?

**Decision:** Require pre-configured shared PVC instead of dynamic volumes.

**Rationale:**
- **Simplicity:** No need to manage PVC lifecycle
- **Performance:** PVC is faster than alternatives (emptyDir, hostPath)
- **Data Sharing:** Essential for inter-step file passing

**Alternative Considered:** Dynamic PVC provisioning per workflow.
- **Rejected:** Adds complexity, cleanup challenges, and cost.

---

## Limitations

### By Design (Intentional Scope Limits)

1. **CWL Coverage**
   - **Limit:** Only basic CWL features (see README)
   - **Rationale:** Focus on common use cases, avoid feature bloat
   - **Impact:** Cannot run arbitrary CWL workflows
   - **Mitigation:** Document supported features clearly

2. **Glob Patterns**
   - **Limit:** No wildcards, only exact paths
   - **Rationale:** Simplifies output tracking, no ambiguity
   - **Impact:** Cannot use `*.txt` patterns
   - **Mitigation:** Require tools to write to specific filenames

3. **Workflow-Level File Inputs**
   - **Limit:** Only primitive types as workflow inputs
   - **Rationale:** Prefect UI form generation is easier for primitives
   - **Impact:** Cannot pass files directly to workflow
   - **Mitigation:** Use InitialWorkDirRequirement or mount external volumes

4. **Partial Scatter Support**
   - **Supported:** Single-input `scatter` only (one inport mapped over a list)
   - **Supported:** Scatter source can come from workflow input arrays or upstream produced list outputs
   - **Supported:** Gather-like behavior for scattered outputs (ordered list by scatter index)
   - **Not Supported:** Multi-input scatter / dotproduct / nested cross-product semantics
   - **Mitigation:** Keep scatter definitions to one array input per step

### Implementation Constraints (Technical Limits)

1. **Shared Mutable State**
   - **Limit:** `produced` dict is shared across tasks
   - **Risk:** Potential race conditions if waves are incorrect
   - **Mitigation:** Topological sort ensures correctness

2. **File-Based Data Passing**
   - **Limit:** All inter-step data goes through filesystem
   - **Impact:** Large datasets can be slow
   - **Mitigation:** Use fast storage (SSD, local volumes)

3. **Scatter Concurrency Controls**
   - **Limit:** Two controls exist with different scope:
     - Local executor gate via `PREFECT_CWL_SCATTER_CONCURRENCY`
     - Prefect orchestration hard limit via tag concurrency
   - **Impact:** If only local gating is configured, other flows/workers can still consume cluster capacity
   - **Mitigation:** Configure a Prefect tag limit for `PREFECT_CWL_SCATTER_TAG` to enforce global hard caps

4. **No Output Validation**
   - **Limit:** Doesn't verify outputs exist after execution
   - **Risk:** Silent failures if tool doesn't create expected outputs
   - **Mitigation:** Plan to add in future (see Roadmap)

5. **Workspace Cleanup**
   - **Limit:** User must clean up workspace directories
   - **Risk:** Disk space exhaustion
   - **Mitigation:** Document cleanup responsibility, consider TTL

6. **K8s Job Accumulation**
   - **Limit:** Jobs remain until TTL (default 1 hour)
   - **Impact:** Namespace pollution, potential quota issues
   - **Mitigation:** Set appropriate TTL, clean up old jobs

7. **Docker User Mapping (Windows)**
   - **Limit:** UID/GID mapping only works on Unix
   - **Risk:** Permission issues on Windows hosts
   - **Mitigation:** Document as Unix-only feature

### Performance Considerations

1. **Sequential K8s Jobs**
   - **Issue:** mkdir → listings → main job executed sequentially
   - **Impact:** ~2-3 seconds overhead per step
   - **Potential Fix:** Merge mkdir + listings into single job

2. **Command Line Length**
   - **Issue:** Large base64-encoded listings in shell command
   - **Risk:** May exceed shell command length limits
   - **Potential Fix:** Write script to ConfigMap and mount

3. **No Streaming**
   - **Issue:** Cannot stream large outputs between steps
   - **Impact:** All data must fit in memory/disk
   - **Potential Fix:** Support streaming via named pipes or cloud storage
