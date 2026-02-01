# Future Roadmap

### Priority 1: Critical Features (Next Release)

#### 1.1 Output Validation
**Goal:** Verify outputs exist after step execution

**Implementation:**
```python
# In backends, after execution:
for output_name, expected_path in step_plan.out_artifacts.items():
    if not expected_path.exists():
        raise RuntimeError(
            f"Step {step_name} did not produce {output_name} at {expected_path}"
        )
```

**Benefit:** Catch silent failures early

#### 1.2 Better Error Messages
**Goal:** Include step context in all errors

**Implementation:**
- Wrap all backend exceptions with step name, tool ID
- Include last N lines of logs in error message
- Suggest common fixes (missing permissions, wrong paths)

**Benefit:** Faster debugging

#### 1.3 Workspace Cleanup
**Goal:** Automatic cleanup of old workspaces

**Implementation:**
```python
class WorkspaceManager:
    def cleanup_old(self, max_age_days: int = 7):
        # Remove workspaces older than threshold
        pass
```

**Benefit:** Prevent disk space exhaustion

**Effort:** Low (2-3 days)

#### 1.4 Prefect JOB template
**Goal:** Reuse Prefect JOB template for all backends, merging it when spawning jobs or templates


**Benefit:** Reuse existing infrastructure


### Priority 2: Performance & Optimization (Q2 2026)

#### 2.1 Merge K8s Setup Jobs
**Goal:** Combine mkdir + listings into single job

**Implementation:**
```python
def _setup_job(self, job_name, dirs, listings):
    # Generate shell script that:
    # 1. Creates directories
    # 2. Writes listings
    # Return single job manifest
```

**Benefit:** Reduce overhead from ~3s to ~1s per step

#### 2.2 File Type Support
**Goal:** Robust File type passing between steps

**Implementation:**
- Fix edge cases with same-directory file mounts
- Support File[] types
- Validate file mounts don't conflict

**Benefit:** More CWL compatibility

#### 2.3 Caching
**Goal:** Cache unchanged step executions

**Implementation:**
- Hash step inputs (template + workflow_inputs + upstream outputs)
- Store hash → output path mapping
- Skip execution if hash matches

**Benefit:** Faster re-runs, cost savings

**Challenges:**
- Cache invalidation
- Storage backend for cache metadata
- Detecting input changes (file content vs path)

### Priority 3: CWL Feature Parity (Q3 2026)

#### 3.1 Simple Glob Patterns
**Goal:** Support basic wildcards like `*.txt`

**Implementation:**
- Expand glob at runtime after execution
- Track multiple output files
- Pass as array to downstream steps

**Benefit:** More CWL compatibility


**Challenges:**
- Prefect UI customization
- File upload handling
- Storage management

#### 3.2 JavaScript Expressions (Limited)
**Goal:** Extends `$(...)` expressions

**Implementation:**
- Parse and evaluate JavaScript in Python (js2py or similar)
- Sandbox execution for security
- Support only simple expressions (no functions)

**Benefit:** More CWL compatibility


**Risks:**
- Security concerns with code execution
- JavaScript/Python semantic differences
- Maintenance burden

### Priority 4: Advanced Features (Q4 2026)

#### 4.1 Scatter/Gather
**Goal:** Support array-based step parallelism

**Implementation:**
- Detect scatter directive in CWL
- Generate N parallel Prefect tasks
- Gather results and pass to next step

**Benefit:** Major CWL compatibility improvement

**Challenges:**
- Complex dependency tracking
- Result aggregation
- UI representation

#### 4.2 Subworkflows
**Goal:** Support nested workflows

**Implementation:**
- Recursively parse subworkflow definitions
- Create nested Prefect flows
- Track outputs across flow boundaries

**Benefit:** Modular workflow composition

#### 4.3 Resource Requirements
**Goal:** Support ResourceRequirement (CPU, memory, disk)

**Implementation:**
- Parse CWL resource hints
- Pass to backend as container limits
- Docker: `--cpus`, `--memory`
- K8s: resources.requests/limits

**Benefit:** Better resource management

### Priority 5: Developer Experience (Ongoing)

#### 5.1 Comprehensive Testing
**Current:** Basic unit tests
**Goal:** 80%+ coverage

**Tasks:**
- Unit tests for all pure functions
- Integration tests with real CWL workflows
- Backend tests with mocked Docker/K8s
- End-to-end tests in CI

#### 5.2 Documentation
**Current:** README + DESIGN
**Goal:** Full documentation site

**Tasks:**
- API reference (auto-generated from docstrings)
- Tutorial series (beginner to advanced)
- Example workflows repository
- Troubleshooting guide

#### 5.3 Observability
**Goal:** Better monitoring and debugging

**Tasks:**
- Structured logging with correlation IDs
- Metrics (step duration, failure rate)
- Tracing (OpenTelemetry integration)
- Prefect UI enhancements (custom artifacts)


---

Last updated: January 2026
