prefect-cwl 0.2.0 (2026-03-03)
==============================

Features
--------

- Added runtime output artifact collection from rendered CWL output globs across Docker and Kubernetes backends, including scalar/array cardinality validation and propagation of list outputs through flow orchestration. Added a new working `sample_cwl/nbr` example for next-iteration refinement. (#900)
- Add sample CWL and DESIGN docs for step-level ResourceRequirement support.
- Basic support for scattering, without multi-input
- Kubernetes backend now merges selected Prefect work-pool/deployment job variables into spawned CWL step jobs at runtime (namespace, service account, env, volumes, volume mounts, and image pull secrets), while preserving required ``prefect-cwl`` PVC constraints.
- Read processing requirements from the CWL and apply them inside the executors


prefect-cwl 0.1.1 (02/11/26)
============================

Features
--------

- First release published
- Avoid wrapping execution inside a flow by default, offering different API to get a flow or plain execution plan
- General improvements over docs
  Ruff checks and fixes
- Relax Python version to >=3.10


Improved Documentation
----------------------

- Deploy sample tutorial
- General improvement over docstrings


Misc
----

- Sample CWL with .serve API
- Serving flow as sample reference inside the sample_cwl folder
