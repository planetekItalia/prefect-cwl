# Contributing to Prefect CWL

Thank you for your interest in contributing! This project welcomes issues, bug fixes, documentation, tests, and new features aligned with the roadmap.

## Ways to contribute
- **Issues:** Bug reports, feature requests, and documentation improvements
- **Code:** Fix bugs, add tests, improve docs, or implement roadmap items
- **Discussion:** Share ideas and feedback in issues/PRs/discussions

## Development setup
1. Install `uv` (https://github.com/astral-sh/uv)
2. Clone this repository
3. Install dependencies:
   ```bash
   uv sync --all-extras --group dev
   ```
4. Ensure `PYTHONPATH` includes the `prefect_cwl/` directory or install in editable mode
5. Install *pre-commit* hooks:
   ```bash
   uv run pre-commit install
   ```
6. Start Prefect server for integration testing when needed:
   ```bash
   uv run prefect server start
   ```

## Branching model
- `main` is the production branch
- `develop` is the development branch
- Create feature branches from `develop`: `feat/<short-desc>`, `fix/<short-desc>`, `docs/<short-desc>`

## Commit messages
Follow Conventional Commits:
- `feat: ...` new feature
- `fix: ...` bug fix
- `docs: ...` documentation changes
- `test: ...` tests only
- `refactor: ...` code refactor without behavior change
- `chore: ...` tooling, CI, deps

Example: `feat(docker-backend): cache pulled images`

## Pull requests
- Keep PRs focused and small
- Link related issues; describe the problem and the solution
- Include before/after behavior when relevant
- Add/Update tests and docs
- Ensure CI passes

### PR checklist
- [ ] Tests added/updated
- [ ] Documentation updated (README/DESIGN/Examples)
- [ ] Newsfragment added
    - Use `towncrier create -c <change>`
    - Follow Conventional Commits
- [ ] Type hints present for public functions
- [ ] No breaking changes unless discussed and documented
- [ ] Performance impact considered

## Code style and quality
- **Formatting:** Black, 100-char line length
- **Typing:** Mypy/pyright-friendly; use explicit types
- **Docstrings:** Google style for public APIs
- **Linting:** Ruff or flake8 recommended

Run locally:
```bash
uv run ruff check --fix
uv run pytest -q
```

## Testing
- Prefer unit tests for pure functions and parsing logic
- Add integration tests for planner, builder, and backends
- Mock Docker/K8s backends where possible for reliability

### E2E sample flow tests
- `tests/test_sample_cwl_e2e.py` is marked with `@pytest.mark.e2e`.
- Heavy external I/O/network cases are marked with `@pytest.mark.heavy_io`.
- Run non-E2E suite:
  ```bash
  uv run --group dev python -m pytest -q -m "not e2e"
  ```
- Run full suite including E2E:
  ```bash
  uv run --group dev python -m pytest -q -m "e2e or not e2e"
  ```
- Enable heavy I/O E2E inputs (remote assets/catalogs):
  ```bash
  PREFECT_CWL_E2E_HEAVY_IO=1 uv run --group dev python -m pytest -q -m "e2e or not e2e"
  ```
  or equivalently:
  ```bash
  PREFECT_CWL_E2E_NETWORK=1 uv run --group dev python -m pytest -q -m "e2e or not e2e"
  ```
- Enable K8s E2E execution:
  ```bash
  PREFECT_CWL_E2E_K8S=1 PREFECT_CWL_E2E_HEAVY_IO=1 uv run --group dev python -m pytest -q -m "e2e or not e2e"
  ```

## Security
- Do not include secrets in code or tests
- Report security concerns privately to maintainers

## Release process (maintainers)
- Ensure changelog is updated
- Tag a release and publish artifacts as needed

## Code of Conduct
Be respectful and constructive. We follow the spirit of the Contributor Covenant. Harassment or disrespectful behavior is not tolerated.
