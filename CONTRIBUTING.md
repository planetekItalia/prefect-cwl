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

## Security
- Do not include secrets in code or tests
- Report security concerns privately to maintainers

## Release process (maintainers)
- Ensure changelog is updated
- Tag a release and publish artifacts as needed

## Code of Conduct
Be respectful and constructive. We follow the spirit of the Contributor Covenant. Harassment or disrespectful behavior is not tolerated.
