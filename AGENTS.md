# Repository Guidelines

## Project Structure and Module Organization
- Source package: `framework/`
- Input handlers: `framework/handlers/input/`
  - `expression.py`, `state_machine.py`, `factory.py`, `input_controller_type.py`
- Output handlers: `framework/handlers/output/`
  - `api_controller.py`, `kafka_controller.py`, `tsdb_controller.py`, `factory.py`, `manager.py`, `type.py`
- Subscription handlers: `framework/handlers/subscription/`
  - `factory.py`, `manager.py`, `type.py`
- Shared handler utilities: `framework/handlers/utils/`
  - `db_handler.py`, `kafka_handler.py`, `mqtt_handler.py`
- Shared utilities: `framework/utils/`
  - `expression.py`, `redis_timeseries.py`, `timeseries.py`, `timeseries_manager.py`
- Worker modules: `framework/worker/`
  - `base_worker.py`, `deployment/deployment_handler.py`, `state_handler/state_handler.py`, `utils/config_handler.py`
- Type contracts: `framework/types.py`
- Tests: `tests/` (mirrors module behavior and protocol coverage)
- Tooling files: `pyproject.toml`, `uv.lock`, `mypy.ini`, `pytest.ini`

## Documentation Source of Truth
- Use `docs/` for all project documentation.
- Entry point: `docs/index.md`.
- Do not add new module-local readme files under `framework/**`.
- Keep root `README.md` as a concise gateway; put detailed docs in `docs/`.

## Build, Test, and Development Commands
- Install dependencies and dev tools: `uv sync`
- Runtime-only dependencies: `uv sync --no-dev`
- Run tests: `uv run pytest`
- Type check: `uv run mypy --config-file mypy.ini framework tests`
- Format: `uv run black framework tests`

## Coding Style and Naming
- Python `>=3.14`
- Black line length: `120`
- Naming:
  - modules/functions/variables: `snake_case`
  - classes: `CamelCase`
  - constants: `UPPER_SNAKE`
- Prefer module loggers over `print`.
- Prefer typed interfaces from `framework/types.py` when adding handler contracts.

## Testing Guidance
- Framework: `pytest`
- Add regression tests for bug fixes and behavior changes.
- Mock external integrations (Postgres, Kafka, Redis, MQTT, Kubernetes) in unit tests.
- Keep behavior coverage aligned with current enhancements:
  - Structured consume envelopes for Kafka/MQTT with typed metadata and parse errors.
  - Protocol/type contract coverage (`tests/test_protocol_coverage.py`).
  - Worker deployment/state/config paths and failure handling.

## Commit and Pull Request Guidance
- Commits: imperative, present tense, scoped to one concern.
- PRs should include:
  - linked issue/task
  - behavior summary
  - commands executed (tests/type-check/format)
  - operational impact notes (worker behavior, schema/client generation)

## Security and Configuration Tips
- Keep secrets in `.env`; never commit credentials.
- Avoid hardcoded real hosts/passwords in docs, tests, or examples.
- Repository currently does not track an `openapi/` directory; do not assume checked-in schemas unless explicitly added.

## Related Documentation
- Docs index: `docs/index.md`
- Root overview: `README.md`
