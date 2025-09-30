# Repository Guidelines

## Project Structure & Module Organization
main.py orchestrates CLI workflows, while api.py and database.py expose shared data access. Services contains update, portfolio, and quality logic; providers houses third-party adapters; models and data_source.py hold schema helpers. Persist DuckDB files and staged imports in data, keep runtime logs in logs, and clone config_example.yaml into a private config.yaml.

## Build, Test, and Development Commands
Create a virtual environment with python3 -m venv .venv, activate it, then pip install -r requirements.txt. Discover tasks via python main.py --help; common flows are python main.py init to bootstrap DuckDB and python main.py daily for rolling refreshes. Run python api_server/main.py to launch the HTTP interface on port 5001. Scripts holds direct entry points; frequently used ones are python scripts/run_tests.py and python scripts/check_data.py.

## Coding Style & Naming Conventions
Target Python 3.10+, four-space indentation, snake_case functions and modules, PascalCase classes, and UPPER_CASE constants. Add type hints and concise docstrings that describe side effects and return payloads. Reuse logging.getLogger(__name__) and the handler setup already defined in main.py. Format with black and lint with flake8 before proposing changes.

## Testing Guidelines
The tests directory mixes unittest coverage, such as tests/test_jqdata.py, with API smoke exercises. For a full sweep run python scripts/run_tests.py; for focused work use pytest tests -k name -v. Name new modules test_feature.py, place shared fixtures in tests/__init__.py, and mock external providers so runs stay deterministic and offline.

## Commit & Pull Request Guidelines
Follow the short imperative commit style in git history, for example update api_servers. Avoid bundling refactors, migrations, and behavioural changes in one commit. Pull requests should summarise intent, list validation commands like python main.py daily and pytest tests -v, and call out schema or configuration impacts. Attach artefacts only when response payloads or database layouts change.

## Configuration & Security Tips
Store provider credentials for services such as JQData or Tushare in local config.yaml or environment variables. Snapshot DuckDB files to backups before running migrations like python scripts/restructure_database.py. When ports, paths, or feature flags change, update config.yaml, api_server/config.py, and the relevant docs entry to keep deployments reproducible.
