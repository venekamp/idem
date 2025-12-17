# Contributing to idem

idem is currently developed and maintained by a single author.
Contributions are welcome, but please open an issue before starting
any significant work.

## Development setup

idem uses modern Python tooling:

- Python 3.12+
- uv
- ruff
- basedpyright
- pytest

Run all checks before submitting changes:

```bash
uv run ruff format .
uv run ruff check .
uv run basedpyright
uv run pytest
