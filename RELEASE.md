# Release Checklist

This document describes the steps required to create a new release of **idem**.

Releases are created by tagging a version in Git. Installation is currently
performed via pip using a Git tag, for example:

    pip install git+https://github.com/<you>/idem.git@vX.Y.Z

---

## 1. Decide the release version

- Follow semantic versioning: MAJOR.MINOR.PATCH
- Decide whether this is:
  - a PATCH release (bug fixes)
  - a MINOR release (new functionality)
  - a MAJOR release (breaking changes)

---

## 2. Update version metadata

- Edit `pyproject.toml`
- Update:

  ```toml
  [project]
  version = "X.Y.Z"

## 3. Update changelog and release notes

- Update `CHANGELOG.md`
  - Move items from `[Unreleased]` into a new version section
  - Add the release version and date
  - Ensure entries are user-visible changes only

- Prepare GitHub Release notes
  - Summarize the changes for this release
  - Highlight notable features or limitations
  - Include installation instructions if applicable
