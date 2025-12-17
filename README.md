# idem

**idem** is a command-line tool for discovering identical files across one or
more directories. It helps identify unnecessary duplicates in a safe,
deterministic, and restartable way — without deleting anything automatically.

---

## Status

⚠️ **Early-stage project**

idem currently focuses on **indexing and content identification**. User-facing
reports and review workflows are planned but not yet available.

---

## Badges

![Python](https://img.shields.io/badge/python-3.11–3.14-blue)
![uv](https://img.shields.io/badge/packaging-uv-blueviolet)
![ruff](https://img.shields.io/badge/code%20style-ruff-46a2f1)
![basedpyright](https://img.shields.io/badge/types-basedpyright-4c1)
![CI](https://github.com/<you>/idem/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-green)

---

## What idem does

- Recursively scans one or more root directories
- Identifies files by **content**, not by name
- Uses **SHA-256** for content identification
- Stores results in a **SQLite-backed index**
- Supports **resumable indexing**
- Uses **bounded parallelism** for hashing
- Keeps memory usage low, even for large trees

idem never deletes files. It is designed to **surface information**, not make
destructive decisions. After reviewing the results, you can create a new
directory tree with single copies and remove the original directories yourself.

---

## What idem does *not* do (yet)

- No user-facing report of duplicate files
- No grouping or tagging of ambiguous duplicates
- No interactive review or deletion workflow

These capabilities will be added in future releases.

---

## Installation

idem is currently installed directly from Git using pip.

```bash
pip install git+https://github.com/venekamp/idem.git@v0.1.0
```
```
