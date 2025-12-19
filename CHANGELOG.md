# Changelog

All notable changes to **idem** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added
- 

### Changed
- 

### Fixed
- 

---

## [0.1.0] — 2025-12-19

### Added
- Initial release of idem
- Recursive indexing of configured root directories
- Content-based file identification using SHA-256
- Low-memory, resumable indexing backed by SQLite
- Parallel hashing with bounded concurrency
- Safe handling of disappearing files and symlinks
- Deterministic, restartable indexing process

### Known limitations
- No user-facing report of duplicate files yet
- No grouping or tagging of ambiguous duplicates
- No interactive review or deletion workflow
