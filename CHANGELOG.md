# Changelog

## 0.3.0

- Reworked the reference documentation into a production-level API guide.
- Added dedicated reference pages for the CLI, pipeline model, transform object, YAML / IR format, sources, and sinks.
- Expanded the transform reference into per-transform sections with signatures, Python/YAML examples, argument explanations, and linked Expression DSL guidance.
- Expanded the errors reference into a structured family-based guide for user-facing failure modes.

## 0.2.3

- Added epidemiology examples covering line-list cleanup and weekly incidence preparation.
- Integrated the epidemiology walkthrough into the examples navigation and repository examples index.
- Added regression tests to ensure the checked-in epidemiology example pipelines execute successfully.
- Fixed `Pipeline.from_yaml()` so inline YAML text is not misinterpreted as a filesystem path on CI.

## 0.2.1

- Expanded `string` with Python-style operations including casing, trimming, splitting, prefix/suffix removal, formatting, encoding, partitioning, replacement, and zero-padding.
- Added dedicated examples and reference documentation for the new `string` operations.
- Added `tox` configuration with Python 3.14 as the default test environment.
- Raised package coverage for the `wowdata` codebase to 100%.

## 0.2.0

- Added a new `string` transform for column-level string cleaning.
- Added `regex_replace` and `regex_extract` actions for normalization and substring extraction.
- Documented the new transform and related `E_STRING_*` errors in the reference docs.
- Added tests covering the new transform behavior.

## 0.1.2

- Added repository badges.
- Updated package metadata to advertise Python 3.13 and 3.14 support.

## 0.1.1

- Improved the PyPI publish workflow so re-runs can safely skip files that already exist.

## 0.1.0

- Initial tagged release.
- Added the CI matrix and PyPI publish workflows for automated packaging and release.
