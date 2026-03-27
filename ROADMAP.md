# Roadmap

This roadmap reflects the repository after the current `v0.5` changes. It is intentionally conservative. The project is still a small local tool, and future work should keep that shape unless there is a clear reason to broaden it.

## v0.3

`v0.3` provided:

- deterministic request extraction with explicit extraction-layer boundaries
- strict spec validation and normalization
- explicit template registry and template selection
- two real local Flink templates
- local project generation with placeholder rendering safeguards
- deterministic post-generation review
- machine-readable generation report output
- a simple CLI with small inspection modes
- fixture-based and end-to-end local tests

## v0.4

`v0.4` extends the repository from a single-family tool into a small multi-family generator:

- explicit `job_family` field on the spec model (`keyed_temporal_rule`, `windowed_aggregation`)
- deterministic extractor recognizes and distinguishes both families
- two real template families with family-aware rendering
- template registry resolves by both `job_family` and `rule_type`
- `{{JOB_FAMILY}}` placeholder in generated project READMEs
- family-specific review checks (function file existence, job family in README)
- generation report includes `job_family`
- CLI summaries show job family
- broader deterministic extraction patterns (`stream from kafka`, `listen to kafka`, `partition by`)
- richer fixture-based end-to-end tests covering both families
- updated documentation across all docs

## Current v0.5

`v0.5` adds a repair loop and optional compile verification to make the pipeline more trustworthy:

- standalone `repair.py` module with a deterministic multi-pass repair loop
- repair strategies: trailing placeholder removal, missing final newline, trailing whitespace
- standalone `verification.py` module for optional `mvn compile` verification
- refined staged pipeline: generate → repair → review → verify (optional) → report
- `--verify` CLI flag for opt-in compile verification
- generation report now includes `pipeline_status`, `repair_pass`, and `compile_verification`
- review module simplified to pure structural checks (repair logic extracted)
- fixed missing `WindowedCountProcessWindowFunction` import in windowed aggregation template
- new test modules: `test_repair.py` (5 tests), `test_verification.py` (3 tests)
- 70 total tests, all deterministic and local

## Likely v0.6

The next likely version should focus on:

- richer compile verification feedback (structured error extraction)
- broader deterministic extraction patterns
- stronger review checks for content consistency per family
- optional generated-project test execution

Non-goals for `v0.6`:

- many template families
- broad natural-language understanding
- hosted services
- full autonomous repair

## v1.0 Target

`v1.0` should represent a stable small internal tool rather than a broad platform.

The `v1.0` target is:

- stable template families with clear compatibility rules
- provider-backed extraction available as an optional path behind the existing interface
- stronger artifact validation after generation
- more complete generated scaffolds for the supported templates
- predictable CLI and report outputs suitable for local automation

Likely `v1.0` characteristics:

- two or a few template families, not many
- deterministic stub path retained for tests and local development
- optional real provider path that does not leak provider details across the codebase
- stronger review plus optional compile-oriented verification for generated artifacts
- generated templates that still stay small, but are more coherent and practical than the current starter scaffolds

`v1.0` is not meant to imply a fully autonomous agent or production deployment system. It is a stability milestone for the constrained-tool vision.

## Later Optional Directions

These directions are plausible later, but they are not current commitments.

### Optional provider work

- support one real extraction provider behind the existing extraction interfaces
- improve prompt versioning and prompt testing

### Optional verification work

- optional Maven compile checks for generated projects
- optional generated-project test execution
- stricter artifact completeness checks per template

### Optional template growth

- one or two additional focused template families
- richer template metadata and compatibility constraints

### Optional packaging and delivery work

- better generated project packaging guidance
- optional deployment helpers for generated projects

## Out Of Scope For Now

The following are still not near-term priorities:

- UI development
- web APIs
- database-backed state
- broad workflow orchestration
- many templates at once
- unconstrained natural-language generation
- infrastructure-heavy deployment systems

## Compact Stage Summary

- `v0.3`: stable narrow local pipeline with two real templates, deterministic review, and generation reporting
- `v0.4`: explicit multi-family support, family-aware extraction/registry/review/report, broader patterns, richer tests
- `v0.5`: repair loop, optional compile verification, refined staged pipeline, template compile fixes
- `v0.6`: richer verification feedback, broader extraction, content consistency checks
- `v1.0`: stabilize a small set of templates, keep provider-backed extraction optional, and improve artifact-level guarantees
- later: optional provider, verification, and packaging improvements if the constrained tool remains maintainable
