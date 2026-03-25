# Roadmap

This roadmap reflects the repository after the current `v0.3` changes. It is intentionally conservative. The project is still a small local tool, and future work should keep that shape unless there is a clear reason to broaden it.

## Current v0.3

The repository currently provides:

- deterministic request extraction with explicit extraction-layer boundaries
- strict spec validation and normalization
- explicit template registry and template selection
- one real local Flink template
- local project generation with placeholder rendering safeguards
- deterministic post-generation review
- machine-readable generation report output
- a simple CLI with small inspection modes
- fixture-based and end-to-end local tests

This is enough for a narrow request-to-project path, but it is still intentionally limited. The repository is not yet trying to be a general Flink generation platform.

## Likely v0.4

The next likely version should improve confidence and coverage without changing the tool’s basic shape.

Primary themes:

- second real template family
- limited repair loop for safe obvious issues
- richer deterministic review checks
- better request interpretation within the current narrow scope
- stricter output guarantees

Likely changes:

- add one second template that is materially different from the current keyed rule job
- refine template selection so mismatches fail earlier and more clearly
- broaden deterministic extraction for a few more supported phrasings without moving to open-ended interpretation
- add a small repair pass only for narrow and auditable cases, such as trivial generated-file inconsistencies
- strengthen review checks around expected generated files and generated content
- tighten report contents so successful generation leaves behind a more complete machine-readable summary

Non-goals for `v0.4`:

- many template families
- broad natural-language understanding
- hosted services
- compile-and-run verification by default

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
- `v0.4`: add one more real template, limited repair, stronger review, and better deterministic request handling
- `v1.0`: stabilize a small set of templates, keep provider-backed extraction optional, and improve artifact-level guarantees
- later: optional provider, verification, and packaging improvements if the constrained tool remains maintainable
