# Roadmap

This roadmap reflects the repository after the current `v0.6` changes. It stays aligned with the constrained-tool vision: a small local generator with explicit extraction boundaries, strict validation, limited template scope, and machine-readable reporting.

## Current v0.6

`v0.6` is the current baseline.

The repository now provides:

- deterministic extraction as the default path
- optional provider-backed extraction behind the existing extraction interface
- strict provider normalization before the shared `FlinkJobSpec` validation path
- configurable provider fallback to deterministic extraction
- extraction provenance in both CLI summaries and `generation_report.json`
- two supported job families with explicit template resolution
- deterministic project generation from local templates
- deterministic repair, structural review, and optional compile-only verification
- fixture-driven and end-to-end coverage for deterministic, provider, and fallback extraction paths
- documentation that treats provider-backed extraction as optional and bounded, not autonomous

What `v0.6` does not claim:

- a built-in real provider integration
- broad natural-language understanding
- many template families
- autonomous repair or autonomous design
- production deployment workflows

## Likely v0.7

The next likely version should strengthen the current constrained pipeline rather than broaden it dramatically.

Most likely themes:

- richer ambiguity handling for requests that are close to supported patterns but still underspecified
- stronger deterministic repairs that remain safe, local, and auditable
- slightly broader aggregation support within the existing multi-family approach
- improved compile-time verification feedback and failure summaries
- stronger consistency checks between extracted spec, rendered files, and report output

Possible `v0.7` additions if they stay small and well-bounded:

- one more carefully chosen template family later, only if it fits the existing spec/template model
- better negative tests for ambiguous extraction and partial provider failures
- tighter report checks for artifact completeness and provenance consistency

Non-goals for `v0.7`:

- many template families at once
- broad LLM-style planning or free-form generation
- hosted services, telemetry, or remote reporting
- provider-specific logic leaking into generation or review
- repair logic that mutates generated projects beyond narrow deterministic fixups

## v1.0 Target

`v1.0` should represent a stable small internal tool, not a broad platform.

The `v1.0` target is:

- stable multi-family generation with clear compatibility rules
- optional provider-backed extraction retained under strict normalization and validation
- generation artifacts that are more consistently verified before being presented as successful
- stronger trust and provenance guarantees in CLI output and report artifacts
- predictable local behavior suitable for scripting and repeatable internal use

Likely `v1.0` characteristics:

- a small number of carefully maintained template families
- deterministic extraction kept as the default and as the baseline test path
- provider-backed extraction available as an optional front-end, not a separate product mode
- repair, review, verification, and reporting working together as one explicit quality boundary
- generated artifacts that are still starter scaffolds, but with fewer avoidable inconsistencies

`v1.0` is not meant to imply autonomous software generation, production readiness for every Flink use case, or large-scale provider orchestration. It is a stability milestone for a narrow tool.

## Later Optional Directions

These directions are plausible later, but they are not current commitments.

### Optional provider work

- support one real provider integration behind the current adapter boundary
- improve prompt versioning and prompt regression testing
- add more explicit provider failure categorization where it materially improves debugging

### Optional verification work

- optional generated-project test execution in addition to compile-only verification
- stronger compile-time error summarization in reports
- stricter artifact completeness checks per template family

### Optional template growth

- one or two more focused template families, added slowly
- richer template metadata and compatibility constraints
- slightly broader aggregation variants if they remain compatible with the small spec model

### Optional packaging and delivery work

- better guidance for packaging the generated starter projects
- small local helpers around project handoff or packaging steps

## Out Of Scope For Now

The following are still not near-term priorities:

- UI development
- web APIs
- database-backed state
- broad workflow orchestration
- unconstrained natural-language generation
- many templates at once
- infrastructure-heavy deployment systems
- claims of autonomy or general intelligence

## Compact Stage Summary

- `v0.6`: optional provider-backed extraction, deterministic fallback, extraction provenance, repair/review/verification/report pipeline, and full local test coverage across extraction paths
- `v0.7`: better ambiguity handling, stronger deterministic repairs, slightly broader aggregation support, improved compile feedback, and possibly one more carefully chosen template family
- `v1.0`: stable multi-family generation, optional provider-backed extraction under strict validation, verified artifacts, and stronger trust/provenance guarantees
- later: selective provider, verification, template, and packaging improvements if they preserve the constrained-tool shape
