# Roadmap

This roadmap reflects the repository at the `v0.8` milestone. The project remains a constrained local generator with explicit interpretation boundaries, strict validation, limited template scope, and machine-readable reporting.

## Current v0.8

`v0.8` is the current baseline.

The repository now provides:

- deterministic extraction as the default path
- optional provider-backed extraction behind the same bounded extraction interface
- explicit invalid vs ambiguous vs unsupported request taxonomy
- provider normalization plus provider quality gating before final validation
- an ambiguity assessment stage before final spec creation
- a conservative ambiguity policy with explicit safe-default behavior for low-risk cases only
- clarification questions for a narrow set of recoverable ambiguity cases
- explicit fallback behavior from provider-backed extraction to deterministic extraction when configured
- trust and provenance reporting in both CLI summaries and `generation_report.json`
- two supported job families with explicit template resolution
- keyed temporal-rule generation
- keyed tumbling event-time count aggregation
- keyed event-time session-window count aggregation with an explicit inactivity gap
- deterministic repair, structural review, and optional compile-only verification
- fixture-driven and end-to-end coverage for trust, ambiguity, provider degradation, fallback, and window-template selection
- documentation that treats provider-backed extraction as optional and bounded, not authoritative

What `v0.8` does not claim:

- a built-in real provider integration
- broad natural-language understanding
- broad Flink job-family coverage
- autonomous repair or autonomous design
- production deployment workflows
- arbitrary sessionization, joins, enrichment, deduplication, or sliding windows

## Next Stabilization Work

The next work should improve verification and release discipline rather than add many new Flink shapes at once.

Likely themes:

- stronger compile-time error summaries in reports
- optional execution of generated Java tests in addition to compile-only verification
- stricter generated-artifact completeness checks per template
- more explicit compatibility tests across extraction mode, rule type, and template selection
- tighter fixture coverage for pre-generation failures and low-quality provider output
- release synchronization between `develop` and `main`

## v1.0 Target

`v1.0` should represent a stable small internal tool, not a broad platform.

The `v1.0` target is:

- stable multi-family generation with clear compatibility rules
- optional provider-backed extraction kept under strict trust, provenance, normalization, and validation controls
- clear ambiguity-handling guarantees for the supported request surface
- verified scaffold artifacts with predictable local behavior suitable for scripting
- repair, review, verification, and reporting working together as one explicit quality boundary

Likely `v1.0` characteristics:

- a small number of carefully maintained template families
- deterministic extraction kept as the default and as the baseline test path
- provider-backed extraction available as an optional front-end, not a separate product mode
- generated artifacts that are still starter scaffolds, but with fewer avoidable inconsistencies
- report artifacts that are useful for both humans and downstream automation

`v1.0` is not meant to imply autonomous software generation, production readiness for every Flink use case, or large-scale provider orchestration. It is a stability milestone for a narrow tool.

## Later Optional Directions

### Optional provider work

- support one real provider integration behind the current adapter boundary
- improve extraction-input versioning and regression coverage
- add more explicit provider failure categorization where it materially improves debugging

### Optional verification work

- optional generated-project test execution
- stronger compile-time error summarization in reports
- stricter artifact completeness checks per template family

### Optional template growth

- sliding event-time counts if a second duration field can be introduced without weakening the spec boundary
- one or two additional focused template families, added slowly
- richer template metadata and compatibility constraints

### Optional delivery work

- better guidance for packaging generated starter projects
- small local helpers around handoff or packaging steps

## Out Of Scope For Now

- UI development
- web APIs
- database-backed state
- broad workflow orchestration
- unconstrained natural-language generation
- many templates at once
- infrastructure-heavy deployment systems
- claims of autonomy or general intelligence

## Compact Stage Summary

- `v0.7`: interpretation safety, request taxonomy, provider quality gating, safe-default policy boundaries, trust/provenance reporting, and degraded-extraction fixtures
- `v0.8`: clarification questions, stronger deterministic repair coverage, three explicit templates, and keyed event-time session-window aggregation
- `v1.0`: stable multi-family generation, strict trust controls, stronger verification, clear ambiguity guarantees, and release synchronization
- later: selective provider, template, verification, and packaging improvements if they preserve the constrained-tool shape
