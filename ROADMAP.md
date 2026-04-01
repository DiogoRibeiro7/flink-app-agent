# Roadmap

This roadmap reflects the repository after the current `v0.7` changes. It stays aligned with the constrained-tool vision: a small local generator with explicit interpretation boundaries, strict validation, limited template scope, and machine-readable reporting.

## Current v0.7

`v0.7` is the current baseline.

The repository now provides:

- deterministic extraction as the default path
- optional provider-backed extraction behind the same bounded extraction interface
- explicit invalid vs ambiguous vs unsupported request taxonomy
- provider normalization plus provider quality gating before final validation
- an ambiguity assessment stage before final spec creation
- a conservative ambiguity policy model with explicit safe-default behavior for low-risk cases only
- explicit fallback behavior from provider-backed extraction to deterministic extraction when configured
- trust and provenance reporting in both CLI summaries and `generation_report.json`
- two supported job families with explicit template resolution
- deterministic project generation from local templates
- deterministic repair, structural review, and optional compile-only verification
- fixture-driven and end-to-end coverage for trust, ambiguity, provider degradation, and fallback paths
- documentation that treats provider-backed extraction as optional and bounded, not authoritative

What `v0.7` does not claim:

- a built-in real provider integration
- broad natural-language understanding
- many template families
- interactive clarification flows
- autonomous repair or autonomous design
- production deployment workflows

## Likely v0.8

The next likely version should continue tightening interpretation boundaries and generation quality rather than broadening the tool dramatically.

Most likely themes:

- a limited interactive clarification flow for a very small set of recoverable ambiguity cases
- one more carefully chosen template family or slightly richer aggregation support if it fits the existing spec/template model
- stronger deterministic repair coverage that stays safe, local, and auditable
- stronger compile-time verification feedback and failure summaries
- better boundaries around which defaults are acceptable and which ambiguities must still fail

Possible `v0.8` additions if they stay small and well-bounded:

- more explicit compile/report consistency checks
- stricter fixture coverage for pre-generation failure reports
- slightly better negative coverage for provider-backed low-quality output

Non-goals for `v0.8`:

- many template families at once
- broad LLM-style planning or free-form generation
- hosted services, telemetry, or remote reporting
- provider-specific logic leaking into generation or review
- default inference that silently crosses interpretation boundaries

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

These directions are plausible later, but they are not current commitments.

### Optional provider work

- support one real provider integration behind the current adapter boundary
- improve extraction-input versioning and regression coverage
- add more explicit provider failure categorization where it materially improves debugging

### Optional verification work

- optional generated-project test execution in addition to compile-only verification
- stronger compile-time error summarization in reports
- stricter artifact completeness checks per template family

### Optional template growth

- one or two more focused template families, added slowly
- richer template metadata and compatibility constraints
- slightly broader aggregation variants if they remain compatible with the small spec model

### Optional delivery work

- better guidance for packaging generated starter projects
- small local helpers around handoff or packaging steps

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

- `v0.7`: explicit ambiguity handling, request taxonomy, provider quality gating, safe-default policy boundaries, trust/provenance reporting, and fixture coverage for degraded extraction paths
- `v0.8`: limited clarification for narrow recoverable ambiguity, one careful template or richer aggregation step, stronger deterministic repair coverage, stronger compile feedback, and tighter default-inference boundaries
- `v1.0`: stable multi-family generation, optional provider-backed extraction under strict trust controls, clear ambiguity guarantees, and verified scaffold artifacts
- later: selective provider, verification, template, and packaging improvements if they preserve the constrained-tool shape
