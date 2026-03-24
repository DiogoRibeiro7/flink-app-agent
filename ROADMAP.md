# Roadmap

This roadmap reflects the current repository state. It is incremental and intentionally conservative.

The project already has:

- a deterministic extraction pipeline
- strict spec validation
- explicit template selection
- one registered real template
- local project generation
- lightweight post-generation review
- a machine-readable generation report

The next work should strengthen those pieces before adding broader scope.

## Phase 1: Stronger Extraction

Objective:
Make the deterministic extraction layer more reliable before introducing a real provider.

Likely work:

- tighten supported request patterns and error messages
- improve handling of ambiguous wording without broadening scope too quickly
- add more focused tests around supported request variants
- make safe defaults more explicit in docs and code

Exit criteria:

- fewer extractor edge cases
- clearer failure modes when essential fields are missing
- stable tests for the supported deterministic request surface

## Phase 2: Stronger Validation

Objective:
Keep invalid or inconsistent specs from reaching generation.

Likely work:

- add more spec-level compatibility checks for template selection
- validate relationships between `rule_type` and generated naming
- make normalization behavior more explicit and more testable

Exit criteria:

- template selection failures happen before generation
- validation errors remain precise and deterministic

## Phase 3: Rendering Improvements

Objective:
Make template rendering more explicit and safer without overbuilding it.

Likely work:

- strengthen unresolved placeholder reporting
- improve file rename handling and generated class naming consistency
- add template-focused tests for expected generated paths and placeholder replacement

Exit criteria:

- rendering failures are easy to diagnose
- generated file naming is consistent across the current template and future additions

## Phase 4: Template Growth

Objective:
Prepare the single-template path for one or two more templates later without broadening scope too quickly.

Likely work:

- refine template metadata and compatibility rules
- keep the current single-template registry small and explicit
- add a second template only when compatibility rules stay easy to understand

Non-goal for this phase:

- supporting many Flink patterns at once

Exit criteria:

- template selection stays small and explicit
- template-specific tests remain easy to understand

## Phase 5: Review And Repair

Objective:
Improve the deterministic review pass without turning it into a full linting or build system.

Likely work:

- expand checks for key generated files per template
- add a few more small repairs where the fix is obvious and safe
- make review output easier to consume from the CLI

Exit criteria:

- review catches common structural mistakes
- repairs remain narrow, deterministic, and easy to audit

## Phase 6: Real LLM Integration

Objective:
Replace the stub extractor with a real provider-backed implementation without changing the rest of the pipeline.

Likely work:

- implement a provider-backed extractor behind the existing extraction interface
- use the checked-in prompts as the provider input
- keep spec validation and template selection unchanged
- preserve deterministic tests for the stub path

Constraints:

- do not let provider-specific details spread across generator or review modules

Exit criteria:

- provider-backed extraction is optional and isolated
- the stub path still exists for tests and local development

## Phase 7: Deployment Support

Objective:
Add the minimum packaging and deployment support after generation is stable.

Likely work:

- generated project packaging guidance
- deployment-oriented template documentation
- optional repository-level Docker or deployment helpers later

Non-goal for this phase:

- a full deployment framework inside this repository

## Phase 8: Compile And Test Verification

Objective:
Add stronger verification after generation once template structure is stable.

Likely work:

- optional Maven compile checks for generated projects
- optional generated-project test execution
- CI checks that stay fast enough for routine development

Constraints:

- keep these checks optional at first
- avoid turning generation into a heavy build pipeline too early

## Out Of Scope For Now

The following are not current roadmap priorities:

- UI development
- web APIs
- database-backed project storage
- broad natural-language support
- many Flink template families at once
- full runtime configuration management
