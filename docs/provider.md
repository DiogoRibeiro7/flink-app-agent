# Provider Extraction Boundary

This repository defines a provider boundary, not a built-in provider integration.

The relevant modules are:

- `src/flink_app_agent/provider_adapter.py`
- `src/flink_app_agent/provider_normalizer.py`
- `src/flink_app_agent/llm.py`
- `src/flink_app_agent/config.py`

## Adapter Boundary

`provider_adapter.py` isolates provider-specific mechanics from the rest of the pipeline.

It handles three narrow concerns:

1. building the request message pair
2. invoking a provider client through a small protocol
3. extracting a clean JSON string from the raw provider response

The rest of the codebase does not import a provider SDK directly. It only deals with an injected `ProviderCallable`.

That keeps provider-specific concerns out of:

- spec validation
- template selection
- generation
- repair
- review
- report writing

## Structured Output Expectations

Provider-backed extraction is only accepted when the provider response can be mapped into the internal spec contract.

The current expectation is a JSON object whose keys map to the `FlinkJobSpec` fields:

- `job_family`
- `job_name`
- `source_topic`
- `sink_topic`
- `key_by`
- `event_time_field`
- `input_event_name`
- `output_event_name`
- `rule_type`
- `rule_condition`
- `time_window_minutes`

The response may use some supported aliases such as camelCase or hyphenated keys. `provider_normalizer.py` maps those aliases to canonical names before later quality, ambiguity, and validation stages.

Unknown fields are dropped. Missing required fields, incoherent family/rule combinations, and unsafe type mismatches are rejected.

## Normalization And Validation

Provider-backed extraction does not write directly into generation.

The accepted path is:

1. raw provider text
2. JSON parsing
3. provider payload normalization
4. provider quality gating
5. ambiguity assessment and policy handling
6. strict `FlinkJobSpec` validation

This separation matters because the provider normalizer handles messy external output while `spec.py` enforces the final contract used by the rest of the repository.

## Failure Behavior

Provider-backed extraction can fail in a few explicit ways:

- provider call failure
- invalid JSON
- non-object JSON
- unusable provider quality
- ambiguity that remains disallowed under the active policy
- missing required fields
- incoherent family and rule type
- type coercion failure
- spec validation failure

When fallback policy is `fail`, those errors stop the run.

When fallback policy is `deterministic`, provider extraction is attempted first and deterministic extraction is used only if the provider path fails.

## Provenance And Reporting

The report artifact records extraction provenance in a compact machine-readable form.

That includes:

- `selected_mode`
- `fallback_policy`
- `actual_path`
- `fallback_occurred`
- `fallback_reason`
- `provider_status` when relevant
- `provider_quality`
- `provider_quality_summary`
- `provider_quality_findings`
- `interpretation_risk`
- `ambiguity_status`
- `ambiguity_policy`
- `ambiguity_policy_result`
- `ambiguity_findings`
- `defaults_injected`
- extraction `warnings`
- extraction `errors`

This does not make provider-backed extraction authoritative. It makes provider-backed interpretation auditable.
