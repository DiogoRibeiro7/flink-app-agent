# Defaults And Normalization

Defaults in `flink-app-agent` are intentionally narrow.

The repository separates three concerns:

1. payload normalization
2. ambiguity policy
3. final spec validation

That separation keeps defaulted values explicit and auditable.

## Normalization

`provider_normalizer.py` maps provider-backed payloads into the internal field contract used by `FlinkJobSpec`.

Normalization may:

- map alias keys to canonical names
- drop unknown fields
- preserve partial payloads long enough for quality and ambiguity checks

Normalization does not silently turn an underspecified request into a trusted final spec.

## Default Injection

Defaults are only injected by the ambiguity policy layer, not by the base validator.

Current behavior is conservative:

- policy `fail`: no defaults are injected
- policy `minor_defaults`: safe defaults may be injected only for explicitly classified low-risk ambiguity

At the moment the main safe default is:

- `sink_topic = inferred-events` when the sink topic is missing but the rest of the request is otherwise clear

## Reporting

When defaults are injected, the run records that explicitly in both the CLI and `generation_report.json`.

The report includes:

- `ambiguity_policy`
- `ambiguity_policy_result`
- `defaults_injected`
- extraction warnings describing what was applied

The CLI summary also prints whether defaults were injected at all, including `Injected defaults: no` for straightforward runs.

## Why This Boundary Exists

Without a separate defaults layer, missing fields can look like ordinary successful parsing. That would hide interpretation risk and make provider-backed fallback harder to trust.

The current boundary makes three things visible:

- which values came from the request
- which values came from controlled defaults
- whether the run continued under a relaxed ambiguity policy
