# Ambiguity Handling

`flink-app-agent` now treats ambiguity as a first-class extraction concern instead of letting underspecified requests fall through as if they were certain.

## What Counts As Ambiguity

The ambiguity stage runs on extracted candidate payloads before final spec validation.

Current ambiguity findings include cases such as:

- missing sink topic
- missing key field
- unclear key field with multiple plausible values
- unclear job family
- conflicting aggregation intent
- vague temporal language without a concrete numeric window

The assessment is deterministic and returns structured findings rather than a single message string.

## Ambiguous vs Invalid vs Unsupported

The repository keeps three request categories separate:

- `invalid`: the request is malformed or missing required structure for a supported family
- `ambiguous`: the request has multiple plausible interpretations or not enough specificity
- `unsupported`: the request is understandable, but outside the current feature scope

Examples:

- invalid: a keyed rule request with no source topic at all
- ambiguous: a request that asks to emit an event but never identifies the key field
- unsupported: a join request

This distinction matters because only ambiguity is eligible for policy handling.

## Policy Handling

After ambiguity assessment, the pipeline applies a small explicit policy layer.

Supported policies:

- `fail`
- `minor_defaults`

`fail` is the default and stops the run when ambiguity is present.

`minor_defaults` allows only narrow low-risk defaults. Today that mainly means applying a safe default sink topic when the rest of the request is otherwise clear enough to continue.

Major ambiguity still fails under `minor_defaults`.

## What This Does Not Do

The repository still does not:

- ask the user follow-up questions
- resolve ambiguity interactively
- infer broad business logic from vague wording
- treat provider-backed output as authoritative just because it is structured

The goal is explicit handling, not hidden cleverness.
