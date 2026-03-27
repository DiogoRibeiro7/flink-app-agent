# Extraction Modes

`flink-app-agent` supports two extraction modes behind the same internal interface:

- `deterministic`
- `provider`

Deterministic extraction remains the default.

## Deterministic Mode

Deterministic mode uses `DeterministicSpecPayloadExtractor` in `src/flink_app_agent/llm.py`.

Characteristics:

- local only
- pattern-based for a narrow request surface
- predictable failure behavior
- no provider dependency

This is the baseline path for normal use and for most tests.

## Provider Mode

Provider mode uses `ProviderSpecPayloadExtractor` in `src/flink_app_agent/llm.py`.

It is optional. The repository does not ship a concrete provider integration. Instead, provider mode expects an injected callable resolved from `FLINK_AGENT_PROVIDER_ENTRY_POINT`.

Provider mode does not bypass internal validation. The path is:

1. preprocess the request
2. load the extraction prompt
3. call the injected provider callable
4. parse the provider response as JSON
5. normalize the payload through `provider_normalizer.py`
6. validate the normalized payload through `spec.py`
7. continue with template selection and generation

If any of those steps fail, the run fails explicitly unless fallback is configured.

## Fallback

Fallback is controlled by `--fallback` or `FLINK_AGENT_FALLBACK`.

Supported policies:

- `fail`
- `deterministic`

`fail` is the default.

If `provider` mode is selected with `deterministic` fallback:

- provider extraction is attempted first
- provider errors are reported clearly
- deterministic extraction is attempted next
- the actual extraction path is recorded as `provider -> deterministic`

Fallback is not a silent success path. It is surfaced in the CLI summary and in `generation_report.json`.

## CLI Surface

The extraction controls are intentionally small:

```text
--extractor deterministic|provider
--fallback fail|deterministic
```

The CLI summary reports:

- requested extractor
- actual extraction path
- whether fallback occurred
- fallback reason when fallback was triggered

## What Provider Mode Does Not Mean

Provider mode does not make the tool open-ended or autonomous.

It still operates within the same constraints:

- one strict `FlinkJobSpec`
- two supported job families
- local template selection
- deterministic repair and review stages
- machine-readable local report artifact
