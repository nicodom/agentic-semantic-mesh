# 14. Qwen3 14B as the default local model

Status: Accepted (2026-10-30)

## Context

ADR 0005 puts a local model served by Ollama behind the provider interface, and makes it the default for development runs. Every agent in this project works through tool calls, so the local model must emit well-formed tool calls reliably. It must also be fast enough that an eval suite runs in minutes. The development machine is an M1 Max with 64 GB of RAM.

## Options considered

- Qwen3 14B (about 9 GB at Q4): native tool calling in Ollama, optional "thinking" mode
- gpt-oss-20b (about 13 GB): OpenAI's open-weight model, native tool calling
- Llama 3.1 8B (about 5 GB): small and fast, but less reliable at tool calling

## Decision

Qwen3 14B (`qwen3:14b` in Ollama) is the default local model.

A smoke test on 2026-10-30 (Ollama 0.40, one tool, one question, three runs per mode) gave a well-formed call with correct arguments in 6 of 6 runs. Latency was about 1.5 s per call with thinking off and about 8 s with thinking on, where the model first spends about 170 tokens reasoning. That is a smoke test, not an evaluation: the M1 eval suite (step 8) is the real measure, and may lead to a superseding ADR.

## Consequences

+ Fast enough for development loops and local eval runs
+ Fits a 16 GB laptop, so readers can run the demo
- Thinking mode costs about five times the latency per call; whether it improves answers is a question for the evals, and the provider interface must let it be switched
- Too large for a standard GitHub Actions runner, so ADR 0011's "evals in CI against the local model" needs a smaller model or a self-hosted runner. Open question for M1 step 8
