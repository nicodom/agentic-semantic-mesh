# 5. Claude, OpenAI and a local model behind one provider interface

Status: Accepted (2026-10-27)

## Context

Model choice drives cost, latency and quality, and changes fast. Comparing providers on the same tasks is part of the project's value.

## Options considered

- Claude only
- Claude behind an interface, others later
- Claude, OpenAI and a local open model from day one

## Decision

A thin provider interface (chat, tool calls, token usage) with three adapters from day one: Claude, OpenAI, and a local model served by Ollama. Development loops default to the local model.

## Consequences

+ Cost/quality comparison on identical eval cases
+ Free development loop; a data-residency story via the local model
- Tool-calling differences between providers must be normalised
- Three adapters to maintain
