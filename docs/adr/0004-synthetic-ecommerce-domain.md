# 4. Synthetic e-commerce events as the primary data source

Status: Accepted (2026-10-27)

## Context

Agent answers can only be scored if the true answer is known. Real public streams have no ground truth.

## Options considered

- A synthetic e-commerce generator (orders, carts, inventory, returns, payments)
- A real public stream (e.g. Wikimedia recent changes)
- Both

## Decision

A synthetic e-commerce generator first, with injectable scenarios (e.g. a payment-provider outage that lowers checkout conversion) whose effects are known. A real public stream is added later as a second source.

## Consequences

+ Every eval case has a ground-truth answer
+ Anomalies can be injected on demand for the workflow
+ A domain the author knows from running e-commerce engineering
- Synthetic data can be too clean; the second source tests realism
