# 1. Record architecture decisions

Status: Accepted (2026-10-27)

## Context

Decisions in this project are made by one person, often in conversation with an AI assistant, and are easy to forget or re-litigate. The project also exists to explain its trade-offs to others.

## Options considered

- Keep decisions in commit messages and the README
- Architecture Decision Records (Michael Nygard's format), one file per decision

## Decision

Use ADRs in `docs/adr/`, numbered, one page each: context, options, decision, consequences. Write an ADR when the decision is made. An accepted ADR is never edited; a change of mind is a new ADR that supersedes it.

## Consequences

+ The reasoning survives and can be read in two minutes
+ The ADR list doubles as an outline for the write-up and talk
- Ten minutes of writing per decision
