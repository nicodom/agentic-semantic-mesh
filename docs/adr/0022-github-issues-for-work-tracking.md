# 22. GitHub Issues, Milestones and a Project board to track work

Status: Accepted (2026-11-17)

## Context

Work so far is tracked in `docs/plan/` (what each milestone contains), in ADRs (decisions) and in pull requests (what changed). Loose ends have no home: open questions recorded inside ADRs (the CI eval model in ADR 0014, the catalog choice in ADR 0016), "Later" items in `docs/scope.md`, and the owner's own tasks such as creating API keys. Nothing shows at a glance what is done, what is next and what is still undecided. The project is public and meant to be explained, so the way the work is run is part of what it shows.

## Options considered

- Keep everything in Markdown (a TODO list in `docs/plan/`)
- Notion (or a similar tool): rich pages and databases, but private by default, separate from the code, and not reachable from the assistant's tools, so it would be kept in sync by hand
- GitHub Issues, Milestones and a Project board: next to the code and public; issues link to pull requests and close when they merge; scriptable with `gh`

## Decision

GitHub, used lightly:

- **Milestones** M1–M4, matching the README.
- **One issue per plan step**, with a task checklist; not one issue per small task. Pull requests close their step's issue ("Closes #N").
- **Issues for loose ends**, labelled `open-question` (to be decided, usually in an ADR), `later` (agreed future work) or `decision-needed` (a step blocked on a choice), and the owner's own tasks.
- **A public Project board** with the columns Backlog, Next, In progress and Done.

Roles stay separate: `docs/plan/` holds the narrative of each milestone and links to the issues, ADRs hold decisions, issues hold the state of the work. An issue that leads to a decision links to its ADR and closes when the ADR is accepted.

## Consequences

+ Progress, next steps and open questions are visible in one place, to the owner and to readers
+ Loose ends are no longer buried in ADRs and the scope document
+ The trail issue → pull request → ADR shows how the work was run
- Some overhead: issues and the board need updating as work moves
- A second place that mentions the plan; `docs/plan/` stays the narrative and links to issues rather than repeating their checklists
