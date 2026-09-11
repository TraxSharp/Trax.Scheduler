# Trax.Scheduler

Timetable management: manifests, the work queue, retries, dead-lettering, job dependencies,
and the remote execution topologies (HTTP workers, SQS, Lambda). It sits above
`Trax.Mediator` and below `Trax.Api`, `Trax.Dashboard` and `Trax.Samples`.

This file is the entry point. It routes; it does not restate the rules.

## Architecture decisions

`docs/adr/` records **why** things are the way they are. A documentation page says what the
rule is; an ADR says whether it is a deliberate constraint or an accident, so you can tell
which ones are safe to change. Read the relevant one before proposing to change a rule, and
if your work contradicts one, say so rather than silently overriding it.

| Working on | Read first |
| --- | --- |
| the remote run request or response | [0001](./docs/adr/0001-remote-execution-is-a-json-wire-contract.md), the shape is a deployed contract with no version field |
| SQL, or anything provider-shaped | [0002](./docs/adr/0002-a-database-provider-is-interchangeable.md), the difference belongs behind `ISqlDialect` |

Decisions binding more than one repo live in the central corpus at `Trax.Docs/adr/`, whose
index lists them by repo. Eight name `scheduler`, and `0007` (the canonical train name is the
interface FullName) is the one this repo touches most, since it is the string stored in
`work_queue.train_name` and sent over the wire. In a workspace checkout the index is at
`../Trax.Docs/adr/README.md`; that path does not resolve on GitHub, because it crosses a
repository boundary.

## When your change makes a decision

Most changes do not. When one does (reversing it would cost something real, a future reader
would ask why it is like this, and there were genuine alternatives), it takes five steps and
the build enforces four. The `adr-guard` job runs on every pull request.

| | Step | Enforced |
| --- | --- | --- |
| 1 | Notice you made a decision, and write the ADR | no, this is the human step |
| 2 | Tag it `areas`, and add it to `docs/adr/README.md` | yes |
| 3 | Say where it stands in `## Status` and record it in `## Changelog` | yes |
| 4 | Give it `## Exemplars`: guards, `**Enforced elsewhere:**`, or `**Unenforced:**` with a reason | yes |
| 5 | Have each guard you named cite the ADR back, in its docstring and its failure message | yes |

Step 1 is the only one you have to remember, because no test can detect a decision you chose
not to record. The format is
[`.claude/skills/recording-decisions/ADR-FORMAT.md`](./.claude/skills/recording-decisions/ADR-FORMAT.md).

## Guards

`tests/Trax.Scheduler.Tests.Meta/` holds eleven convention guards, and **all eleven are
shared** with the other repos. The two guards specific to this repo live with the suites they
belong to rather than in `Tests.Meta`: `RemoteRunContractTests` and `ProviderConsistencyTests`.

The census (every guard credited to an ADR or explicitly opted out) is **not** switched on
here yet. Trax.Docs runs it over its own guards; this repo will once the shared copies carry
citations of the central ADRs they enforce.

## Running the tests

```bash
docker compose up -d          # Postgres for the integration and stress suites
dotnet test
```
