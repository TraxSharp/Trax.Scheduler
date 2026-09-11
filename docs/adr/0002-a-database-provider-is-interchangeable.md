---
authors: [Theauxm]
areas: [scheduling, providers]
status: accepted
---

# Swapping the database provider changes no scheduler registration

`AddScheduler()` registers the same services whatever provider sits underneath it. A host
that moves from Postgres to Sqlite changes one call in `AddEffects(...)` and nothing else:
the manifest manager, the SQL dialect and the provider flag all resolve the same way.

## Status

**Accepted.**

## Why this is written down

Because the scheduler is the layer most tempted to break it. It writes SQL for the work
queue and the dead-letter table, and the two providers genuinely differ (`jsonb` and
`timestamptz` against `TEXT` and `INTEGER`, a `trax` schema against no schemas at all). The
easy path is a provider branch in the scheduler. Instead the difference is confined behind
`ISqlDialect`, and the scheduler asks for the dialect rather than asking which provider it
got.

The payoff is that a consumer can develop against Sqlite and deploy against Postgres without
a second code path, and that the Sqlite integration suite is testing the same wiring
production uses.

## Consequences

**A new provider must supply a dialect, not a special case.** Anything the scheduler cannot
express through `ISqlDialect` is a gap in the dialect, and widening it is the fix.

**A provider-specific optimisation has nowhere to live.** That is deliberate and it is a
real cost: a Postgres-only index hint or an `ON CONFLICT` form has to be expressed through
the dialect or not at all.

## Exemplars

- `ProviderConsistencyTests` builds the full `AddTrax().AddEffects(UseSqlite).AddScheduler()`
  stack against a throwaway database file and resolves the registrations that would break
  first if a provider branch crept in.

Not covered, and this is narrower than it looks: the guard asserts three registrations
resolve under Sqlite (`HasDatabaseProvider`, `ManifestManagerTrain`, `ISqlDialect`). It does
**not** diff the Sqlite service set against the Postgres one, so a service registered for
Postgres only, and not among those three, is invisible to it.

## Changelog

- **2026-09-11**: Recorded.
