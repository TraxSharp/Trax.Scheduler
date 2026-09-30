---
authors: [Theauxm]
areas: [scheduling]
status: accepted
---

# A settings save writes only what it names, and every running scheduler applies it

`OperationsService.UpdateSchedulerConfigAsync` changes only the fields a patch sets in the
`trax.scheduler_config` row, and every scheduler host re-reads that row every few seconds
(`SchedulerConfiguration.SettingsRefreshInterval`, 5 seconds) and applies it without a restart.
The settings are listed once, in `SchedulerSettings`, and the save, the startup load and the
refresh all go through that list.

## Status

**Accepted.** Half of it is in force. The other half, a row that stores only the settings a save
named so that the code value applies to the rest, needs a column Trax.Effect does not have yet;
see *What is still missing*.

## Why

The save used to copy the saving process's whole configuration into the row. The operations
service also runs on API-only hosts (`AddTraxJobRunner()` plus `OperationsService`), whose
configuration is the empty one `AddTraxJobRunner` registers, so changing one setting there
stored defaults for all the others, and the scheduler took them at its next restart. Between two
scheduler hosts a save carried the saving host's values for every field, and reached the other
host only when it restarted.

The options were to refuse saves on a host that does not run the scheduler and document that a
save pins every setting, or to make a save sparse and have schedulers pick it up while running.
The second is what an operator means by changing one setting, and it is the one the dashboard and
the GraphQL API already describe, so it is the one chosen.

A poll rather than the `SchedulerConfig` change signal carries the save to other processes: the
signal crosses processes only when the host has a broadcaster configured, and a settings change
must arrive either way. One primary-key read every few seconds per scheduler host is cheap.

## Consequences

**The first save must be made on a scheduler host.** The row's columns are not nullable, so
creating it records a value for every setting. Only a host that ran `AddScheduler` knows the
values the scheduler runs with, so on any other host the first save is refused with a message
saying so. After that, saves from anywhere change only what they name.

**A stored value wins over the code value until the row changes.** Until the row can leave a
setting unset, the first save records the saving host's value for every setting, and a later
change to one of them in code does not apply while the row exists. Deleting the row returns every
running scheduler to its configured values within one refresh.

**A save reaches every scheduler, including values the saving host happened to hold.** With more
than one scheduler host and no row yet, the first save records that host's values for every
setting, and the others apply them within seconds rather than at their next restart.

**A setting without a column is live-only.** `FailureCountWindow` has no column yet, so its
entry in `SchedulerSettings` has no row mapping: a patch changes the host that received it until
that host restarts, writes nothing, and the startup load and the refresh leave it alone.

**`LocalWorkerCount` applies at the next start of the worker pool.** It is stored and applied to
the options at once, but the pool starts its workers once.

## What is still missing

Sparse storage needs a way for the row to say "not set". The row is typed columns, all `NOT NULL`
except `max_active_jobs` (where null already means "no limit"), `local_worker_count` and the two
metadata cleanup columns. The smallest change is one additive Trax.Effect migration adding a
nullable `overrides` column (`jsonb` on Postgres, `TEXT` on SQLite) and a `string? Overrides`
property on `SchedulerConfig`: a JSON object holding only the settings a save named, so a new
setting needs no further migration. A row whose `overrides` is null was written by an older
version and is read as setting everything, so no operator's saved value is dropped on upgrade; a
"reset to code value" field in the patch removes a key. With that column, the first-save refusal
and the first two consequences above go away.

## Exemplars

- `SchedulerConfigFromApiHostTests` asserts that a change from an API host leaves the scheduler's
  other settings alone, that the first change from such a host is refused, that a change saved on
  one host reaches a running scheduler on another, and that removing the row restores the
  configured values.
- [Mutations: config](/docs/sdk-reference/graphql-api/mutations) is the rule this produces.

Not covered: nothing asserts that a new setting is added to `SchedulerSettings.All`; one left out
is neither saved nor applied, and would have to be caught in review.

## Changelog

- **2026-09-30**: Recorded.
