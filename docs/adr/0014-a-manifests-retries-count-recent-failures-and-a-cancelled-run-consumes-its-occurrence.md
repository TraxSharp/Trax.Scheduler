---
authors: [Theauxm]
areas: [scheduling]
status: accepted
---

# A manifest's retries count recent failures, and a cancelled run consumes its occurrence

Three rules decide when a scheduled manifest runs again after a run that did not succeed.
`MaxRetries(n)` is the number of retries after the first run, so a manifest is dead-lettered
when its counted failures exceed `n`. A failure counts toward that and toward the retry backoff
only while it started within `FailureCountWindow` (24 hours by default) and after the manifest's
latest resolved dead letter. A cancelled run, whether it timed out or an operator cancelled it,
consumes the occurrence it ran for: the schedule is evaluated from the later of the last success
and the last cancelled run.

## Status

**Accepted.**

## Why this is written down

Each rule replaced one that looked reasonable and failed in production-shaped cases.
`FailedCount >= MaxRetries` dead-lettered a `MaxRetries(0)` manifest before it ever ran and gave
`MaxRetries(n)` only n - 1 retries. Counting every failure since the last resolution meant a
failure a month old delayed every later run by the backoff forever, and three failures spread
over three months dead-lettered a healthy manifest. And because nothing looked at a cancelled
run, a job that always outran its timeout was queued again on the next five-second cycle, for
ever, with no backoff and no dead letter.

## Considered options

**Count consecutive failures (reset on every success).** Recommended by the audit and rejected by
the user: a manifest that alternates failure and success would never dead-letter, and flapping
is exactly what an operator needs to see. A window counts the flapping and forgets the old.

**Keep `>=` and require `MaxRetries >= 1`.** Rejected: `MaxRetries(0)` is the natural way to
say "run once, do not retry", and "retries" in the name reads as retries after the first run.
The cost is a behaviour change: the default of 3 now allows four attempts, where it allowed
three.

**Count a timeout as a failure** (so backoff and dead-lettering apply) **and let an operator's
cancel run again.** Rejected: a cancel is deliberate, and restarting a runaway run seconds after
an operator cancelled it is the opposite of what they asked. Treating both kinds of cancel alike
also needs no new column: the cancelled run's own end time is the record.

## Consequences

**The window is scheduler-wide.** A per-manifest override (`FailureWindow` on `ScheduleOptions`
and `ManifestOptions`) was asked for and needs a manifest column in Trax.Effect; until it exists
every manifest uses `FailureCountWindow`. The persisted scheduler settings row has no column for
the window either, so a value patched through `UpdateSchedulerConfigAsync` lasts until restart.

**A triggered run that is cancelled moves the schedule** the same way a triggered success
already did, because the anchor is the run's end time, not the occurrence it was queued for.

## Exemplars

- `FailureCountWindowTests` covers a failure 30 days ago (no backoff), three failures over 90
  days (no dead letter), failures inside the window (backoff and dead letter) and a configured
  window.
- `MaxRetriesBoundaryTests` pins the boundary: dead-lettered on failure n + 1, never on n.
- `CancelledOccurrenceIsNotRerunTests` covers interval, cron, `Once` and dependent manifests
  whose run was cancelled, and that the following occurrence still runs.
- [Scheduling Options](/docs/scheduler/scheduling-options) and
  [ManifestManager](/docs/scheduler/admin-trains/manifest-manager) are the rules this produces.

Not covered: nothing checks that a new place counting failures (a dashboard statistic, a health
check) applies the same window, or that a new place deciding "due" consults the cancelled run.

## Changelog

- **2026-09-30**: Recorded.
