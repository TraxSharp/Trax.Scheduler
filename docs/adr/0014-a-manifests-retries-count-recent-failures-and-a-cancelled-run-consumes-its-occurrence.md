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

**A manifest can carry its own window.** `FailureWindow` on `ScheduleOptions` and
`ManifestOptions` stores it in `manifest.failure_window_seconds`, and the failure count uses it
in place of `FailureCountWindow` for that manifest. Following scheduler/0011 it is written on a
seed only when the code states it, so removing it from the code leaves the stored window in
place.

**A window too short for the retry count is warned about, not refused.** Each retry waits out
its backoff first, so when the backoff for `DefaultMaxRetries` retries adds up to the window, the
oldest failure leaves it before the last one happens and a manifest that always fails is retried
for ever. The scheduler logs a warning at startup instead of refusing to build: the window and
the retry settings each change at runtime, and the combination is a trade-off rather than a value
the scheduler cannot run with. The reaper also dead-letters only a manifest with at least one
counted failure, so a negative `MaxRetries` reaching a row cannot dead-letter one that never
failed.

**A triggered run that is cancelled moves the schedule** the same way a triggered success
already did, because the anchor is the run's end time, not the occurrence it was queued for.

**A cancelled dependent run consumes only the parent success it started for.** A dependent is
anchored on when its latest run started, cancelled or successful, not when it ended: a parent
success that landed while the run was going was not seen by it, so it still earns a run.

## Exemplars

- `FailureCountWindowTests` covers a failure 30 days ago (no backoff), three failures over 90
  days (no dead letter), failures inside the window (backoff and dead letter), a configured
  window, and a manifest's own window overriding it either way.
- `MaxRetriesBoundaryTests` pins the boundary: dead-lettered on failure n + 1, never on n.
- `CancelledOccurrenceIsNotRerunTests` covers interval, cron, `Once` and dependent manifests
  whose run was cancelled, and that the following occurrence still runs.
- [Scheduling Options](/docs/scheduler/scheduling-options) and
  [ManifestManager](/docs/scheduler/admin-trains/manifest-manager) are the rules this produces.

Not covered: nothing checks that a new place counting failures (a dashboard statistic, a health
check) applies the same window, or that a new place deciding "due" consults the cancelled run.

## Changelog

- **2026-09-30**: Recorded.
- **2026-09-30**: A manifest's own `FailureWindow` overrides `FailureCountWindow`, now that
  Trax.Effect 1.57.4 has the column.
- **2026-09-30**: A window the retry backoff outlasts is warned about at startup; the reaper
  requires a counted failure before it dead-letters.
- **2026-09-30**: A cancelled dependent run is anchored on its start, not its end, so a parent
  success during the cancelled run is not lost.
