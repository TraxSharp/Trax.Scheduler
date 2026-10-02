---
authors: [Theauxm]
areas: [scheduling]
status: accepted
---

# A manifest's retry replays the decisions of the run it retries

When a manifest's run fails and the ManifestManager queues its retry, or an operator requeues the
manifest's dead letter, the new entry carries `replay_decisions_of` naming the failed run. The
retry takes the tracks the failed run's deciders chose instead of asking the model again. The
link is set only when replaying is sound; otherwise the retry asks afresh, and that is never an
error.

## Status

**Accepted.** Extends central `docs/0041`, which until now had a requeue through
`RequeueExecutionAsync` replay and a dead-letter retry and a manifest's own retry not replay.

## Why this is written down

A retry exists because something after a decision failed: a tool step threw, a database timed
out. Trax has no per-junction retry or checkpoint, so the retry runs the chain from its first
junction. Asking the model again costs a model call for each question, and can be answered
differently, so the retry may do different work from the run it retries with nothing on screen to
say so. Replaying makes the retry repeat what the failed run decided.

What is replayed is the answers, and only those. Every ordinary junction runs again, side effects
included. A train whose junctions are not safe to repeat is as unsafe to retry as before.

## When the link is set

The source is read from the database by `RetryDecisionReplay`, never taken from a caller: no
public scheduler API takes a run to replay. It is the manifest's latest finished run, when that
run failed (the same run `LoadManifestsJunction` reads to decide that the next run is a retry).
The link is set only when every run the replay will follow, the failed run and each run it
replayed in turn, passes all of these:

- **It still exists.** A run deleted outside Trax, or a chain longer than the replay will follow,
  would fail the retry permanently, because a replay that cannot be honoured fails the run
  (`docs/0041`). The check is made before the link is written, so the retry asks afresh instead.
  Once written, metadata cleanup keeps a run a queued entry or another run names.
- **It is a run of the same manifest and the same train.** Same manifest means the same owner and
  the same declared work. Answers given to another manifest's run, even of the same train with
  the same input, are another run's answers.
- **It recorded its decisions** (`decisions_recorded`). A run that did not may have acted on
  answers nobody can know.
- **It was queued with the same input.** Answers were given about an input. The work queue entry
  the run was dispatched from holds that input exactly as the run read it, so the retry's input
  (the manifest's `properties` now) is compared with it as a string, ordinally, together with the
  input type name. A manifest edited between the failure and the retry asks afresh. An edit that
  serializes differently but means the same also asks afresh; that costs a question, never a
  wrong answer, which is why the comparison is not canonicalized. A run with no entry to compare
  (its entry deleted, or run directly) asks afresh. An entry with a subject key was not queued by
  the manifest, and asks afresh too.

Last, the failed run must have decisions to replay (`HasDecisionsToReplay`, the requeue's test),
so a train that never decides is retried exactly as before. Within a linked replay, the existing
per-answer fingerprint still applies: a question reworded or offered different options since is
asked afresh.

## Considered options

**Replay only from `RequeueExecutionAsync`.** What `docs/0041` decided. Rejected for retries for
the reason above: the automatic retry is the common case, and it is the one that redid the model's
work.

**Point the link at the run that first asked, skipping the failed run.** Rejected: the replay
already follows the chain back, with the nearer run's answer winning, which is what the nearer run
acted on. Pointing past it would lose a question the failed run asked afresh.

**Compare canonicalized JSON, or hash the input.** Rejected for now: both inputs come from the same
stored string, so exact comparison already matches whenever the input is unchanged, and any
mismatch only asks afresh. A hash saves nothing when both strings are already loaded.

**Trust the link and let the replay fail when it cannot be honoured.** Rejected: a permanent
failure on a retry is a worse outcome than a fresh question, and the retry was not asked for by
anyone who expected a replay.

## Consequences

**There is no per-manifest opt-out yet.** A manifest that should always ask afresh on retry needs
a stored flag, which is a manifest column in Trax.Effect's schema. Until it exists, every manifest
retry that passes the checks above replays.

**The InMemory provider does not replay retries.** Its ManifestManager dispatches without work
queue entries, so there is no queued input to compare, and it does not record the link.

## Exemplars

- `ManifestRetryReplaysDecisionsTests` pins that a retry takes the failed run's tracks without
  asking, that three chained failures still replay the first run's answers, that a changed
  fingerprint re-asks, that both dead-letter requeues replay, that an occurrence after a success
  replays nothing, that a retry given a different input, one whose chain names a run that no
  longer exists, one of a run that did not record, a run of another manifest or of another train
  each ask afresh and complete, and that no public scheduler API accepts a run to replay.

**Enforced elsewhere:** `DecisionRecordingTests` in Trax.Effect pins the replay itself: the chain
walk, the nearest answer winning, and the fingerprint check.

Not covered: a change to what a track's junctions do is in no fingerprint and no input, so a retry
after a deploy that changed a junction replays the old answers into the new code.

## Changelog

- **2026-10-02**: Recorded.
