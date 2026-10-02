---
authors: [Theauxm]
areas: [scheduling]
status: accepted
---

# A manifest's retry replays the decisions of the run it retries, once

When a manifest's run fails and the ManifestManager queues its retry, or an operator requeues the
manifest's dead letter, the new entry carries `replay_decisions_of` naming the failed run. The
retry takes the tracks the failed run's deciders chose instead of asking the model again. The
link is set only when replaying is sound, and at most once in a row: answers that were replayed
into a failure are not replayed again. Otherwise the retry asks afresh, and that is never an
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
public scheduler API takes a run to replay, only a `bool` that asks afresh. It is the manifest's
latest finished run, when that run failed (the run `LoadManifestsJunction` reads to decide that
the next run is a retry). The link is set only when all of these hold:

- **The manifest replays decisions on retry** (`replay_decisions_on_retry`, below).
- **The failed run asked its deciders itself.** A failed run that was itself a replay, or one
  whose answers another run already replayed and failed with (a manifest retry, or a requeue
  through `RequeueExecutionAsync`, which belongs to no manifest), is retried afresh. One bad or
  unusable answer would otherwise hold the manifest in a loop of retries, a dead letter and
  requeues that all repeat it. Because the source never replays another run, the replay reads its
  answers alone: there is no chain for the scheduler to walk or for metadata cleanup to keep.
- **It is a run of the manifest's train**, and **it recorded its decisions**
  (`decisions_recorded`) and acted on at least one. A run that did not record may have acted on
  answers nobody can know.
- **The manifest queued it, with the same input.** The work queue entry the run was dispatched
  from must belong to the manifest, carry no subject key, and hold exactly the input and input
  type the retry is queued with (the manifest's `properties` now), compared as stored strings,
  ordinally. Answers were given about an input; a manifest edited between the failure and the
  retry asks afresh. An edit that serializes differently but means the same also asks afresh,
  which costs a question, never a wrong answer, so the comparison is not canonicalized. A run with
  no entry to compare (its entry deleted) asks afresh.

What the checks guarantee is that the answers replayed were given by this manifest's own failed
run, to this train's questions, about this exact input, and have not already failed once on
replay. They are not a tenant boundary: `manifest.owner` names the application that declared a
manifest, is restamped by whichever application seeds it, and is not checked. Within the replay,
the per-answer fingerprint still applies: a question reworded or offered different options since
is asked afresh.

The lookup runs on a short-lived context of its own, outside the ManifestManager's leader
transaction, and any exception in it is logged and treated as "ask afresh", so a failed lookup
never holds up a cycle or fails a requeue. It is set-based: the ManifestManager looks up every due
retry in one pass, and a dead-letter batch or requeue-all page in one pass, each a fixed number of
queries.

## Asking afresh on purpose

**A manifest can opt out.** `ScheduleOptions.ReplayDecisionsOnRetry(false)` (or
`ManifestOptions.ReplayDecisionsOnRetry` in a batch's `configureEach`, null meaning not stated) is
stored on the manifest as `replay_decisions_on_retry`, default true, and every retry of that
manifest, a dead-letter requeue included, asks afresh. Replaying is the default because a retry
exists to repeat the run; a manifest whose questions should be answered on current information is
the exception the flag is for. It is stored rather than held in a host's configuration so every
scheduler host reads the same answer, for a manifest scheduled at runtime too. A re-seed writes it
only when the code states it (scheduler/0011), and `IOperationsService.SetManifestsReplayDecisionsOnRetryAsync`
sets it at runtime.

Turning it off reaches a retry already queued: the write clears the link on the manifest's queued
entry, and the dispatcher checks the flag again when it claims an entry, dropping the link of one
whose manifest no longer replays.

**An operator can ask afresh once.** The dead-letter requeues (single, batch and all) and the
manifest trigger take `askAfresh`. A requeue asked afresh queues no link; a trigger asked afresh
clears the link of the queued retry it releases.

## Considered options

**Replay only from `RequeueExecutionAsync`.** What `docs/0041` decided. Rejected for retries for
the reason above: the automatic retry is the common case, and it is the one that redid the model's
work.

**Replay every retry, following the chain back.** The first version of this decision. Rejected:
answers that already failed once are the likeliest cause of the next failure, and repeating them
on every retry and every requeue makes a bad answer a trap only an operator can spring.

**Compare canonicalized JSON, or hash the input.** Rejected: both inputs come from the same stored
string, so exact comparison matches whenever the input is unchanged, and any mismatch only asks
afresh.

**Trust the link and let the replay fail when it cannot be honoured.** Rejected: a permanent
failure on a retry is a worse outcome than a fresh question.

## Consequences

**The InMemory provider does not replay retries.** Its ManifestManager dispatches without work
queue entries, so there is no queued input to compare, and it does not record the link.

**Metadata cleanup repeats its keep test in every statement.** A run selected as unreferenced may
be linked before it is deleted; the delete statements test again, so it is kept with its entry.

## Exemplars

- `ManifestRetryReplaysDecisionsTests` pins that a retry takes the failed run's tracks without
  asking, that it replays once and the retry after a failed replay asks afresh, that a dead-letter
  requeue after a failed replay asks afresh, that a manifest retry asks afresh once a requeue
  replayed its failed run and failed, that a changed fingerprint re-asks, that every dead-letter
  requeue replays unless asked afresh, that a trigger asked afresh clears a queued retry's link,
  that a manifest that opts out (when scheduled, at runtime, or by a write the dispatcher must
  catch) asks afresh, that a failed lookup queues the retry to ask afresh, that a different input,
  input type, subject key, missing entry, another manifest's entry, another train's run or an
  unrecorded run each ask afresh and complete, that an occurrence after a success replays nothing,
  and that no public scheduler API accepts a run to replay.
- `ReplayDecisionsOnRetrySeedingTests` pins that the opt-out reaches the manifest through every
  way one is scheduled, that a re-seed that does not state it keeps an explicit false, that
  reading it back in `configureEach` does not state it, that a re-seed turning it off clears the
  queued retry's link, and the runtime setter.
- `MetadataCleanupTrainTests.Delete_KeepsARunLinkedAfterItWasSelected` shows that a run linked after
  the cleanup selected it is kept; the rest of that class belongs to the cleanup's own rules.

**Enforced elsewhere:** `DecisionRecordingTests` in Trax.Effect pins the replay itself and the
fingerprint check.

Not covered: a change to what a track's junctions do is in no fingerprint and no input, so a retry
after a deploy that changed a junction replays the old answers into the new code.

## Changelog

- **2026-10-02**: A retry replays at most once in a row, so the scheduler no longer walks a
  chain; operators can ask afresh on a requeue or trigger; turning the opt-out off reaches a
  queued retry; the lookup is set-based, isolated and fails open to asking afresh; cleanup keeps a
  run linked after it was selected; the owner is no longer described as a boundary.
- **2026-10-02**: A manifest can opt out of replaying decisions on retry.
- **2026-10-02**: Recorded.
