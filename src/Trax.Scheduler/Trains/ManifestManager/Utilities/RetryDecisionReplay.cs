using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Trax.Effect.Data.Decisions;
using Trax.Effect.Data.Services.DataContext;
using Trax.Effect.Enums;
using Trax.Effect.Models.Manifest;
using Trax.Scheduler.Trains.JobDispatcher;

namespace Trax.Scheduler.Trains.ManifestManager.Utilities;

/// <summary>
/// Chooses the run a manifest's retry replays the decisions of: the manifest's failed run, when
/// replaying its answers is sound. See <c>docs/adr/0017-a-manifests-retry-replays-the-decisions-of-the-run-it-retries.md</c>.
/// </summary>
/// <remarks>
/// <para>
/// The source is read from the database here and nowhere else, never from anything a caller
/// supplies. The two retry paths call it: the ManifestManager's retry of a failed run, and a
/// dead-letter requeue. An ordinary occurrence (the latest finished run succeeded or was
/// cancelled) replays nothing.
/// </para>
/// <para>
/// The link is only set when the replay is certain to be honoured, because a replay that cannot
/// be honoured fails the run (central <c>docs/0041</c>). Every run in the chain the replay will
/// follow, the failed run and every run it replayed in turn, must still exist, be a run of the
/// same manifest and the same train, have recorded its decisions, and have been queued with
/// exactly the input and input type the retry is queued with. Anything else returns null and the
/// retry asks its deciders afresh; it is never an error.
/// </para>
/// <para>
/// Inputs are compared as the stored strings of the work queue entries, ordinally. Both are the
/// manifest's <c>properties</c> as stored, and the dispatcher reads a run's input from exactly
/// that string, so equal strings are equal inputs. An edit that serializes differently but means
/// the same is treated as a change: that only costs a fresh question, never a wrong answer.
/// </para>
/// </remarks>
internal static class RetryDecisionReplay
{
    /// <summary>
    /// The run a retry of <paramref name="manifest"/>, queued with <paramref name="input"/> and
    /// <paramref name="inputTypeName"/>, replays the decisions of, or null when it asks afresh.
    /// </summary>
    public static async Task<long?> SourceForRetryAsync(
        IDataContext context,
        Manifest manifest,
        string? input,
        string? inputTypeName,
        ILogger logger,
        CancellationToken ct
    )
    {
        // The latest run that finished, chosen as LoadManifestsJunction chooses it. Only a failure
        // is retried; a run after a success or a cancel is an ordinary occurrence.
        var latest = await context
            .Metadatas.AsNoTracking()
            .Where(m =>
                m.ManifestId == manifest.Id
                && (
                    m.TrainState == TrainState.Completed
                    || m.TrainState == TrainState.Cancelled
                    || (
                        m.TrainState == TrainState.Failed
                        && m.FailureException != DispatchFailure.Requeued
                    )
                )
            )
            .OrderByDescending(m => m.StartTime)
            .ThenByDescending(m => m.Id)
            .Select(m => new { m.Id, m.TrainState })
            .FirstOrDefaultAsync(ct);

        if (latest is not { TrainState: TrainState.Failed })
            return null;

        var failedRun = latest.Id;

        if (await BrokenLink(context, manifest, failedRun, input, inputTypeName, ct) is { } why)
        {
            logger.LogInformation(
                "The retry of manifest {ManifestId} asks its deciders afresh instead of replaying "
                    + "run {FailedRun}: {Reason}",
                manifest.Id,
                failedRun,
                why
            );
            return null;
        }

        // The same test a requeue applies: a run that recorded an answer it acted on, or that
        // itself replayed another. A run of a train that never decides is retried plainly.
        return await context.HasDecisionsToReplay(failedRun, ct) ? failedRun : null;
    }

    /// <summary>
    /// Why the chain from <paramref name="failedRun"/> back cannot be replayed into this retry, or
    /// null when it can.
    /// </summary>
    private static async Task<string?> BrokenLink(
        IDataContext context,
        Manifest manifest,
        long failedRun,
        string? input,
        string? inputTypeName,
        CancellationToken ct
    )
    {
        var seen = new HashSet<long>();
        long? next = failedRun;

        while (next is { } id)
        {
            if (!seen.Add(id))
                return $"the runs it replays lead back to run {id}";

            // The replay itself fails a chain longer than this, so the retry does not start one.
            if (seen.Count > DecisionJournal.MaxReplayChain)
                return $"the runs it replays go back more than {DecisionJournal.MaxReplayChain} runs";

            var run = await context
                .Metadatas.AsNoTracking()
                .Where(m => m.Id == id)
                .Select(m => new
                {
                    m.Name,
                    m.ManifestId,
                    m.DecisionsRecorded,
                    m.ReplayDecisionsOf,
                })
                .FirstOrDefaultAsync(ct);

            if (run is null)
                return $"run {id} no longer exists";

            // Same manifest, so the same owner and the same declared work; same train, so the
            // answers were given to this train's questions.
            if (run.ManifestId != manifest.Id || run.Name != manifest.Name)
                return $"run {id} is not a run of this manifest's train";

            if (!run.DecisionsRecorded)
                return $"run {id} did not record its decisions";

            // The entry the run was dispatched from holds the input exactly as the run read it.
            // A run with none (run directly, or its entry gone) has no input to compare.
            var queued = await context
                .WorkQueues.AsNoTracking()
                .Where(q => q.MetadataId == id)
                .Select(q => new
                {
                    q.ManifestId,
                    q.Input,
                    q.InputTypeName,
                    q.SubjectKey,
                })
                .FirstOrDefaultAsync(ct);

            if (queued is null || queued.ManifestId != manifest.Id)
                return $"run {id} has no work queue entry of this manifest to compare inputs with";

            // A manifest's entries carry no subject key; one that does was queued some other way.
            if (queued.SubjectKey is not null)
                return $"run {id} was queued under a subject key";

            if (
                !string.Equals(queued.InputTypeName, inputTypeName, StringComparison.Ordinal)
                || !string.Equals(queued.Input, input, StringComparison.Ordinal)
            )
                return $"run {id} was given a different input from the one the retry is given";

            next = run.ReplayDecisionsOf;
        }

        return null;
    }
}
