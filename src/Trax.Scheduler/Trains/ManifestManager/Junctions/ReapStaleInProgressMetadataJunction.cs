using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Trax.Effect.Data.Services.DataContext;
using Trax.Effect.Enums;
using Trax.Effect.Services.EffectJunction;
using Trax.Scheduler.Configuration;

namespace Trax.Scheduler.Trains.ManifestManager.Junctions;

/// <summary>
/// Fails InProgress metadata that has not completed within the configured timeout.
/// </summary>
/// <remarks>
/// Acts as a safety net for worker crashes, Lambda hard-kills, or OOM events where the
/// process dies without reaching FinishServiceTrain. If a job remains in InProgress state
/// longer than <see cref="SchedulerConfiguration.StaleInProgressTimeout"/>, this junction
/// will mark the metadata as Failed so it doesn't stay orphaned and block the
/// DormantDependentContext concurrency guard or count against MaxActiveJobs capacity.
///
/// A run whose manifest has a Timeout longer than <see cref="SchedulerConfiguration.DefaultJobTimeout"/>
/// is given as much longer: it is reaped at the later of
/// <see cref="SchedulerConfiguration.StaleInProgressTimeout"/> and its manifest's timeout plus the
/// grace the defaults leave between the job timeout and the stale timeout
/// (<c>StaleInProgressTimeout - DefaultJobTimeout</c>, never negative). A run still inside its own
/// timeout is therefore never reaped while it runs, and one that overran it is first cancelled by
/// CancelTimedOutJobsJunction and only then, once cancellation has had the same time to land,
/// failed here.
///
/// This junction runs after ReapStalePendingMetadataJunction and before LoadManifestsJunction
/// so that newly-failed metadata is counted in the same ManifestManager cycle (enabling
/// dead-lettering if retries are exhausted).
/// </remarks>
internal class ReapStaleInProgressMetadataJunction(
    IDataContext dataContext,
    SchedulerConfiguration config,
    ILogger<ReapStaleInProgressMetadataJunction> logger
) : EffectJunction<Unit, Unit>
{
    public override async Task<Unit> Run(Unit input)
    {
        var now = DateTime.UtcNow;
        var cutoff = now - config.StaleInProgressTimeout;

        // Candidates are past the global stale timeout; a run whose manifest allows it longer
        // is kept until its own threshold.
        var candidates = await dataContext
            .Metadatas.Where(m =>
                m.TrainState == TrainState.InProgress
                && m.StartTime < cutoff
                && !config.ExcludedTrainTypeNames.Contains(m.Name)
            )
            .Select(m => new
            {
                m.Id,
                m.Name,
                m.StartTime,
                m.ManifestId,
                TimeoutSeconds = m.Manifest != null ? m.Manifest.TimeoutSeconds : null,
            })
            .AsNoTracking()
            .ToListAsync(CancellationToken);

        var staleMetadata = candidates
            .Where(m => now - m.StartTime > StaleThreshold(m.TimeoutSeconds))
            .ToList();

        if (staleMetadata.Count == 0)
        {
            logger.LogDebug(
                "ReapStaleInProgressMetadataJunction: no stale in-progress metadata found"
            );
            return Unit.Default;
        }

        var staleIds = new List<long>(staleMetadata.Count);

        foreach (var md in staleMetadata)
        {
            staleIds.Add(md.Id);
            logger.LogWarning(
                "Metadata {MetadataId} (train: {TrainName}, manifest: {ManifestId}) "
                    + "has been InProgress since {StartTime} — marking as failed",
                md.Id,
                md.Name,
                md.ManifestId,
                md.StartTime
            );
        }

        await dataContext
            .Metadatas.Where(m => staleIds.Contains(m.Id) && m.TrainState == TrainState.InProgress)
            .ExecuteUpdateAsync(
                s =>
                    s.SetProperty(m => m.TrainState, TrainState.Failed)
                        .SetProperty(m => m.EndTime, now)
                        .SetProperty(
                            m => m.FailureReason,
                            "Job was stuck InProgress beyond the configured stale in-progress timeout"
                        )
                        .SetProperty(m => m.FailureException, "StaleInProgressTimeout")
                        .SetProperty(
                            m => m.FailureJunction,
                            nameof(ReapStaleInProgressMetadataJunction)
                        ),
                CancellationToken
            );

        logger.LogInformation(
            "ReapStaleInProgressMetadataJunction completed: {Count} stale in-progress job(s) marked as failed",
            staleIds.Count
        );

        return Unit.Default;
    }

    /// <summary>
    /// How long a run may stay InProgress before it is failed: the stale in-progress timeout, or
    /// its manifest's timeout plus the grace between the default job timeout and the stale timeout
    /// when that is longer.
    /// </summary>
    private TimeSpan StaleThreshold(int? manifestTimeoutSeconds)
    {
        if (manifestTimeoutSeconds is not { } seconds)
            return config.StaleInProgressTimeout;

        var grace = config.StaleInProgressTimeout - config.DefaultJobTimeout;
        if (grace < TimeSpan.Zero)
            grace = TimeSpan.Zero;

        var ownThreshold = TimeSpan.FromSeconds(seconds) + grace;
        return ownThreshold > config.StaleInProgressTimeout
            ? ownThreshold
            : config.StaleInProgressTimeout;
    }
}
