using LanguageExt;
using Trax.Effect.Services.ServiceTrain;
using Trax.Scheduler.Trains.ManifestManager.Junctions;

namespace Trax.Scheduler.Trains.ManifestManager;

/// <summary>
/// Orchestrates the manifest-based job scheduling system: each run is one scheduler cycle that
/// turns due manifests into work queue entries.
/// Infrastructure the scheduler registers and runs itself; not intended to be called directly.
/// </summary>
internal class ManifestManagerTrain : ServiceTrain<Unit, Unit>, IManifestManagerTrain
{
    /// <summary>
    /// One scheduler cycle, in order: reap stale Pending and InProgress metadata, load manifests,
    /// cancel jobs past their timeout, resolve stale staged entries, reap failed jobs into dead
    /// letters, determine which manifests are due, and write work queue entries for them.
    /// The manifests are loaded after the reapers so a run they fail this cycle is already
    /// counted toward its manifest's retries and dead letter.
    /// </summary>
    protected override Task<Either<Exception, Unit>> Junctions() =>
        Chain<ReapStalePendingMetadataJunction>()
            .Chain<ReapStaleInProgressMetadataJunction>()
            .Chain<LoadManifestsJunction>()
            .Chain<CancelTimedOutJobsJunction>()
            .Chain<ResolveStaleStagedEntriesJunction>()
            .Chain<ReapFailedJobsJunction>()
            .Chain<DetermineJobsToQueueJunction>()
            .Chain<CreateWorkQueueEntriesJunction>()
            .Resolve();
}
