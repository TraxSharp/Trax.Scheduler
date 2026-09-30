using Trax.Effect.Models.Manifest;
using Trax.Effect.Models.ManifestGroup;

namespace Trax.Scheduler.Trains.ManifestManager;

/// <summary>
/// Lightweight projection of a Manifest with pre-computed aggregate flags.
/// Avoids eagerly loading unbounded child collections (Metadatas, DeadLetters, WorkQueues).
/// </summary>
internal record ManifestDispatchView
{
    public required Manifest Manifest { get; init; }
    public required ManifestGroup ManifestGroup { get; init; }
    public required int FailedCount { get; init; }
    public required bool HasAwaitingDeadLetter { get; init; }
    public required bool HasQueuedWork { get; init; }
    public required bool HasActiveExecution { get; init; }
    public required bool HasSuccessfulMetadata { get; init; }

    /// <summary>
    /// When the manifest's latest successful run started, loaded for dependents only (null for
    /// every other schedule type, and when no successful run is on record).
    /// </summary>
    /// <remarks>
    /// A dependent is due when its parent succeeded after this: a parent success that lands
    /// while the dependent is running was not seen by that run, so it earns another.
    /// </remarks>
    public DateTime? LatestSuccessfulRunStart { get; init; }
}
