using Microsoft.EntityFrameworkCore;
using Trax.Effect.Data.Services.DataContext;
using Trax.Effect.Enums;
using Trax.Scheduler.Extensions;

namespace Trax.Scheduler.Trains.ManifestManager.Utilities;

/// <summary>
/// Clears the replay link of a manifest's queued entry once the manifest stops replaying decisions
/// on retry, so a retry waiting out its backoff asks afresh (docs/adr/0017).
/// </summary>
internal static class RetryReplayLinks
{
    /// <summary>
    /// Clears <c>replay_decisions_of</c> on every still-queued entry of the given manifests.
    /// </summary>
    /// <param name="context">The context to clear through.</param>
    /// <param name="manifestIds">The manifests that no longer replay.</param>
    /// <param name="save">
    /// On a provider without set updates, whether to save the rows changed. False leaves them
    /// tracked for the caller's own save.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>How many entries were cleared.</returns>
    internal static async Task<int> ClearQueuedAsync(
        IDataContext context,
        IReadOnlyCollection<long> manifestIds,
        CancellationToken ct,
        bool save = true
    )
    {
        var linked = context.WorkQueues.Where(q =>
            q.ManifestId != null
            && manifestIds.Contains(q.ManifestId.Value)
            && q.Status == WorkQueueStatus.Queued
            && q.ReplayDecisionsOf != null
        );

        if (context.SupportsSetUpdates())
            return await linked.ExecuteUpdateAsync(
                s => s.SetProperty(q => q.ReplayDecisionsOf, (long?)null),
                ct
            );

        var rows = await linked.ToListAsync(ct);
        foreach (var row in rows)
            row.ReplayDecisionsOf = null;
        if (save && rows.Count > 0)
            await context.SaveChanges(ct);
        return rows.Count;
    }
}
