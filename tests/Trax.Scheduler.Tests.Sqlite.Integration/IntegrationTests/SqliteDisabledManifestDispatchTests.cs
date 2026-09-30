using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Trax.Effect.Enums;
using Trax.Effect.Models.Manifest;
using Trax.Effect.Models.Manifest.DTOs;
using Trax.Effect.Models.WorkQueue;
using Trax.Effect.Models.WorkQueue.DTOs;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Tests.Sqlite.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Sqlite.Integration.Fixtures;
using Trax.Scheduler.Trains.JobDispatcher.Junctions;

namespace Trax.Scheduler.Tests.Sqlite.Integration.IntegrationTests;

/// <summary>
/// A disabled manifest pauses its scheduled work, and the dispatch query is what decides it, so
/// the held entries take no place in a group's share of a cycle.
/// </summary>
[TestFixture]
public class SqliteDisabledManifestDispatchTests : TestSetup
{
    private DateTime _createdAt = DateTime.UtcNow.AddMinutes(-10);

    [Test]
    public async Task Entries_held_by_disabled_manifests_do_not_take_the_groups_share_of_a_cycle()
    {
        var config = Scope.ServiceProvider.GetRequiredService<SchedulerConfiguration>();
        var original = config.MaxQueuedJobsPerCycle;
        config.MaxQueuedJobsPerCycle = 2;
        try
        {
            var group = await CreateAndSaveManifestGroup(
                DataContext,
                name: $"g-{Guid.NewGuid():N}"
            );
            var held = new List<long>();
            for (var i = 0; i < 2; i++)
            {
                var disabled = await SaveManifestAsync($"off{i}", isEnabled: false, group.Id);
                await QueueScheduledEntryAsync(disabled.Id);
                held.Add(disabled.Id);
            }
            var enabled = await SaveManifestAsync("on", isEnabled: true, group.Id);
            await QueueScheduledEntryAsync(enabled.Id);

            var loaded = await LoadQueuedAsync();

            loaded
                .Select(e => e.ManifestId)
                .Should()
                .Contain(enabled.Id, "the enabled manifest's entry is due")
                .And.NotContain(held.Cast<long?>(), "a disabled manifest's schedule is paused");
        }
        finally
        {
            config.MaxQueuedJobsPerCycle = original;
        }
    }

    private Task<List<WorkQueue>> LoadQueuedAsync() =>
        ActivatorUtilities
            .CreateInstance<LoadQueuedJobsJunction>(Scope.ServiceProvider)
            .Run(Unit.Default);

    private async Task QueueScheduledEntryAsync(long manifestId)
    {
        var entry = WorkQueue.Create(
            new CreateWorkQueue
            {
                TrainName = typeof(SchedulerTestTrain).FullName!,
                Input = "{}",
                InputTypeName = typeof(SchedulerTestInput).FullName,
                ManifestId = manifestId,
            }
        );
        // Distinct created_at values in the order queued, so the held entries rank ahead of the
        // enabled one.
        entry.CreatedAt = _createdAt = _createdAt.AddSeconds(1);
        await DataContext.Track(entry);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
    }

    private async Task<Manifest> SaveManifestAsync(string value, bool isEnabled, long groupId)
    {
        var manifest = Manifest.Create(
            new CreateManifest
            {
                Name = typeof(SchedulerTestTrain),
                IsEnabled = isEnabled,
                ScheduleType = ScheduleType.None,
                MaxRetries = 3,
                Properties = new SchedulerTestInput { Value = value },
            }
        );
        manifest.ManifestGroupId = groupId;
        await DataContext.Track(manifest);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
        return manifest;
    }
}
