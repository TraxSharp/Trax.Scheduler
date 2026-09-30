using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Trax.Effect.Enums;
using Trax.Effect.Models.Manifest;
using Trax.Effect.Models.Manifest.DTOs;
using Trax.Effect.Models.Metadata;
using Trax.Effect.Models.Metadata.DTOs;
using Trax.Scheduler.Tests.Sqlite.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Sqlite.Integration.Fixtures;
using Trax.Scheduler.Trains.ManifestManager;

namespace Trax.Scheduler.Tests.Sqlite.Integration.IntegrationTests;

/// <summary>
/// The dependent baseline on SQLite, which stores timestamps as text: the latest successful
/// run's start is a MAX over those strings, so it has to sort like the instants it encodes.
/// </summary>
[TestFixture]
public class SqliteDependentRunsAfterEachParentSuccessTests : TestSetup
{
    private static readonly DateTime T0 = DateTime.UtcNow.AddHours(-1);

    [Test]
    public async Task A_parent_success_during_the_dependents_run_queues_the_dependent_again()
    {
        // The dependent started at T0+5, the parent succeeded again at T0+10, and the dependent
        // finished at T0+20: that run began before the parent's latest output existed.
        var dependent = await ArrangeAsync(
            parentSucceededAt: T0.AddMinutes(10),
            dependentStartedAt: T0.AddMinutes(5),
            dependentSucceededAt: T0.AddMinutes(20)
        );

        await RunManifestManagerAsync();

        (await QueuedCount(dependent)).Should().Be(1);
    }

    [Test]
    public async Task A_dependent_that_started_after_the_parents_latest_success_is_not_queued()
    {
        var dependent = await ArrangeAsync(
            parentSucceededAt: T0.AddMinutes(10),
            dependentStartedAt: T0.AddMinutes(15),
            dependentSucceededAt: T0.AddMinutes(20)
        );

        await RunManifestManagerAsync();

        (await QueuedCount(dependent)).Should().Be(0, "nothing new happened since it started");
    }

    [Test]
    public async Task The_extra_run_is_queued_once_and_not_again_after_it_succeeds()
    {
        var dependent = await ArrangeAsync(
            parentSucceededAt: T0.AddMinutes(10),
            dependentStartedAt: T0.AddMinutes(5),
            dependentSucceededAt: T0.AddMinutes(20)
        );

        await RunManifestManagerAsync();
        await RunManifestManagerAsync();
        (await QueuedCount(dependent)).Should().Be(1, "a queued entry is not duplicated");

        // The queued run goes ahead and succeeds.
        var entry = await DataContext.WorkQueues.SingleAsync(q => q.ManifestId == dependent.Id);
        entry.Status = WorkQueueStatus.Dispatched;
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
        await AddMetadataAsync(dependent.Id, TrainState.Completed, T0.AddMinutes(30));
        await SetLastSuccessAsync(dependent.Id, T0.AddMinutes(31));

        await RunManifestManagerAsync();

        (await QueuedCount(dependent)).Should().Be(0);
    }

    [Test]
    public async Task A_dependent_whose_run_history_was_pruned_falls_back_to_its_last_success()
    {
        var dependent = await ArrangeAsync(
            parentSucceededAt: T0.AddMinutes(10),
            dependentStartedAt: null,
            dependentSucceededAt: T0.AddMinutes(20)
        );

        await RunManifestManagerAsync();

        (await QueuedCount(dependent)).Should().Be(0);
    }

    private async Task<Manifest> ArrangeAsync(
        DateTime parentSucceededAt,
        DateTime? dependentStartedAt,
        DateTime dependentSucceededAt
    )
    {
        var group = await CreateAndSaveManifestGroup(
            DataContext,
            name: $"group-{Guid.NewGuid():N}"
        );

        var parent = await SaveManifestAsync(
            new CreateManifest
            {
                Name = typeof(SchedulerTestTrain),
                IsEnabled = true,
                ScheduleType = ScheduleType.Interval,
                IntervalSeconds = 86400,
                MaxRetries = 3,
                Properties = new SchedulerTestInput { Value = "parent" },
            },
            group.Id
        );
        await AddMetadataAsync(parent.Id, TrainState.Completed, parentSucceededAt.AddMinutes(-1));
        await SetLastSuccessAsync(parent.Id, parentSucceededAt);

        var dependent = await SaveManifestAsync(
            new CreateManifest
            {
                Name = typeof(SchedulerTestTrain),
                IsEnabled = true,
                ScheduleType = ScheduleType.Dependent,
                MaxRetries = 3,
                Properties = new SchedulerTestInput { Value = "dependent" },
                DependsOnManifestId = parent.Id,
            },
            group.Id
        );
        if (dependentStartedAt is not null)
            await AddMetadataAsync(dependent.Id, TrainState.Completed, dependentStartedAt.Value);
        await SetLastSuccessAsync(dependent.Id, dependentSucceededAt);

        return dependent;
    }

    private async Task<Manifest> SaveManifestAsync(CreateManifest create, long groupId)
    {
        var manifest = Manifest.Create(create);
        manifest.ManifestGroupId = groupId;
        await DataContext.Track(manifest);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
        return manifest;
    }

    private async Task AddMetadataAsync(long manifestId, TrainState state, DateTime startTime)
    {
        var metadata = Metadata.Create(
            new CreateMetadata
            {
                Name = typeof(SchedulerTestTrain).FullName!,
                ExternalId = Guid.NewGuid().ToString("N"),
                Input = new SchedulerTestInput(),
                ManifestId = manifestId,
            }
        );
        metadata.TrainState = state;
        metadata.StartTime = startTime;
        await DataContext.Track(metadata);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
    }

    private async Task SetLastSuccessAsync(long manifestId, DateTime at)
    {
        var manifest = await DataContext.Manifests.FirstAsync(m => m.Id == manifestId);
        manifest.LastSuccessfulRun = at;
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
    }

    private async Task RunManifestManagerAsync()
    {
        await Scope.ServiceProvider.GetRequiredService<IManifestManagerTrain>().Run(Unit.Default);
        DataContext.Reset();
    }

    private Task<int> QueuedCount(Manifest dependent) =>
        DataContext.WorkQueues.CountAsync(q =>
            q.ManifestId == dependent.Id && q.Status == WorkQueueStatus.Queued
        );
}
