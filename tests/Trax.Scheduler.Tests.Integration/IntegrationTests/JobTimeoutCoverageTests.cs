using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Trax.Effect.Enums;
using Trax.Effect.Models.Manifest;
using Trax.Effect.Models.Manifest.DTOs;
using Trax.Effect.Models.Metadata;
using Trax.Effect.Models.Metadata.DTOs;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.ManifestManager;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// "Each polling cycle, the CancelTimedOutJobsJunction checks all InProgress metadata and cancels
/// any where the elapsed time exceeds the manifest's TimeoutSeconds (or the global
/// DefaultJobTimeout)" (scheduling-options, Timeout Enforcement).
/// </summary>
[TestFixture]
public class JobTimeoutCoverageTests : TestSetup
{
    private SchedulerConfiguration _config = null!;
    private TimeSpan _previousTimeout;

    public override async Task TestSetUp()
    {
        await base.TestSetUp();
        _config = Scope.ServiceProvider.GetRequiredService<SchedulerConfiguration>();
        _previousTimeout = _config.DefaultJobTimeout;
        _config.DefaultJobTimeout = TimeSpan.FromMinutes(20);
    }

    [TearDown]
    public void RestoreTimeout() => _config.DefaultJobTimeout = _previousTimeout;

    [Test]
    public async Task A_queued_run_with_no_manifest_past_DefaultJobTimeout_is_cancelled()
    {
        var run = await CreateInProgressRun(manifestId: null, startedMinutesAgo: 30);

        await Scope.ServiceProvider.GetRequiredService<IManifestManagerTrain>().Run(Unit.Default);

        DataContext.Reset();
        (await DataContext.Metadatas.AsNoTracking().FirstAsync(m => m.Id == run.Id))
            .CancellationRequested.Should()
            .BeTrue("the run has been InProgress for 30 minutes against a 20 minute default");
    }

    [Test]
    public async Task A_run_of_a_manifest_disabled_while_it_runs_is_still_timed_out()
    {
        var manifest = await CreateManifest(isEnabled: false);
        var run = await CreateInProgressRun(manifest.Id, startedMinutesAgo: 30);

        await Scope.ServiceProvider.GetRequiredService<IManifestManagerTrain>().Run(Unit.Default);

        DataContext.Reset();
        (await DataContext.Metadatas.AsNoTracking().FirstAsync(m => m.Id == run.Id))
            .CancellationRequested.Should()
            .BeTrue("disabling a manifest stops new runs; it does not exempt a running one");
    }

    private async Task<Manifest> CreateManifest(bool isEnabled)
    {
        var group = await CreateAndSaveManifestGroup(
            DataContext,
            name: $"group-{Guid.NewGuid():N}"
        );
        var manifest = Manifest.Create(
            new CreateManifest
            {
                Name = typeof(SchedulerTestTrain),
                IsEnabled = isEnabled,
                ScheduleType = ScheduleType.Interval,
                IntervalSeconds = 3600,
                MaxRetries = 3,
                Properties = new SchedulerTestInput { Value = "timeout" },
            }
        );
        manifest.ManifestGroupId = group.Id;
        await DataContext.Track(manifest);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
        return manifest;
    }

    private async Task<Metadata> CreateInProgressRun(long? manifestId, int startedMinutesAgo)
    {
        var run = Metadata.Create(
            new CreateMetadata
            {
                Name = typeof(SchedulerTestTrain).FullName!,
                ExternalId = Guid.NewGuid().ToString("N"),
                Input = new SchedulerTestInput { Value = "timeout" },
                ManifestId = manifestId,
            }
        );
        run.TrainState = TrainState.InProgress;
        await DataContext.Track(run);
        await DataContext.SaveChanges(CancellationToken.None);
        await DataContext
            .Metadatas.Where(m => m.Id == run.Id)
            .ExecuteUpdateAsync(s =>
                s.SetProperty(m => m.StartTime, DateTime.UtcNow.AddMinutes(-startedMinutesAgo))
            );
        DataContext.Reset();
        return run;
    }
}
