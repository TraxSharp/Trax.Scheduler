using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Trax.Effect.Data.Postgres.Extensions;
using Trax.Effect.Data.Services.IDataContextFactory;
using Trax.Effect.Extensions;
using Trax.Mediator.Extensions;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Extensions;
using Trax.Scheduler.Services.Operations;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.JobRunner;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// The operations surface can live on an API-only host that talks to a remote scheduler
/// (add-trax-graphql: "AddTraxJobRunner() plus services.AddScoped&lt;IOperationsService,
/// OperationsService&gt;()"), or on any one of several scheduler hosts. A settings change made
/// there changes only the setting it names, and reaches the running schedulers without a restart.
///
/// <para>Enforces <c>docs/adr/0010-a-settings-save-writes-only-what-it-names-and-every-scheduler-applies-it.md</c>.</para>
/// </summary>
[Property(
    "adr",
    "docs/adr/0010-a-settings-save-writes-only-what-it-names-and-every-scheduler-applies-it.md"
)]
[TestFixture]
[NonParallelizable]
public class SchedulerConfigFromApiHostTests
{
    private const string AdrPath =
        "docs/adr/0010-a-settings-save-writes-only-what-it-names-and-every-scheduler-applies-it.md";

    private static readonly TimeSpan SyncTimeout = TimeSpan.FromSeconds(10);

    private static ServiceProvider ApiHost() =>
        new ServiceCollection()
            .AddLogging(x => x.SetMinimumLevel(LogLevel.Warning))
            .AddTrax(trax =>
                trax.AddEffects(effects => effects.UsePostgres(TestPostgres.ConnectionString))
                    .AddMediator(typeof(AssemblyMarker).Assembly, typeof(JobRunnerTrain).Assembly)
            )
            .AddTraxJobRunner()
            .AddScoped<IOperationsService, OperationsService>()
            .BuildServiceProvider();

    private static Task<SchedulerE2EFixture> Scheduler() =>
        SchedulerE2EFixture.CreateAsync(s =>
            s.StalePendingTimeout(TimeSpan.FromMinutes(5))
                .JobDispatcherPollingInterval(TimeSpan.FromSeconds(10))
        );

    private static SchedulerConfigBootstrapHostedService SettingsService(SchedulerE2EFixture fx) =>
        fx
            .Services.GetServices<IHostedService>()
            .OfType<SchedulerConfigBootstrapHostedService>()
            .Single();

    private static async Task<OperationResult> Save(
        IServiceProvider services,
        UpdateSchedulerConfigInput input
    )
    {
        using var scope = services.CreateScope();
        return await scope
            .ServiceProvider.GetRequiredService<IOperationsService>()
            .UpdateSchedulerConfigAsync(input, CancellationToken.None);
    }

    private static async Task DeleteConfigRow()
    {
        await using var host = ApiHost();
        var factory = host.GetRequiredService<IDataContextProviderFactory>();
        using var db = await factory.CreateDbContextAsync(CancellationToken.None);
        await db.SchedulerConfigs.ExecuteDeleteAsync();
    }

    private static async Task WaitFor(Func<bool> condition, string because)
    {
        var deadline = DateTime.UtcNow + SyncTimeout;
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(50); // determinism: polls the condition, bounded by SyncTimeout
        condition().Should().BeTrue($"{because}. See {AdrPath}.");
    }

    [SetUp]
    [TearDown]
    public Task CleanRow() => DeleteConfigRow();

    [Test]
    public async Task Changing_one_setting_from_an_api_host_changes_only_that_setting()
    {
        // The scheduler host made an earlier change, so the settings row exists.
        await using (var first = await Scheduler())
            (await Save(first.Services, new UpdateSchedulerConfigInput(DefaultMaxRetries: 4)))
                .Success.Should()
                .BeTrue();

        await using (var apiHost = ApiHost())
            (await Save(apiHost, new UpdateSchedulerConfigInput(MaxActiveJobs: 20)))
                .Success.Should()
                .BeTrue();

        // The scheduler process, configured in code, restarts.
        await using var scheduler = await Scheduler();
        var settings = SettingsService(scheduler);
        await settings.StartAsync(CancellationToken.None);
        await settings.StopAsync(CancellationToken.None);

        scheduler
            .Configuration.MaxActiveJobs.Should()
            .Be(20, "that is the change the operator made");
        scheduler.Configuration.DefaultMaxRetries.Should().Be(4, "the earlier change stands");
        scheduler
            .Configuration.StalePendingTimeout.Should()
            .Be(
                TimeSpan.FromMinutes(5),
                $"nobody changed the stale pending timeout, and a save writes only what it names ({AdrPath})"
            );
        scheduler
            .Configuration.JobDispatcherPollingInterval.Should()
            .Be(TimeSpan.FromSeconds(10), "nobody changed the dispatcher polling interval");
    }

    [Test]
    public async Task The_first_change_from_a_host_that_runs_no_scheduler_is_refused()
    {
        await using var apiHost = ApiHost();

        var result = await Save(apiHost, new UpdateSchedulerConfigInput(MaxActiveJobs: 20));

        result.Success.Should().BeFalse();
        result.Message.Should().Contain("AddScheduler");

        var factory = apiHost.GetRequiredService<IDataContextProviderFactory>();
        using var db = await factory.CreateDbContextAsync(CancellationToken.None);
        (await db.SchedulerConfigs.AnyAsync())
            .Should()
            .BeFalse("the API host's defaults would have become every scheduler's settings");
    }

    [Test]
    public async Task A_change_saved_on_one_host_reaches_a_running_scheduler_on_another()
    {
        await using var running = await Scheduler();
        running.Configuration.SettingsRefreshInterval = TimeSpan.FromMilliseconds(100);
        var settings = SettingsService(running);
        await settings.StartAsync(CancellationToken.None);
        try
        {
            await using (var other = await Scheduler())
                (await Save(other.Services, new UpdateSchedulerConfigInput(MaxActiveJobs: 7)))
                    .Success.Should()
                    .BeTrue();

            await WaitFor(
                () => running.Configuration.MaxActiveJobs == 7,
                "the running scheduler applies a change another host saved"
            );

            await using (var apiHost = ApiHost())
                (await Save(apiHost, new UpdateSchedulerConfigInput(JobDispatcherEnabled: false)))
                    .Success.Should()
                    .BeTrue();

            await WaitFor(
                () => !running.Configuration.JobDispatcherEnabled,
                "a change saved on an API-only host reaches it too"
            );
            running.Configuration.MaxActiveJobs.Should().Be(7);
        }
        finally
        {
            await settings.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task Removing_the_stored_row_returns_a_running_scheduler_to_its_configured_settings()
    {
        await using var running = await Scheduler();
        running.Configuration.SettingsRefreshInterval = TimeSpan.FromMilliseconds(100);
        var settings = SettingsService(running);
        await settings.StartAsync(CancellationToken.None);
        try
        {
            (await Save(running.Services, new UpdateSchedulerConfigInput(MaxActiveJobs: 7)))
                .Success.Should()
                .BeTrue();
            running.Configuration.MaxActiveJobs.Should().Be(7);

            // A refresh has to have seen the row: removing one this host never read has nothing
            // to undo.
            var seen = settings.CompletedRefreshes;
            await WaitFor(() => settings.CompletedRefreshes > seen, "a refresh reads the row");

            await DeleteConfigRow();

            await WaitFor(
                () =>
                    running.Configuration.MaxActiveJobs
                    == new SchedulerConfiguration().MaxActiveJobs,
                "with no stored settings the configured ones apply"
            );
            running.Configuration.StalePendingTimeout.Should().Be(TimeSpan.FromMinutes(5));
        }
        finally
        {
            await settings.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task With_no_stored_row_a_runtime_change_survives_the_refresh()
    {
        await using var running = await Scheduler();
        running.Configuration.SettingsRefreshInterval = TimeSpan.FromMilliseconds(100);
        var settings = SettingsService(running);
        await settings.StartAsync(CancellationToken.None);
        try
        {
            running.Configuration.DefaultRetryDelay = TimeSpan.FromSeconds(2);
            var after = settings.CompletedRefreshes;

            await WaitFor(
                () => settings.CompletedRefreshes >= after + 2,
                "two refreshes finish after the change"
            );

            running
                .Configuration.DefaultRetryDelay.Should()
                .Be(
                    TimeSpan.FromSeconds(2),
                    "with no row ever applied, a refresh has nothing to undo"
                );
        }
        finally
        {
            await settings.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task A_live_only_setting_changes_this_host_writes_no_row_and_survives_the_refresh()
    {
        await using var running = await Scheduler();
        running.Configuration.SettingsRefreshInterval = TimeSpan.FromMilliseconds(100);
        var settings = SettingsService(running);
        await settings.StartAsync(CancellationToken.None);
        try
        {
            var result = await Save(
                running.Services,
                new UpdateSchedulerConfigInput { FailureCountWindow = TimeSpan.FromHours(2) }
            );
            result.Success.Should().BeTrue();
            result.Count.Should().Be(1);
            running.Configuration.FailureCountWindow.Should().Be(TimeSpan.FromHours(2));

            var factory = running.Services.GetRequiredService<IDataContextProviderFactory>();
            using (var db = await factory.CreateDbContextAsync(CancellationToken.None))
                (await db.SchedulerConfigs.AnyAsync())
                    .Should()
                    .BeFalse("the row has no column for a live-only setting");

            (await Save(running.Services, new UpdateSchedulerConfigInput(MaxActiveJobs: 7)))
                .Success.Should()
                .BeTrue();
            await WaitFor(
                () => running.Configuration.MaxActiveJobs == 7,
                "the refresh applies the stored change"
            );
            await Save(running.Services, new UpdateSchedulerConfigInput(MaxActiveJobs: 8));
            await WaitFor(
                () => running.Configuration.MaxActiveJobs == 8,
                "the refresh applies the second stored change"
            );

            running
                .Configuration.FailureCountWindow.Should()
                .Be(TimeSpan.FromHours(2), "applying the row does not reset a live-only setting");
        }
        finally
        {
            await settings.StopAsync(CancellationToken.None);
        }
    }
}
