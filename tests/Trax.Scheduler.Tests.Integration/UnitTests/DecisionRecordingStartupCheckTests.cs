using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Trax.Effect.Configuration.TraxBuilder;
using Trax.Effect.Data.Extensions;
using Trax.Effect.Data.Postgres.Extensions;
using Trax.Effect.Extensions;
using Trax.Mediator.Extensions;
using Trax.Mediator.Services.TrainDiscovery;
using Trax.Scheduler.Extensions;
using Trax.Scheduler.Services.DecisionRecording;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.JobRunner;

namespace Trax.Scheduler.Tests.Integration.UnitTests;

/// <summary>
/// A host that runs trains which ask a decider, without AddDecisionRecording, runs them fine but
/// fails every requeue that replays recorded decisions. It is told so at startup, by name, rather
/// than by the first replay that fails.
/// </summary>
[TestFixture]
public class DecisionRecordingStartupCheckTests
{
    [Test]
    public async Task A_host_without_decision_recording_is_warned_naming_the_trains_that_decide()
    {
        await using var provider = Build(recordDecisions: false);
        var logger = new CapturingLogger();

        await CheckOver(provider, logger).StartAsync(CancellationToken.None);

        var warning = logger.Warnings.Should().ContainSingle().Subject;
        warning.Should().Contain(typeof(IDecisionProbeTrain).FullName!);
        warning.Should().Contain("AddDecisionRecording()");
        warning
            .Should()
            .NotContain(
                typeof(ISchedulerTestTrain).FullName!,
                "a train that asks no decider has nothing to replay"
            );
    }

    [Test]
    public async Task A_host_that_records_decisions_is_not_warned()
    {
        await using var provider = Build(recordDecisions: true);
        var logger = new CapturingLogger();

        await CheckOver(provider, logger).StartAsync(CancellationToken.None);

        logger.Warnings.Should().BeEmpty();
    }

    [Test]
    public void Every_host_that_runs_trains_has_the_check_once()
    {
        var scheduler = Services(recordDecisions: false);
        scheduler.AddTraxJobRunner();
        var worker = new ServiceCollection().AddLogging();
        worker.AddTrax(trax =>
            trax.AddEffects(effects => effects).AddMediator(typeof(AssemblyMarker).Assembly)
        );
        worker.AddTraxWorker();

        foreach (var services in new[] { scheduler, worker })
            services
                .Count(d =>
                    d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(DecisionRecordingStartupCheck)
                )
                .Should()
                .Be(1);
    }

    private static DecisionRecordingStartupCheck CheckOver(
        IServiceProvider provider,
        ILogger<DecisionRecordingStartupCheck> logger
    ) =>
        new(
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IServiceProviderIsService>(),
            provider.GetRequiredService<ITrainDiscoveryService>(),
            logger
        );

    private static ServiceProvider Build(bool recordDecisions) =>
        Services(recordDecisions).BuildServiceProvider();

    private static IServiceCollection Services(bool recordDecisions)
    {
        var services = new ServiceCollection().AddLogging();
        services.AddTrax(trax =>
            trax.AddEffects(effects =>
                {
                    var data = effects.UsePostgres(TestPostgres.ConnectionString);
                    return recordDecisions ? data.AddDecisionRecording() : data;
                })
                .AddMediator(typeof(AssemblyMarker).Assembly, typeof(JobRunnerTrain).Assembly)
                .AddScheduler(scheduler => scheduler.UseInMemoryWorkers())
        );
        return services;
    }

    private sealed class CapturingLogger : ILogger<DecisionRecordingStartupCheck>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }
}
