using LanguageExt;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Services.JobDispatcherPollingService;
using Trax.Scheduler.Services.SchedulerLiveness;
using Trax.Scheduler.Trains.JobDispatcher;

namespace Trax.Scheduler.Tests.Integration.UnitTests;

/// <summary>
/// A polling interval changed at runtime applies to the running poller's next wait, without a
/// restart.
/// </summary>
[TestFixture]
public class PollingIntervalLiveTests
{
    private static readonly TimeSpan SyncTimeout = TimeSpan.FromSeconds(10);

    [Test]
    public async Task Shortening_the_dispatcher_interval_at_runtime_ends_the_current_wait()
    {
        var calls = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var train = Substitute.For<IJobDispatcherTrain>();
        train
            .When(t => t.Run(Arg.Any<Unit>(), Arg.Any<CancellationToken>()))
            .Do(_ =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                    first.TrySetResult();
                else
                    second.TrySetResult();
            });

        var config = new SchedulerConfiguration
        {
            JobDispatcherPollingInterval = TimeSpan.FromHours(1),
        };

        var sp = Substitute.For<IServiceProvider>();
        var scope = Substitute.For<IServiceScope>();
        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        var scoped = Substitute.For<IServiceProvider>();
        scope.ServiceProvider.Returns(scoped);
        scopeFactory.CreateScope().Returns(scope);
        sp.GetService(typeof(IServiceScopeFactory)).Returns(scopeFactory);
        scoped.GetService(typeof(IJobDispatcherTrain)).Returns(train);

        var service = new JobDispatcherPollingService(
            sp,
            config,
            new SchedulerLivenessMonitor(TimeProvider.System),
            NullLogger<JobDispatcherPollingService>.Instance
        );

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        try
        {
            // The first cycle runs at start.
            await first.Task.WaitAsync(SyncTimeout);

            // An hour-long wait is under way; the operator shortens the interval.
            config.JobDispatcherPollingInterval = TimeSpan.FromMilliseconds(50);

            // The new interval applies to the wait already under way; before, this waited an hour.
            await second.Task.WaitAsync(SyncTimeout);
        }
        finally
        {
            cts.Cancel();
            await service.StopAsync(CancellationToken.None);
        }
    }
}
