using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NUnit.Framework;
using Trax.Effect.Data.InMemory.Extensions;
using Trax.Effect.Data.Services.IDataContextFactory;
using Trax.Effect.Extensions;
using Trax.Mediator.Services.TrainDiscovery;
using Trax.Mediator.Services.TrainExecution;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Services.Operations;

namespace Trax.Scheduler.Tests.Integration.UnitTests;

/// <summary>
/// Unit tests that exercise the LocalWorkerOptions branches of
/// <see cref="OperationsService.UpdateSchedulerConfigAsync"/>. The integration fixture
/// uses <c>UseInMemoryWorkers()</c> so it never registers <c>LocalWorkerOptions</c>;
/// these tests construct the service directly, over an InMemory database, to cover the
/// branch.
/// </summary>
[TestFixture]
public class OperationsServiceLocalWorkerTests
{
    private static OperationsService BuildServiceWithLocalWorkers(
        out SchedulerConfiguration cfg,
        out LocalWorkerOptions workerOpts,
        out IDataContextProviderFactory factory
    )
    {
        cfg = new SchedulerConfiguration { IsSchedulerHost = true };
        workerOpts = new LocalWorkerOptions { WorkerCount = 4 };
        var discovery = Substitute.For<ITrainDiscoveryService>();

        // A fresh InMemory database per service: the save reads and writes the settings row.
        factory = new ServiceCollection()
            .AddLogging()
            .AddTrax(trax => trax.AddEffects(effects => effects.UseInMemory()))
            .BuildServiceProvider()
            .GetRequiredService<IDataContextProviderFactory>();

        // These tests exercise scheduler-config mutation, not enqueueing, so the execution
        // service is never called — but it is required rather than optional so that no code
        // path can enqueue without going through authorization.
        return new OperationsService(
            discovery,
            factory,
            cfg,
            Substitute.For<ITrainExecutionService>(),
            workerOpts
        );
    }

    [Test]
    public async Task UpdateSchedulerConfig_LocalWorkerCount_ChangesSingleton()
    {
        var service = BuildServiceWithLocalWorkers(out _, out var workerOpts, out _);

        var result = await service.UpdateSchedulerConfigAsync(
            new UpdateSchedulerConfigInput(LocalWorkerCount: 12),
            CancellationToken.None
        );

        result.Count.Should().Be(1);

        workerOpts.WorkerCount.Should().Be(12);
    }

    [Test]
    public async Task UpdateSchedulerConfig_ClearLocalWorkerCount_ResetsToProcessorCount()
    {
        var service = BuildServiceWithLocalWorkers(out _, out var workerOpts, out _);
        workerOpts.WorkerCount = 999; // far from Environment.ProcessorCount

        await service.UpdateSchedulerConfigAsync(
            new UpdateSchedulerConfigInput(ClearLocalWorkerCount: true),
            CancellationToken.None
        );

        workerOpts.WorkerCount.Should().Be(Environment.ProcessorCount);
    }

    [Test]
    public async Task UpdateSchedulerConfig_ClearLocalWorkerCount_AlreadyDefault_NoChange()
    {
        var service = BuildServiceWithLocalWorkers(out _, out var workerOpts, out _);
        workerOpts.WorkerCount = Environment.ProcessorCount;

        var result = await service.UpdateSchedulerConfigAsync(
            new UpdateSchedulerConfigInput(ClearLocalWorkerCount: true),
            CancellationToken.None
        );

        result.Count.Should().Be(0);
        result.Success.Should().BeTrue();
    }

    [Test]
    public async Task BootstrapHostedService_FailedDependencyResolution_LogsAndReturns()
    {
        // Service provider with no registrations at all → GetRequiredService throws
        // inside StartAsync; the hosted service should swallow the exception.
        var emptyServices = new ServiceCollection().BuildServiceProvider();
        var hosted = new SchedulerConfigBootstrapHostedService(
            emptyServices,
            NullLogger<SchedulerConfigBootstrapHostedService>.Instance
        );

        // Should NOT throw.
        Func<Task> act = () => hosted.StartAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();
    }
}
