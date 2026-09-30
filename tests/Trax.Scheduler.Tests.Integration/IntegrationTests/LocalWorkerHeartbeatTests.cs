using System.Text.Json;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Trax.Effect.Data.Services.SqlDialect;
using Trax.Effect.Enums;
using Trax.Effect.Models.BackgroundJob;
using Trax.Effect.Models.BackgroundJob.DTOs;
using Trax.Effect.Models.Metadata;
using Trax.Effect.Models.Metadata.DTOs;
using Trax.Effect.Utils;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Services.CancellationRegistry;
using Trax.Scheduler.Services.LocalWorkerService;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.JobRunner;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// A local worker keeps its claim on a job for as long as the job runs, so a job that runs longer
/// than <see cref="LocalWorkerOptions.VisibilityTimeout"/> is not claimed again by another worker,
/// and a cancel still reaches it.
/// </summary>
[TestFixture]
public class LocalWorkerHeartbeatTests : TestSetup
{
    private static readonly TimeSpan VisibilityTimeout = TimeSpan.FromSeconds(2);

    [Test]
    public async Task A_job_running_past_the_visibility_timeout_is_not_claimed_again_and_can_still_be_cancelled()
    {
        var key = Guid.NewGuid().ToString("N");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        DeliveryProbeTrain.Gates[key] = gate;
        var started = DeliveryProbeTrain.Started.GetOrAdd(
            key,
            _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        );

        var metadata = await QueueLocalJob(new DeliveryProbeInput { Key = key });

        // Two workers on one host share its cancellation registry.
        var registry = new CancellationRegistry();
        using var stop = new CancellationTokenSource();
        var first = NewWorker(registry);
        await first.StartAsync(stop.Token);

        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var claimedAt = DateTime.UtcNow;

            var second = NewWorker(registry);
            await second.StartAsync(stop.Token);

            // negative-wait: the job must still be held by the first worker well after its
            // visibility timeout has passed, which only elapsed time can show.
            while (DateTime.UtcNow - claimedAt < VisibilityTimeout * 2.5)
                await Task.Delay(TimeSpan.FromMilliseconds(100));

            DataContext.Reset();
            var jobRunnerRuns = await DataContext
                .Metadatas.AsNoTracking()
                .CountAsync(m => m.Name.Contains(nameof(JobRunnerTrain)));
            jobRunnerRuns.Should().Be(1, "only the first worker claimed the job");
            DeliveryProbeTrain.Runs[key].Should().Be(1);

            registry.TryCancel(metadata.Id).Should().BeTrue("the running job is still registered");
            DeliveryProbeTrain.Tokens[key].IsCancellationRequested.Should().BeTrue();

            await WaitForRunState(metadata.Id, TrainState.Cancelled);

            await second.StopAsync(CancellationToken.None);
        }
        finally
        {
            gate.TrySetResult();
            stop.Cancel();
            await first.StopAsync(CancellationToken.None);
            DeliveryProbeTrain.Forget(key);
        }
    }

    private LocalWorkerService NewWorker(ICancellationRegistry registry) =>
        new(
            Scope.ServiceProvider,
            new LocalWorkerOptions
            {
                WorkerCount = 1,
                PollingInterval = TimeSpan.FromMilliseconds(100),
                VisibilityTimeout = VisibilityTimeout,
                ShutdownTimeout = TimeSpan.FromSeconds(5),
            },
            registry,
            Scope.ServiceProvider.GetRequiredService<ILogger<LocalWorkerService>>(),
            Scope.ServiceProvider.GetRequiredService<ISqlDialect>()
        );

    private async Task WaitForRunState(long metadataId, TrainState state)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            DataContext.Reset();
            var row = await DataContext
                .Metadatas.AsNoTracking()
                .SingleAsync(m => m.Id == metadataId);
            if (row.TrainState == state)
                return;
            await Task.Yield();
        }

        throw new TimeoutException($"Metadata {metadataId} did not reach {state}.");
    }

    private async Task<Metadata> QueueLocalJob(DeliveryProbeInput input)
    {
        var metadata = Metadata.Create(
            new CreateMetadata
            {
                Name = typeof(DeliveryProbeTrain).FullName!,
                ExternalId = Guid.NewGuid().ToString("N"),
                Input = null,
            }
        );
        await DataContext.Track(metadata);
        await DataContext.SaveChanges(CancellationToken.None);

        var job = BackgroundJob.Create(
            new CreateBackgroundJob
            {
                MetadataId = metadata.Id,
                Input = JsonSerializer.Serialize(
                    input,
                    TraxJsonSerializationOptions.ManifestProperties
                ),
                InputType = typeof(DeliveryProbeInput).FullName,
            }
        );
        await DataContext.Track(job);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();

        return metadata;
    }
}
