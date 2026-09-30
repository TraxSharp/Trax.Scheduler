using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Trax.Effect.Data.Services.DataContext;
using Trax.Effect.Enums;
using Trax.Effect.Models.WorkQueue;
using Trax.Effect.Models.WorkQueue.DTOs;
using Trax.Scheduler.Services.Scheduling;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.ManifestManager;
using Trax.Scheduler.Trains.ManifestManager.Junctions;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// A manifest holds at most one queued work queue entry (the database enforces it). Triggering a
/// manifest that already has one leaves that entry to run and queues nothing more, and one such
/// manifest does not stop the others in a group or a polling cycle from being queued.
/// </summary>
[TestFixture]
public class TriggerAlreadyQueuedTests
{
    private const string Group = "already-queued-group";

    private static Task<SchedulerE2EFixture> CreateAsync(CapturingLoggerProvider? logs = null) =>
        SchedulerE2EFixture.CreateAsync(
            s =>
                s.Schedule<ISchedulerTestTrain>(
                        "aq-a",
                        new SchedulerTestInput(),
                        Every.Hours(1),
                        opts => opts.Group(Group)
                    )
                    .Schedule<ISchedulerTestTrain>(
                        "aq-b",
                        new SchedulerTestInput(),
                        Every.Hours(1),
                        opts => opts.Group(Group)
                    ),
            services =>
            {
                if (logs is not null)
                    services.AddSingleton<ILoggerProvider>(logs);
            }
        );

    private static async Task<Dictionary<string, int>> QueuedPerManifest(SchedulerE2EFixture fx)
    {
        fx.DataContext.Reset();
        return await fx
            .DataContext.WorkQueues.AsNoTracking()
            .Where(w => w.Status == WorkQueueStatus.Queued && w.Manifest != null)
            .GroupBy(w => w.Manifest!.ExternalId)
            .ToDictionaryAsync(g => g.Key, g => g.Count());
    }

    [Test]
    public async Task Triggering_a_group_queues_the_manifests_not_yet_queued_and_reports_the_rest_skipped()
    {
        var logs = new CapturingLoggerProvider();
        await using var fx = await CreateAsync(logs);
        await fx.MaterializePendingManifestsAsync();
        await fx.Scheduler.TriggerAsync("aq-a");

        var group = await fx
            .DataContext.ManifestGroups.AsNoTracking()
            .FirstAsync(g => g.Name == Group);
        var queued = await fx.Scheduler.TriggerGroupAsync(group.Id);

        queued.Should().Be(1, "only aq-b was not already queued");
        var perManifest = await QueuedPerManifest(fx);
        perManifest.Should().Equal(new Dictionary<string, int> { ["aq-a"] = 1, ["aq-b"] = 1 });
        logs.Messages.Should()
            .Contain(m => m.Contains("1 already queued"), "the skipped manifest is reported");
    }

    [Test]
    public async Task Triggering_a_manifest_that_is_already_queued_leaves_its_entry_and_does_not_throw()
    {
        await using var fx = await CreateAsync();
        await fx.MaterializePendingManifestsAsync();
        await fx.Scheduler.TriggerAsync("aq-a");

        var act = () => fx.Scheduler.TriggerAsync("aq-a");

        await act.Should().NotThrowAsync();
        (await QueuedPerManifest(fx)).Should().Equal(new Dictionary<string, int> { ["aq-a"] = 1 });
    }

    [Test]
    public async Task Triggering_an_already_queued_manifest_with_a_delay_does_not_throw()
    {
        await using var fx = await CreateAsync();
        await fx.MaterializePendingManifestsAsync();
        await fx.Scheduler.TriggerAsync("aq-a");

        var act = () => fx.Scheduler.TriggerAsync("aq-a", TimeSpan.FromMinutes(5));

        await act.Should().NotThrowAsync();
        (await QueuedPerManifest(fx)).Should().Equal(new Dictionary<string, int> { ["aq-a"] = 1 });
    }

    [Test]
    public async Task One_manifest_already_queued_does_not_stop_the_cycle_queueing_the_others()
    {
        await using var fx = await CreateAsync();
        await fx.MaterializePendingManifestsAsync();

        var manifests = await fx
            .DataContext.Manifests.AsNoTracking()
            .Include(m => m.ManifestGroup)
            .Where(m => m.ExternalId == "aq-a" || m.ExternalId == "aq-b")
            .OrderBy(m => m.ExternalId)
            .ToListAsync();

        // The cycle loaded both as due, then aq-a was queued by a trigger before the cycle wrote.
        await fx.Scheduler.TriggerAsync("aq-a");
        fx.DataContext.Reset();

        var views = manifests
            .Select(m => new ManifestDispatchView
            {
                Manifest = m,
                ManifestGroup = m.ManifestGroup,
                FailedCount = 0,
                HasAwaitingDeadLetter = false,
                HasQueuedWork = false,
                HasActiveExecution = false,
                HasSuccessfulMetadata = false,
            })
            .ToList();

        var context = fx.Services.GetRequiredService<IDataContext>();
        using (var transaction = await context.BeginTransaction())
        {
            var junction = new CreateWorkQueueEntriesJunction(
                context,
                fx.Configuration,
                NullLogger<CreateWorkQueueEntriesJunction>.Instance
            );
            await junction.Run(views);

            // A write after the junction, as the reapers make later in the same leader
            // transaction, still succeeds.
            context.WorkQueues.Add(
                WorkQueue.Create(
                    new CreateWorkQueue
                    {
                        TrainName = typeof(ISchedulerTestTrain).FullName!,
                        Input = "{}",
                        InputTypeName = typeof(SchedulerTestInput).FullName!,
                    }
                )
            );
            await context.SaveChanges(CancellationToken.None);
            await context.CommitTransaction();
        }

        (await QueuedPerManifest(fx))
            .Should()
            .Equal(new Dictionary<string, int> { ["aq-a"] = 1, ["aq-b"] = 1 });
    }

    public sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public ConcurrentQueue<string> Messages { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Logger(Messages);

        public void Dispose() { }

        private sealed class Logger(ConcurrentQueue<string> messages) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            ) => messages.Enqueue(formatter(state, exception));
        }
    }
}
