using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Trax.Effect.Enums;
using Trax.Effect.Models.WorkQueue;
using Trax.Effect.Models.WorkQueue.DTOs;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.JobDispatcher;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// A queued entry whose stored input this host cannot read is settled as a failed run instead
/// of staying at the head of its subject, so the entries queued after it are dispatched.
/// </summary>
[TestFixture]
public class UnreadableQueuedInputTests : TestSetup
{
    [Test]
    public async Task A_poison_entry_is_failed_and_its_subjects_next_entry_dispatches()
    {
        var subject = $"subject-{Guid.NewGuid():N}";

        // The input's type was renamed since it was queued, so its name resolves to nothing.
        var poison = await Queue(
            subject,
            """{"value":"old"}""",
            "Some.Renamed.Namespace.SchedulerTestInput"
        );
        var sibling = await Queue(
            subject,
            """{"value":"new"}""",
            typeof(SchedulerTestInput).FullName!
        );

        await RunDispatcher();
        await RunDispatcher();

        DataContext.Reset();
        var poisonRow = await DataContext
            .WorkQueues.AsNoTracking()
            .SingleAsync(q => q.Id == poison.Id);
        poisonRow
            .Status.Should()
            .NotBe(WorkQueueStatus.Queued, "it would block its subject forever");
        poisonRow.MetadataId.Should().NotBeNull();
        var poisonRun = await DataContext
            .Metadatas.AsNoTracking()
            .SingleAsync(m => m.Id == poisonRow.MetadataId);
        poisonRun.TrainState.Should().Be(TrainState.Failed);
        poisonRun.FailureReason.Should().Contain("Some.Renamed.Namespace.SchedulerTestInput");

        var siblingRow = await DataContext
            .WorkQueues.AsNoTracking()
            .SingleAsync(q => q.Id == sibling.Id);
        siblingRow.Status.Should().Be(WorkQueueStatus.Dispatched, "the subject moved on");
        var siblingRun = await DataContext
            .Metadatas.AsNoTracking()
            .SingleAsync(m => m.Id == siblingRow.MetadataId);
        siblingRun.TrainState.Should().Be(TrainState.Completed);
    }

    [Test]
    public async Task An_input_whose_json_no_longer_fits_its_type_is_failed()
    {
        var entry = await Queue(
            subject: null,
            """{"value": {"not": "a string"}}""",
            typeof(SchedulerTestInput).FullName!
        );

        await RunDispatcher();

        DataContext.Reset();
        var row = await DataContext.WorkQueues.AsNoTracking().SingleAsync(q => q.Id == entry.Id);
        row.Status.Should().Be(WorkQueueStatus.Dispatched);
        var run = await DataContext
            .Metadatas.AsNoTracking()
            .SingleAsync(m => m.Id == row.MetadataId);
        run.TrainState.Should().Be(TrainState.Failed);
        run.FailureReason.Should().Contain("could not be read");
    }

    private async Task<WorkQueue> Queue(string? subject, string input, string inputTypeName)
    {
        var entry = WorkQueue.Create(
            new CreateWorkQueue
            {
                TrainName = typeof(SchedulerTestTrain).FullName!,
                Input = input,
                InputTypeName = inputTypeName,
                SubjectKey = subject,
            }
        );
        await DataContext.Track(entry);
        await DataContext.SaveChanges(CancellationToken.None);
        DataContext.Reset();
        return entry;
    }

    private async Task RunDispatcher()
    {
        using var scope = Scope.ServiceProvider.CreateScope();
        var train = scope.ServiceProvider.GetRequiredService<IJobDispatcherTrain>();
        await train.Run(Unit.Default);
    }
}
