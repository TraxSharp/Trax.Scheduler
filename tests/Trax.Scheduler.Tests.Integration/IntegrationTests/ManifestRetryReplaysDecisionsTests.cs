using System.Reflection;
using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Trax.Core.Decisions;
using Trax.Effect.Data.Extensions;
using Trax.Effect.Data.Postgres.Extensions;
using Trax.Effect.Data.Services.DataContext;
using Trax.Effect.Data.Services.IDataContextFactory;
using Trax.Effect.Enums;
using Trax.Effect.Extensions;
using Trax.Effect.Models.DeadLetter;
using Trax.Effect.Models.Manifest;
using Trax.Effect.Models.Manifest.DTOs;
using Trax.Effect.Models.Metadata;
using Trax.Effect.Models.WorkQueue;
using Trax.Effect.Provider.Json.Extensions;
using Trax.Effect.Provider.Parameter.Extensions;
using Trax.Mediator.Extensions;
using Trax.Scheduler.Extensions;
using Trax.Scheduler.Services.TraxScheduler;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.JobDispatcher;
using Trax.Scheduler.Trains.JobRunner;
using Trax.Scheduler.Trains.ManifestManager;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// A manifest's retry, made by the ManifestManager or by a dead-letter requeue, replays the
/// decisions its failed run recorded instead of asking the decider again, and only when that is
/// sound: the source is the failed run of the same manifest and train, read from the database,
/// every run the replay follows recorded its decisions and still exists, and each was given the
/// input the retry is given. Anything else asks afresh rather than failing the retry.
///
/// <para>Enforces <c>docs/adr/0017-a-manifests-retry-replays-the-decisions-of-the-run-it-retries.md</c>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Property("adr", "docs/adr/0017-a-manifests-retry-replays-the-decisions-of-the-run-it-retries.md")]
public class ManifestRetryReplaysDecisionsTests
{
    private const string Adr =
        "docs/adr/0017-a-manifests-retry-replays-the-decisions-of-the-run-it-retries.md";

    private ServiceProvider _provider = null!;
    private ScriptedDecider _decider = null!;

    [OneTimeSetUp]
    public void BuildProvider()
    {
        _decider = new ScriptedDecider();

        _provider = new ServiceCollection()
            .AddLogging(x => x.SetMinimumLevel(LogLevel.Warning))
            .AddSingleton<IDecider>(_decider)
            .AddTrax(trax =>
                trax.AddEffects(effects =>
                        effects
                            .SaveTrainParameters()
                            .UsePostgres(TestPostgres.ConnectionString)
                            .AddDecisionRecording()
                            .AddJson()
                    )
                    .AddMediator(typeof(AssemblyMarker).Assembly, typeof(JobRunnerTrain).Assembly)
                    // No backoff, so a retry is dispatched on the cycle that queues it.
                    .AddScheduler(scheduler =>
                        scheduler.UseInMemoryWorkers().DefaultRetryDelay(TimeSpan.Zero)
                    )
            )
            .AddScoped<IDataContext>(sp =>
                (IDataContext)sp.GetRequiredService<IDataContextProviderFactory>().Create()
            )
            .BuildServiceProvider();
    }

    [OneTimeTearDown]
    public async Task DisposeProvider() => await _provider.DisposeAsync();

    [SetUp]
    public async Task Clean()
    {
        DecisionProbe.Reset();
        Answer(ProbeLane.Slow, ProbeSize.Large);

        using var scope = _provider.CreateScope();
        await TestSetup.CleanupDatabase(scope.ServiceProvider.GetRequiredService<IDataContext>());
    }

    [TearDown]
    public void ResetProbe() => DecisionProbe.Reset();

    [Test]
    public async Task A_retry_takes_the_failed_runs_tracks_without_asking_the_decider()
    {
        var manifest = await CreateManifestAsync("retry");
        var before = _decider.Requests.Count;

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);
        (_decider.Requests.Count - before).Should().Be(2, "the first run asked both questions");

        // A decider asked now would send the retry down the other tracks.
        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var retry = await CycleAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .Be(failed.Id, $"the retry replays the run it retries. See {Adr}");
        (await EntryOf(retry.Id)).ReplayDecisionsOf.Should().Be(failed.Id);
        (_decider.Requests.Count - before)
            .Should()
            .Be(2, $"the retry asked nothing: two questions in all, not four. See {Adr}");
        DecisionProbe
            .TracksOf("retry")
            .Should()
            .Equal(["Slow", "Large", "Slow", "Large"], "the retry took the failed run's tracks");
        (await DecisionsOf(retry.Id))
            .Should()
            .HaveCount(2)
            .And.OnlyContain(d => d.Replayed, "both answers came from the failed run");
    }

    [Test]
    public async Task A_retry_after_three_failures_still_replays_the_first_runs_answers()
    {
        var manifest = await CreateManifestAsync("chain", maxRetries: 3);
        var before = _decider.Requests.Count;

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var first = await CycleAsync(manifest);
        first.TrainState.Should().Be(TrainState.Failed);

        // The second records nothing of its own; the third replays both answers again.
        DecisionProbe.FailAt = ProbeFailure.BeforeFirstQuestion;
        var second = await CycleAsync(manifest);
        second.TrainState.Should().Be(TrainState.Failed);
        second.ReplayDecisionsOf.Should().Be(first.Id);

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var third = await CycleAsync(manifest);
        third.TrainState.Should().Be(TrainState.Failed);
        third.ReplayDecisionsOf.Should().Be(second.Id);

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var fourth = await CycleAsync(manifest);

        fourth.TrainState.Should().Be(TrainState.Completed, fourth.FailureReason);
        fourth.ReplayDecisionsOf.Should().Be(third.Id);
        (_decider.Requests.Count - before)
            .Should()
            .Be(2, $"only the first run asked; every retry replayed it. See {Adr}");
        DecisionProbe
            .TracksOf("chain")
            .Should()
            .Equal(
                ["Slow", "Large", "Slow", "Large", "Slow", "Large"],
                "every run that reached the questions took the first run's tracks"
            );
    }

    [Test]
    public async Task A_retry_whose_recorded_question_no_longer_matches_asks_it_afresh()
    {
        var manifest = await CreateManifestAsync("reworded");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // What a reworded question, or one offered different options, leaves behind: answers
        // recorded under a fingerprint the question no longer has.
        await WithData(data =>
            data.RecordedDecisions.Where(d => d.MetadataId == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(d => d.Fingerprint, "an-older-question"))
        );

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var asked = _decider.Requests.Count;
        var retry = await CycleAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .Be(failed.Id, "the run is linked; the answers no longer fit");
        (_decider.Requests.Count - asked)
            .Should()
            .Be(2, $"an answer given to another question is not replayed. See {Adr}");
        DecisionProbe
            .TracksOf("reworded")
            .Should()
            .Equal(["Slow", "Large", "Fast", "Small"], "the retry took the fresh answers");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_dead_letter_requeue_replays_the_failed_runs_decisions(bool batch)
    {
        var manifest = await CreateManifestAsync("dead-letter", maxRetries: 0);

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // No retries: the next cycle dead-letters the manifest instead of queueing one.
        await RunManifestManagerAsync();
        var deadLetter = await WithData(data =>
            data.DeadLetters.AsNoTracking()
                .SingleAsync(d =>
                    d.ManifestId == manifest.Id && d.Status == DeadLetterStatus.AwaitingIntervention
                )
        );

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var asked = _decider.Requests.Count;

        using (var scope = _provider.CreateScope())
        {
            var scheduler = scope.ServiceProvider.GetRequiredService<ITraxScheduler>();
            if (batch)
                (await scheduler.RequeueDeadLettersAsync([deadLetter.Id])).Count.Should().Be(1);
            else
                (await scheduler.RequeueDeadLetterAsync(deadLetter.Id)).Success.Should().BeTrue();
        }

        var requeued = await DispatchQueuedAsync(manifest);

        requeued.TrainState.Should().Be(TrainState.Completed, requeued.FailureReason);
        requeued
            .ReplayDecisionsOf.Should()
            .Be(failed.Id, $"a dead-letter requeue retries the failed run. See {Adr}");
        (_decider.Requests.Count - asked).Should().Be(0, "the requeue asked nothing");
        DecisionProbe.TracksOf("dead-letter").Should().Equal(["Slow", "Large", "Slow", "Large"]);
    }

    [Test]
    public async Task A_retry_given_a_different_input_asks_afresh()
    {
        var manifest = await CreateManifestAsync("before-edit");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // The manifest's properties are edited between the failure and its retry. The answers
        // were given about the old input.
        var edited = manifest.Properties!.Replace("before-edit", "after-edit");
        await WithData(data =>
            data.Manifests.Where(m => m.Id == manifest.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(m => m.Properties, edited))
        );

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var asked = _decider.Requests.Count;
        var retry = await CycleAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .BeNull($"answers given about one input are not replayed into another. See {Adr}");
        (await EntryOf(retry.Id)).ReplayDecisionsOf.Should().BeNull();
        (_decider.Requests.Count - asked).Should().Be(2, "the retry asked both questions afresh");
        DecisionProbe.TracksOf("after-edit").Should().Equal("Fast", "Small");
    }

    [Test]
    public async Task A_retry_of_a_run_that_did_not_record_its_decisions_asks_afresh()
    {
        var manifest = await CreateManifestAsync("unrecorded");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // What a run on a host that did not record decisions leaves: it may have acted on
        // answers nobody can know. Replaying it would fail the retry permanently.
        await WithData(data =>
            data.Metadatas.Where(m => m.Id == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(m => m.DecisionsRecorded, false))
        );

        await AssertRetryAsksAfreshAsync(manifest, "unrecorded");
    }

    [Test]
    public async Task A_retry_whose_replay_chain_names_a_run_that_no_longer_exists_asks_afresh()
    {
        var manifest = await CreateManifestAsync("pruned");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var first = await CycleAsync(manifest);
        DecisionProbe.FailAt = ProbeFailure.BeforeFirstQuestion;
        var second = await CycleAsync(manifest);
        second.ReplayDecisionsOf.Should().Be(first.Id);

        // The first run is deleted outside Trax, which leaves the second's link dangling.
        await WithData(async data =>
        {
            await data.RecordedDecisions.Where(d => d.MetadataId == first.Id).ExecuteDeleteAsync();
            await data.WorkQueues.Where(q => q.MetadataId == first.Id).ExecuteDeleteAsync();
            return await data.Metadatas.Where(m => m.Id == first.Id).ExecuteDeleteAsync();
        });

        await AssertRetryAsksAfreshAsync(manifest, "pruned");
    }

    [Test]
    public async Task A_retry_does_not_replay_a_run_of_another_manifest()
    {
        // Two manifests of the same train and input, declared by different applications.
        var other = await CreateManifestAsync("shared", owner: "other-application");
        var otherRun = await CycleAsync(other);
        otherRun.TrainState.Should().Be(TrainState.Completed, otherRun.FailureReason);

        var manifest = await CreateManifestAsync("shared", owner: "this-application");
        DecisionProbe.FailAt = ProbeFailure.BeforeFirstQuestion;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // A link this manifest's retries did not set, pointing at the other manifest's run.
        await WithData(data =>
            data.Metadatas.Where(m => m.Id == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(m => m.ReplayDecisionsOf, otherRun.Id))
        );

        await AssertRetryAsksAfreshAsync(manifest, "shared", expectedTracksBefore: 2);
    }

    [Test]
    public async Task A_retry_does_not_replay_a_failed_run_recorded_under_another_train()
    {
        var manifest = await CreateManifestAsync("renamed");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        await WithData(data =>
            data.Metadatas.Where(m => m.Id == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(m => m.Name, "Some.Other.ITrain"))
        );

        await AssertRetryAsksAfreshAsync(manifest, "renamed");
    }

    [Test]
    public async Task An_occurrence_after_a_success_replays_nothing()
    {
        var manifest = await CreateManifestAsync("ordinary");
        var completed = await CycleAsync(manifest);
        completed.TrainState.Should().Be(TrainState.Completed, completed.FailureReason);

        // Due again at once, as an ordinary occurrence rather than a retry.
        await WithData(data =>
            data.Manifests.Where(m => m.Id == manifest.Id)
                .ExecuteUpdateAsync(s =>
                    s.SetProperty(m => m.LastSuccessfulRun, DateTime.UtcNow.AddHours(-2))
                )
        );

        var next = await CycleAsync(manifest);
        next.ReplayDecisionsOf.Should().BeNull($"only a retry replays. See {Adr}");
    }

    [Test]
    public void No_public_scheduler_api_accepts_a_run_to_replay()
    {
        // The retry's source is read from the database by the scheduler. A public parameter or
        // settable property naming a replay source would let a caller point a run at any other
        // run's answers.
        const BindingFlags members =
            BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static;

        var offending = typeof(ITraxScheduler)
            .Assembly.GetExportedTypes()
            .SelectMany(type =>
                type.GetMethods(members)
                    .Concat<MethodBase>(type.GetConstructors(members))
                    .SelectMany(m => m.GetParameters())
                    .Where(p =>
                        p.Name?.Contains("Replay", StringComparison.OrdinalIgnoreCase) == true
                    )
                    .Select(p => $"{type.FullName}.{p.Member.Name}({p.Name})")
                    .Concat(
                        type.GetProperties(members)
                            .Where(p =>
                                p.CanWrite
                                && p.Name.Contains("Replay", StringComparison.OrdinalIgnoreCase)
                            )
                            .Select(p => $"{type.FullName}.{p.Name}")
                    )
            )
            .ToList();

        offending
            .Should()
            .BeEmpty($"only the scheduler chooses the run a retry replays. See {Adr}");
    }

    /// <summary>
    /// Runs the manifest's retry and asserts it was queued without a link, asked both questions,
    /// and completed rather than failing on a replay it could not honour.
    /// </summary>
    private async Task AssertRetryAsksAfreshAsync(
        Manifest manifest,
        string value,
        int? expectedTracksBefore = null
    )
    {
        var tracksBefore = expectedTracksBefore ?? DecisionProbe.TracksOf(value).Count;
        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var asked = _decider.Requests.Count;

        var retry = await CycleAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .BeNull($"a replay that could not be honoured is not linked. See {Adr}");
        (await EntryOf(retry.Id)).ReplayDecisionsOf.Should().BeNull();
        (_decider.Requests.Count - asked).Should().Be(2, "the retry asked both questions afresh");
        DecisionProbe.TracksOf(value).Skip(tracksBefore).Should().Equal("Fast", "Small");
    }

    private void Answer(ProbeLane lane, ProbeSize size) => _decider.Choose(lane).Choose(size);

    private async Task<Manifest> CreateManifestAsync(
        string value,
        int maxRetries = 3,
        string? owner = null
    )
    {
        using var scope = _provider.CreateScope();
        var data = scope.ServiceProvider.GetRequiredService<IDataContext>();

        var group = await TestSetup.CreateAndSaveManifestGroup(
            data,
            name: $"group-{Guid.NewGuid():N}"
        );
        var manifest = Manifest.Create(
            new CreateManifest
            {
                Name = typeof(IDecisionProbeTrain),
                IsEnabled = true,
                ScheduleType = ScheduleType.Interval,
                IntervalSeconds = 3600,
                MaxRetries = maxRetries,
                Properties = new DecisionProbeInput { Value = value },
            }
        );
        manifest.ManifestGroupId = group.Id;
        manifest.Owner = owner;
        await data.Track(manifest);
        await data.SaveChanges(CancellationToken.None);
        return manifest;
    }

    /// <summary>
    /// One polling cycle for the manifest: the ManifestManager queues its next run (a first run,
    /// an occurrence or a retry), the dispatcher runs it, and the run is returned.
    /// </summary>
    private async Task<Metadata> CycleAsync(Manifest manifest)
    {
        await RunManifestManagerAsync();
        return await DispatchQueuedAsync(manifest);
    }

    private async Task RunManifestManagerAsync()
    {
        using var scope = _provider.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IManifestManagerTrain>().Run(Unit.Default);
    }

    private async Task<Metadata> DispatchQueuedAsync(Manifest manifest)
    {
        var entryId = await WithData(data =>
            data.WorkQueues.AsNoTracking()
                .Where(q => q.ManifestId == manifest.Id && q.Status == WorkQueueStatus.Queued)
                .Select(q => q.Id)
                .SingleAsync()
        );

        using (var scope = _provider.CreateScope())
            await scope.ServiceProvider.GetRequiredService<IJobDispatcherTrain>().Run(Unit.Default);

        return await WithData(async data =>
        {
            var entry = await data.WorkQueues.AsNoTracking().SingleAsync(q => q.Id == entryId);
            entry.Status.Should().Be(WorkQueueStatus.Dispatched);
            return await data.Metadatas.AsNoTracking().SingleAsync(m => m.Id == entry.MetadataId);
        });
    }

    private Task<WorkQueue> EntryOf(long metadataId) =>
        WithData(data =>
            data.WorkQueues.AsNoTracking().SingleAsync(q => q.MetadataId == metadataId)
        );

    private Task<List<Effect.Models.RecordedDecision.RecordedDecision>> DecisionsOf(
        long metadataId
    ) =>
        WithData(data =>
            data.RecordedDecisions.AsNoTracking()
                .Where(d => d.MetadataId == metadataId)
                .OrderBy(d => d.Id)
                .ToListAsync()
        );

    private async Task<T> WithData<T>(Func<IDataContext, Task<T>> read)
    {
        using var scope = _provider.CreateScope();
        return await read(scope.ServiceProvider.GetRequiredService<IDataContext>());
    }
}
