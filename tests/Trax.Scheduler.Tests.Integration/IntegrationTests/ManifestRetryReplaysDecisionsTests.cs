using System.Reflection;
using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
using Trax.Scheduler.Services.Operations;
using Trax.Scheduler.Services.TraxScheduler;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Trax.Scheduler.Trains.JobDispatcher;
using Trax.Scheduler.Trains.JobRunner;
using Trax.Scheduler.Trains.ManifestManager;
using Trax.Scheduler.Trains.ManifestManager.Utilities;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// A manifest's retry, made by the ManifestManager or by a dead-letter requeue, replays the
/// decisions its failed run recorded instead of asking the decider again, and only when that is
/// sound: the source is the manifest's failed run, read from the database, a run of its train that
/// recorded its decisions, asked them itself, has not had them replayed into a failure already,
/// and was queued by the manifest with the input the retry is given. Anything else, the lookup
/// failing included, asks afresh rather than failing the retry. An operator can ask afresh
/// explicitly, and a manifest can opt out.
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

    // A host built for one test, in place of the shared one. Disposed by that test.
    private IServiceProvider? _override;

    private IServiceProvider Provider => _override ?? _provider;
    private ScriptedDecider _decider = null!;

    [OneTimeSetUp]
    public void BuildProvider()
    {
        _decider = new ScriptedDecider();
        _provider = BuildProvider(_decider);
    }

    private static ServiceProvider BuildProvider(
        IDecider decider,
        Action<IServiceCollection>? configure = null
    )
    {
        var services = new ServiceCollection()
            .AddLogging(x => x.SetMinimumLevel(LogLevel.Warning))
            .AddSingleton(decider)
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
            );
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    [OneTimeTearDown]
    public async Task DisposeProvider() => await _provider.DisposeAsync();

    [SetUp]
    public async Task Clean()
    {
        DecisionProbe.Reset();
        Answer(ProbeLane.Slow, ProbeSize.Large);

        using var scope = Provider.CreateScope();
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
    public async Task A_retry_replays_once_and_the_retry_after_a_failed_replay_asks_afresh()
    {
        var manifest = await CreateManifestAsync("once", maxRetries: 3);
        var before = _decider.Requests.Count;

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var first = await CycleAsync(manifest);
        first.TrainState.Should().Be(TrainState.Failed);

        // The first retry replays the first run's answers, and fails with them.
        var second = await CycleAsync(manifest);
        second.TrainState.Should().Be(TrainState.Failed);
        second.ReplayDecisionsOf.Should().Be(first.Id);
        (_decider.Requests.Count - before).Should().Be(2, "the first retry asked nothing");

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var third = await CycleAsync(manifest);

        third.TrainState.Should().Be(TrainState.Completed, third.FailureReason);
        third
            .ReplayDecisionsOf.Should()
            .BeNull($"answers already replayed into a failure are not replayed again. See {Adr}");
        (_decider.Requests.Count - before)
            .Should()
            .Be(4, "the second retry asked both questions afresh");
        DecisionProbe
            .TracksOf("once")
            .Should()
            .Equal(["Slow", "Large", "Slow", "Large", "Fast", "Small"]);
    }

    [Test]
    public async Task A_dead_letter_requeue_after_a_failed_replay_asks_afresh()
    {
        // One retry: the first run fails, its replay fails, and the manifest is dead-lettered.
        var manifest = await CreateManifestAsync("trapped", maxRetries: 1);

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var first = await CycleAsync(manifest);
        var replay = await CycleAsync(manifest);
        replay.ReplayDecisionsOf.Should().Be(first.Id);
        replay.TrainState.Should().Be(TrainState.Failed);

        await RunManifestManagerAsync();
        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var requeued = await RequeueDeadLetterAndRunAsync(manifest, askAfresh: false);

        requeued.TrainState.Should().Be(TrainState.Completed, requeued.FailureReason);
        requeued
            .ReplayDecisionsOf.Should()
            .BeNull($"the requeue does not repeat the answers that already failed. See {Adr}");
        DecisionProbe.TracksOf("trapped").TakeLast(2).Should().Equal("Fast", "Small");
    }

    [Test]
    public async Task A_manifest_retry_asks_afresh_once_a_requeue_replayed_its_failed_run_and_failed()
    {
        var manifest = await CreateManifestAsync("requeued", maxRetries: 3);

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // An operator requeues the failed run itself. Its run replays the failed run's answers
        // and fails too. It belongs to no manifest, so the manifest never retries it directly.
        long requeueEntry;
        using (var scope = Provider.CreateScope())
        {
            var result = await scope
                .ServiceProvider.GetRequiredService<IOperationsService>()
                .RequeueExecutionAsync(failed.Id, CancellationToken.None);
            result.Success.Should().BeTrue(result.Message);
            requeueEntry = result.Id!.Value;
        }
        var requeued = await DispatchEntryAsync(requeueEntry);
        requeued.TrainState.Should().Be(TrainState.Failed);
        requeued.ReplayDecisionsOf.Should().Be(failed.Id);
        requeued
            .ManifestId.Should()
            .BeNull("a requeue through the operations service is no manifest's run");

        await AssertRetryAsksAfreshAsync(manifest, "requeued");
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

    [TestCase(DeadLetterRequeue.Single, false)]
    [TestCase(DeadLetterRequeue.Batch, false)]
    [TestCase(DeadLetterRequeue.All, false)]
    [TestCase(DeadLetterRequeue.Single, true)]
    [TestCase(DeadLetterRequeue.Batch, true)]
    [TestCase(DeadLetterRequeue.All, true)]
    public async Task A_dead_letter_requeue_replays_the_failed_runs_decisions_unless_asked_afresh(
        DeadLetterRequeue how,
        bool askAfresh
    )
    {
        var manifest = await CreateManifestAsync("dead-letter", maxRetries: 0);

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // No retries: the next cycle dead-letters the manifest instead of queueing one.
        await RunManifestManagerAsync();

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var asked = _decider.Requests.Count;

        var requeued = await RequeueDeadLetterAndRunAsync(manifest, askAfresh, how);

        requeued.TrainState.Should().Be(TrainState.Completed, requeued.FailureReason);
        if (askAfresh)
        {
            requeued
                .ReplayDecisionsOf.Should()
                .BeNull($"the operator asked the requeue to ask afresh. See {Adr}");
            (_decider.Requests.Count - asked).Should().Be(2);
            DecisionProbe
                .TracksOf("dead-letter")
                .Should()
                .Equal(["Slow", "Large", "Fast", "Small"]);
        }
        else
        {
            requeued
                .ReplayDecisionsOf.Should()
                .Be(failed.Id, $"a dead-letter requeue retries the failed run. See {Adr}");
            (_decider.Requests.Count - asked).Should().Be(0, "the requeue asked nothing");
            DecisionProbe
                .TracksOf("dead-letter")
                .Should()
                .Equal(["Slow", "Large", "Slow", "Large"]);
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_trigger_that_releases_a_queued_retry_keeps_its_replay_unless_asked_afresh(
        bool askAfresh
    )
    {
        var manifest = await CreateManifestAsync("triggered");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // The retry is queued with its link; an operator triggers the manifest before it runs.
        await RunManifestManagerAsync();
        using (var scope = Provider.CreateScope())
        {
            var scheduler = scope.ServiceProvider.GetRequiredService<ITraxScheduler>();
            if (askAfresh)
                await scheduler.TriggerAsync(manifest.ExternalId, askAfresh: true);
            else
                await scheduler.TriggerAsync(manifest.ExternalId);
        }

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var retry = await DispatchQueuedAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .Be(
                askAfresh ? null : failed.Id,
                $"a trigger asked to ask afresh clears the link. See {Adr}"
            );
        DecisionProbe
            .TracksOf("triggered")
            .TakeLast(2)
            .Should()
            .Equal(askAfresh ? ["Fast", "Small"] : ["Slow", "Large"]);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_manifest_that_stops_replaying_while_its_retry_waits_asks_afresh(
        bool throughOperations
    )
    {
        var manifest = await CreateManifestAsync("flipped");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // The retry is queued, linked, and waits; meanwhile the manifest stops replaying.
        await RunManifestManagerAsync();
        (await QueuedEntryOf(manifest)).ReplayDecisionsOf.Should().Be(failed.Id);

        if (throughOperations)
        {
            using var scope = Provider.CreateScope();
            var result = await scope
                .ServiceProvider.GetRequiredService<IOperationsService>()
                .SetManifestsReplayDecisionsOnRetryAsync(
                    [manifest.Id],
                    false,
                    CancellationToken.None
                );
            result.Success.Should().BeTrue();
            result.Count.Should().Be(1);
            (await QueuedEntryOf(manifest))
                .ReplayDecisionsOf.Should()
                .BeNull("turning the flag off clears the queued retry's link at once");
        }
        else
            // A write that leaves the queued link in place: the dispatcher must still honour it.
            await WithData(data =>
                data.Manifests.Where(m => m.Id == manifest.Id)
                    .ExecuteUpdateAsync(s => s.SetProperty(m => m.ReplayDecisionsOnRetry, false))
            );

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);
        var asked = _decider.Requests.Count;
        var retry = await DispatchQueuedAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .BeNull($"the manifest no longer replays decisions on retry. See {Adr}");
        (await EntryOf(retry.Id)).ReplayDecisionsOf.Should().BeNull();
        (_decider.Requests.Count - asked).Should().Be(2);
    }

    [Test]
    public async Task A_retry_whose_source_lookup_fails_is_queued_to_ask_afresh()
    {
        var manifest = await CreateManifestAsync("lookup-fails");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        // A host whose replay lookup cannot reach its store.
        await using var faulty = BuildProvider(
            _decider,
            services =>
                services.AddScoped(_ => new RetryDecisionReplay(
                    new UnreachableStore(),
                    NullLogger.Instance
                ))
        );
        _override = faulty;
        try
        {
            await AssertRetryAsksAfreshAsync(manifest, "lookup-fails");
        }
        finally
        {
            _override = null;
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_manifest_that_opted_out_asks_afresh_on_retry(bool deadLetter)
    {
        var manifest = await CreateManifestAsync(
            "opted-out",
            maxRetries: deadLetter ? 0 : 3,
            replayDecisionsOnRetry: false
        );
        var before = _decider.Requests.Count;

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);
        failed.TrainState.Should().Be(TrainState.Failed);

        DecisionProbe.FailAt = ProbeFailure.None;
        Answer(ProbeLane.Fast, ProbeSize.Small);

        Metadata retry;
        if (deadLetter)
        {
            await RunManifestManagerAsync();
            retry = await RequeueDeadLetterAndRunAsync(manifest, askAfresh: false);
        }
        else
            retry = await CycleAsync(manifest);

        retry.TrainState.Should().Be(TrainState.Completed, retry.FailureReason);
        retry
            .ReplayDecisionsOf.Should()
            .BeNull($"the manifest set ReplayDecisionsOnRetry(false). See {Adr}");
        (_decider.Requests.Count - before)
            .Should()
            .Be(4, "the failed run and its retry each asked both questions");
        DecisionProbe
            .TracksOf("opted-out")
            .Should()
            .Equal(["Slow", "Large", "Fast", "Small"], "the retry took the fresh answers");
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
    public async Task A_retry_of_a_run_whose_queue_entry_is_gone_asks_afresh()
    {
        var manifest = await CreateManifestAsync("entry-gone");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);

        // With no entry there is no input to compare the retry's with.
        await WithData(data =>
            data.WorkQueues.Where(q => q.MetadataId == failed.Id).ExecuteDeleteAsync()
        );

        await AssertRetryAsksAfreshAsync(manifest, "entry-gone");
    }

    [Test]
    public async Task A_retry_does_not_compare_against_a_queue_entry_of_another_manifest()
    {
        var other = await CreateManifestAsync("elsewhere");
        var manifest = await CreateManifestAsync("own-entry");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);

        // The failed run's entry claims another manifest: it was not queued by this one.
        await WithData(data =>
            data.WorkQueues.Where(q => q.MetadataId == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(q => q.ManifestId, (long?)other.Id))
        );

        await AssertRetryAsksAfreshAsync(manifest, "own-entry");
    }

    [Test]
    public async Task A_retry_of_a_run_queued_under_a_subject_key_asks_afresh()
    {
        var manifest = await CreateManifestAsync("subject");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);

        await WithData(data =>
            data.WorkQueues.Where(q => q.MetadataId == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(q => q.SubjectKey, "tenant-a"))
        );

        await AssertRetryAsksAfreshAsync(manifest, "subject");
    }

    [Test]
    public async Task A_retry_whose_input_type_differs_from_the_failed_runs_asks_afresh()
    {
        var manifest = await CreateManifestAsync("retyped");

        DecisionProbe.FailAt = ProbeFailure.AfterQuestions;
        var failed = await CycleAsync(manifest);

        // Same JSON, read as another type: not the input the answers were given about.
        await WithData(data =>
            data.WorkQueues.Where(q => q.MetadataId == failed.Id)
                .ExecuteUpdateAsync(s => s.SetProperty(q => q.InputTypeName, "Some.Other.Input"))
        );

        await AssertRetryAsksAfreshAsync(manifest, "retyped");
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
        // settable property that names a replay and could carry a run (an id or an external id)
        // would let a caller point a run at any other run's answers. The bool opt-out names no
        // run, so it is not one.
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
                        && CouldNameARun(p.ParameterType)
                    )
                    .Select(p => $"{type.FullName}.{p.Member.Name}({p.Name})")
                    .Concat(
                        type.GetProperties(members)
                            .Where(p =>
                                p.CanWrite
                                && p.Name.Contains("Replay", StringComparison.OrdinalIgnoreCase)
                                && CouldNameARun(p.PropertyType)
                            )
                            .Select(p => $"{type.FullName}.{p.Name}")
                    )
            )
            .ToList();

        offending
            .Should()
            .BeEmpty($"only the scheduler chooses the run a retry replays. See {Adr}");

        static bool CouldNameARun(Type type) =>
            (Nullable.GetUnderlyingType(type) ?? type) is var t && t != typeof(bool) && !t.IsEnum;
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

    public enum DeadLetterRequeue
    {
        Single,
        Batch,
        All,
    }

    /// <summary>Requeues the manifest's awaiting dead letter one of the three ways, and runs it.</summary>
    private async Task<Metadata> RequeueDeadLetterAndRunAsync(
        Manifest manifest,
        bool askAfresh,
        DeadLetterRequeue how = DeadLetterRequeue.Single
    )
    {
        var deadLetterId = await WithData(data =>
            data.DeadLetters.AsNoTracking()
                .Where(d =>
                    d.ManifestId == manifest.Id && d.Status == DeadLetterStatus.AwaitingIntervention
                )
                .Select(d => d.Id)
                .SingleAsync()
        );

        using (var scope = Provider.CreateScope())
        {
            var scheduler = scope.ServiceProvider.GetRequiredService<ITraxScheduler>();
            switch (how)
            {
                case DeadLetterRequeue.Single:
                    (await scheduler.RequeueDeadLetterAsync(deadLetterId, askAfresh))
                        .Success.Should()
                        .BeTrue();
                    break;
                case DeadLetterRequeue.Batch:
                    (await scheduler.RequeueDeadLettersAsync([deadLetterId], askAfresh))
                        .Count.Should()
                        .Be(1);
                    break;
                default:
                    (await scheduler.RequeueAllDeadLettersAsync(askAfresh)).Count.Should().Be(1);
                    break;
            }
        }

        return await DispatchQueuedAsync(manifest);
    }

    private Task<WorkQueue> QueuedEntryOf(Manifest manifest) =>
        WithData(data =>
            data.WorkQueues.AsNoTracking()
                .SingleAsync(q => q.ManifestId == manifest.Id && q.Status == WorkQueueStatus.Queued)
        );

    /// <summary>A store that cannot be reached, for the replay lookup only.</summary>
    private sealed class UnreachableStore : IDataContextProviderFactory
    {
        public Task<IDataContext> CreateDbContextAsync(CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The store cannot be reached.");

        public Trax.Effect.Services.EffectProvider.IEffectProvider Create() =>
            throw new InvalidOperationException("The store cannot be reached.");
    }

    private async Task<Manifest> CreateManifestAsync(
        string value,
        int maxRetries = 3,
        string? owner = null,
        bool replayDecisionsOnRetry = true
    )
    {
        using var scope = Provider.CreateScope();
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
                ReplayDecisionsOnRetry = replayDecisionsOnRetry,
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
        using var scope = Provider.CreateScope();
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

        return await DispatchEntryAsync(entryId);
    }

    private async Task<Metadata> DispatchEntryAsync(long entryId)
    {
        using (var scope = Provider.CreateScope())
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
        using var scope = Provider.CreateScope();
        return await read(scope.ServiceProvider.GetRequiredService<IDataContext>());
    }
}
