using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Trax.Core.Decisions;
using Trax.Core.Monad;
using Trax.Mediator.Services.TrainDiscovery;

namespace Trax.Scheduler.Services.DecisionRecording;

/// <summary>
/// Warns at startup when a host that runs trains registers trains that ask a decider, but does not
/// record decisions (<c>AddDecisionRecording</c>).
/// </summary>
/// <remarks>
/// Such a host runs those trains, and asks their deciders, without trouble. What it cannot do is
/// run a requeue that replays a recorded run's decisions: it has no recorded answers to replay, so
/// the run fails, classified permanent, rather than asking afresh and perhaps taking another track
/// (central <c>docs/0041</c>). A requeue links a run only when the run has decisions to replay, so
/// this happens where the run was recorded by one host and the requeue is run by another that does
/// not record: one worker of a fleet left without the call.
///
/// <para>A warning rather than a refusal to start. The unsafe outcome, a replay that asks afresh,
/// is already refused by the run itself, so nothing here fails open. Recording is a choice a host
/// makes, and a host that runs decision trains without it, and never runs their requeues, is
/// correctly configured; refusing to start it would break it to report a failure it may never
/// have. The warning names the trains and the fix, so the host that does run requeues is
/// found at startup rather than by the first replay that fails.</para>
///
/// <para>A train's chain is read the way the mediator's chain verification reads it. A train that
/// cannot be built or read outside a request is left out silently: the mediator already reports
/// it.</para>
/// </remarks>
internal sealed class DecisionRecordingStartupCheck(
    IServiceScopeFactory scopeFactory,
    IServiceProviderIsService isService,
    // Optional: AddTraxJobRunner registers this check, and a host built without AddMediator has
    // no trains to check, which the mediator's own validation reports.
    ITrainDiscoveryService? discoveryService = null,
    ILogger<DecisionRecordingStartupCheck>? logger = null
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (
            logger is null
            || discoveryService is null
            || isService.IsService(typeof(IDecisionReplay))
        )
            return;

        var deciding = await TrainsThatDecideAsync(cancellationToken);

        if (deciding.Count == 0)
            return;

        logger.LogWarning(
            "{Trains} ask a decider, but this host does not record decisions. It runs them, but a "
                + "requeue of a run whose decisions were recorded fails here, permanently, rather "
                + "than asking afresh. Call AddDecisionRecording() on the effects builder of every "
                + "host that runs these trains.",
            string.Join(", ", deciding)
        );
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>The names of the registered trains whose chains ask a decider, in order.</summary>
    internal async Task<IReadOnlyList<string>> TrainsThatDecideAsync(
        CancellationToken cancellationToken
    )
    {
        // Async, as the mediator's check does: a train's scoped dependency may implement only
        // IAsyncDisposable.
        await using var scope = scopeFactory.CreateAsyncScope();
        var deciding = new List<string>();

        foreach (var registration in discoveryService?.DiscoverTrains() ?? [])
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (DeclaredChain(scope.ServiceProvider, registration) is { } chain && Decides(chain))
                deciding.Add(registration.ServiceType.FullName ?? registration.ServiceTypeName);
        }

        deciding.Sort(StringComparer.Ordinal);
        return deciding;
    }

    private static ChainRecorder? DeclaredChain(
        IServiceProvider services,
        TrainRegistration registration
    )
    {
        try
        {
            var train = services.GetRequiredService(registration.ServiceType);

            return train
                    .GetType()
                    .GetMethod(nameof(Core.Train.Train<,>.DeclaredChain), Type.EmptyTypes)
                    ?.Invoke(train, null) as ChainRecorder;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>Whether the chain, or any track it routes to, asks a decider.</summary>
    private static bool Decides(ChainRecorder chain)
    {
        for (var i = 0; i < chain.Steps.Count; i++)
        {
            if (chain.Steps[i].Kind == ChainStepKind.Decide)
                return true;

            if (chain.TracksAt(i).Any(track => Decides(track.Steps)))
                return true;
        }

        return false;
    }
}
