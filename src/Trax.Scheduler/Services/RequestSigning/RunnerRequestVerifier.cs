using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Trax.Scheduler.Configuration;

namespace Trax.Scheduler.Services.RequestSigning;

/// <summary>
/// The outcome of <see cref="RunnerRequestVerifier.Verify"/>.
/// </summary>
public enum RunnerRequestVerdict
{
    /// <summary>The request may run.</summary>
    Accepted,

    /// <summary>A signing key is configured and the request carries no signature.</summary>
    Missing,

    /// <summary>The signature is malformed or does not match the body.</summary>
    Invalid,

    /// <summary>The signature's timestamp is further from this clock than the allowed skew.</summary>
    Stale,

    /// <summary>The signature's nonce was already accepted inside the skew window.</summary>
    Replayed,
}

/// <summary>
/// Checks a runner request against the posture in <see cref="TraxJobRunnerOptions"/>. Registered
/// as a singleton by <c>AddTraxJobRunner</c>, because the nonces it remembers must outlive a
/// request.
/// </summary>
/// <remarks>
/// Public because Trax.Runner.Lambda, which ships separately, verifies with it. The nonce memory is
/// per process: a runner scaled to several instances refuses a replay only on the instance that
/// saw the original (see scheduler/0006).
/// </remarks>
public sealed class RunnerRequestVerifier
{
    private readonly TraxJobRunnerOptions _options;
    private readonly ILogger<RunnerRequestVerifier> _logger;
    private readonly TimeProvider _time;
    private readonly ConcurrentDictionary<string, long> _seenNonces = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _warnedEntryPoints = new(
        StringComparer.Ordinal
    );
    private long _acceptedSinceSweep;

    /// <summary>Creates a verifier over <paramref name="options"/>.</summary>
    public RunnerRequestVerifier(
        TraxJobRunnerOptions options,
        ILogger<RunnerRequestVerifier> logger,
        TimeProvider? time = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        options.Validate();
        _options = options;
        _logger = logger;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>
    /// Refuses to start an entry point that has no posture of its own: one that cannot apply an
    /// ASP.NET policy (an SQS handler, a Lambda function) needs a signing key or an explicit
    /// <see cref="TraxJobRunnerOptions.AllowUnsignedRequests"/>. Logs a warning, once per entry
    /// point, when it will accept unsigned requests.
    /// </summary>
    /// <param name="entryPoint">A name for the entry point, used in the error and the warning.</param>
    /// <exception cref="InvalidOperationException">No posture is configured.</exception>
    public void EnsurePosture(string entryPoint) => EnsurePosture(entryPoint, policyApplies: false);

    internal void EnsurePosture(string entryPoint, bool policyApplies)
    {
        if (_options.SigningKey is not null)
            return;

        if (policyApplies && _options.AuthorizationPolicy is not null)
            return;

        if (!_options.UnsignedRequestsAllowed)
            throw new InvalidOperationException(
                $"The Trax runner entry point '{entryPoint}' has no authorization posture. "
                    + "Pass AddTraxJobRunner(runner => ...) a SigningKey shared with the scheduler"
                    + (
                        policyApplies
                            ? ", an AuthorizationPolicy that admits only the scheduler,"
                            : ""
                    )
                    + " or call AllowUnsignedRequests() to accept requests from anyone who can reach it."
            );

        if (_warnedEntryPoints.TryAdd(entryPoint, true))
            _logger.LogWarning(
                "Trax runner entry point {EntryPoint} accepts unsigned requests (AllowUnsignedRequests). "
                    + "Anyone who can reach it can run any registered train.",
                entryPoint
            );
    }

    /// <summary>
    /// Verifies <paramref name="signature"/> over <paramref name="body"/>. With no signing key
    /// configured, every request is <see cref="RunnerRequestVerdict.Accepted"/>: the entry point's
    /// posture was checked when it started.
    /// </summary>
    /// <param name="purpose">What the request is for.</param>
    /// <param name="body">The exact bytes received.</param>
    /// <param name="signature">The signature received, or null.</param>
    /// <param name="requireFresh">
    /// Whether to refuse a stale timestamp or a repeated nonce. False only for transports that
    /// redeliver the same message by design (SQS, an asynchronous Lambda invocation), where the
    /// job's Pending metadata row is what stops a second run.
    /// </param>
    public RunnerRequestVerdict Verify(
        RunnerRequestPurpose purpose,
        ReadOnlySpan<byte> body,
        string? signature,
        bool requireFresh
    )
    {
        var key = _options.SigningKey;
        if (key is null)
            return RunnerRequestVerdict.Accepted;

        if (string.IsNullOrEmpty(signature))
            return RunnerRequestVerdict.Missing;

        if (
            !RunnerRequestSignature.TryVerifyMac(
                key,
                purpose,
                body,
                signature,
                out var timestamp,
                out var nonce
            )
        )
            return RunnerRequestVerdict.Invalid;

        if (!requireFresh)
            return RunnerRequestVerdict.Accepted;

        var now = _time.GetUtcNow().ToUnixTimeSeconds();
        var skew = (long)_options.MaxClockSkew.TotalSeconds;
        if (Math.Abs(now - timestamp) > skew)
            return RunnerRequestVerdict.Stale;

        // Remembered until the timestamp itself goes stale; after that freshness refuses it.
        if (!_seenNonces.TryAdd(nonce, timestamp + skew))
            return RunnerRequestVerdict.Replayed;

        if (Interlocked.Increment(ref _acceptedSinceSweep) % 256 == 0)
            SweepExpired(now);

        return RunnerRequestVerdict.Accepted;
    }

    private void SweepExpired(long now)
    {
        foreach (var (nonce, expires) in _seenNonces)
            if (expires < now)
                _seenNonces.TryRemove(nonce, out _);
    }
}
