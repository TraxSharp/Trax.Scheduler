using System.Diagnostics;

namespace Trax.Scheduler.Utilities;

/// <summary>
/// The wait between two polling cycles, read from the live configuration. A background service
/// built its <see cref="PeriodicTimer"/> once, at start, so an interval changed at runtime (through
/// the operations service, or a saved setting another host wrote) did nothing until a restart.
/// This re-reads the interval at least once a second while it waits, so a shorter interval ends
/// the current wait early and a longer one extends it.
/// </summary>
internal static class PollingDelay
{
    private static readonly TimeSpan Slice = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Waits until <paramref name="interval"/>, re-read as it goes, has elapsed since the call.
    /// Returns false when <paramref name="stoppingToken"/> is cancelled, so a loop can read
    /// <c>while (await PollingDelay.WaitAsync(...))</c> the way it read the timer.
    /// </summary>
    public static async Task<bool> WaitAsync(
        Func<TimeSpan> interval,
        CancellationToken stoppingToken
    )
    {
        var elapsed = Stopwatch.StartNew();
        try
        {
            while (true)
            {
                var remaining = interval() - elapsed.Elapsed;
                if (remaining <= TimeSpan.Zero)
                    return true;

                await Task.Delay(remaining < Slice ? remaining : Slice, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
    }
}
