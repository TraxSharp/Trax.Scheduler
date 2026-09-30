using Trax.Scheduler.Services.Operations;

namespace Trax.Scheduler.Configuration;

public partial class SchedulerConfigurationBuilder
{
    /// <summary>
    /// Refuses a duration or count set in code that the scheduler cannot run with, against the
    /// same <see cref="SchedulerConfigLimits"/> the operations service holds a runtime change to.
    /// Each problem names the builder method (or options property) that set the value.
    /// </summary>
    /// <exception cref="InvalidOperationException">A value is outside its range.</exception>
    private void ValidateLimits()
    {
        var problems = new[]
        {
            SchedulerConfigLimits.TimerInterval(
                _configuration.ManifestManagerPollingInterval,
                _manifestManagerIntervalSetBy
            ),
            SchedulerConfigLimits.TimerInterval(
                _configuration.JobDispatcherPollingInterval,
                _jobDispatcherIntervalSetBy
            ),
            SchedulerConfigLimits.NotNegative(
                _configuration.DefaultMaxRetries,
                nameof(DefaultMaxRetries)
            ),
        }
            .OfType<string>()
            .ToList();

        if (problems.Count > 0)
            throw new InvalidOperationException(
                "The scheduler configuration has values it cannot run with. "
                    + string.Join(" ", problems)
            );
    }
}
