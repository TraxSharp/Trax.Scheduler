using Trax.Effect.Models.SchedulerConfig;
using Trax.Scheduler.Configuration;

namespace Trax.Scheduler.Services.Operations;

/// <summary>
/// The settings one host can apply: the scheduler configuration, and the local worker options
/// when the host runs local workers.
/// </summary>
internal sealed record SchedulerSettingsTarget(
    SchedulerConfiguration Configuration,
    LocalWorkerOptions? Workers
);

/// <summary>
/// One persisted scheduler setting: how it is read from and written to the
/// <c>trax.scheduler_config</c> row, from and to the running host, from a patch, and the range it
/// must stay in. <see cref="SchedulerSettings.All"/> lists every one, and the operations service's
/// save, the startup load and the live refresh all work through that list, so adding a setting
/// means adding its column, its patch and snapshot fields, and one entry there.
/// </summary>
internal interface ISchedulerSetting
{
    /// <summary>The setting's name, as the patch input and the snapshot spell it.</summary>
    string Name { get; }

    /// <summary>Whether this host has the setting to apply (local workers, metadata cleanup).</summary>
    bool AppliesTo(SchedulerSettingsTarget target);

    /// <summary>
    /// The stored value, or false when the row does not set it and the host's configured value
    /// stands.
    /// </summary>
    bool TryReadRow(SchedulerConfig row, out object? value);

    /// <summary>Stores <paramref name="value"/> in the row.</summary>
    void WriteRow(SchedulerConfig row, object? value);

    /// <summary>The value the host runs with now.</summary>
    object? ReadLive(SchedulerSettingsTarget target);

    /// <summary>Makes the host run with <paramref name="value"/>.</summary>
    void WriteLive(SchedulerSettingsTarget target, object? value);

    /// <summary>What is wrong with <paramref name="value"/>, or null when the scheduler can run with it.</summary>
    string? Check(object? value);

    /// <summary>The value a patch sets, or false when the patch leaves the setting alone.</summary>
    bool TryReadPatch(UpdateSchedulerConfigInput input, out object? value);
}

/// <summary>A setting of type <typeparamref name="T"/>, built from delegates.</summary>
internal sealed class SchedulerSetting<T>(
    string name,
    Func<SchedulerConfig, (bool IsSet, T Value)> readRow,
    Action<SchedulerConfig, T> writeRow,
    Func<SchedulerSettingsTarget, T> readLive,
    Action<SchedulerSettingsTarget, T> writeLive,
    Func<UpdateSchedulerConfigInput, (bool IsSet, T Value)> readPatch,
    Func<T, string, string?>? check = null,
    Func<SchedulerSettingsTarget, bool>? appliesTo = null
) : ISchedulerSetting
{
    public string Name => name;

    public bool AppliesTo(SchedulerSettingsTarget target) => appliesTo?.Invoke(target) ?? true;

    public bool TryReadRow(SchedulerConfig row, out object? value)
    {
        var (isSet, v) = readRow(row);
        value = v;
        return isSet;
    }

    public void WriteRow(SchedulerConfig row, object? value) => writeRow(row, (T)value!);

    public object? ReadLive(SchedulerSettingsTarget target) => readLive(target);

    public void WriteLive(SchedulerSettingsTarget target, object? value) =>
        writeLive(target, (T)value!);

    public string? Check(object? value) => check?.Invoke((T)value!, name);

    public bool TryReadPatch(UpdateSchedulerConfigInput input, out object? value)
    {
        var (isSet, v) = readPatch(input);
        value = v;
        return isSet;
    }
}

/// <summary>The persisted scheduler settings, in the order the row lists them.</summary>
internal static class SchedulerSettings
{
    private static (bool, T) Set<T>(T value) => (true, value);

    private static (bool, T) Patch<T>(T? value)
        where T : struct => value is { } v ? (true, v) : (false, default);

    private static string? Check<T>(Func<T?, string, string?> limit, T value, string name)
        where T : struct => limit(value, name);

    /// <summary>Every persisted setting.</summary>
    public static readonly IReadOnlyList<ISchedulerSetting> All =
    [
        new SchedulerSetting<bool>(
            nameof(SchedulerConfig.ManifestManagerEnabled),
            r => Set(r.ManifestManagerEnabled),
            (r, v) => r.ManifestManagerEnabled = v,
            t => t.Configuration.ManifestManagerEnabled,
            (t, v) => t.Configuration.ManifestManagerEnabled = v,
            i => Patch(i.ManifestManagerEnabled)
        ),
        new SchedulerSetting<bool>(
            nameof(SchedulerConfig.JobDispatcherEnabled),
            r => Set(r.JobDispatcherEnabled),
            (r, v) => r.JobDispatcherEnabled = v,
            t => t.Configuration.JobDispatcherEnabled,
            (t, v) => t.Configuration.JobDispatcherEnabled = v,
            i => Patch(i.JobDispatcherEnabled)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.ManifestManagerPollingInterval),
            r => Set(r.ManifestManagerPollingInterval),
            (r, v) => r.ManifestManagerPollingInterval = v,
            t => t.Configuration.ManifestManagerPollingInterval,
            (t, v) => t.Configuration.ManifestManagerPollingInterval = v,
            i => Patch(i.ManifestManagerPollingInterval),
            (v, n) => Check(SchedulerConfigLimits.TimerInterval, v, n)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.JobDispatcherPollingInterval),
            r => Set(r.JobDispatcherPollingInterval),
            (r, v) => r.JobDispatcherPollingInterval = v,
            t => t.Configuration.JobDispatcherPollingInterval,
            (t, v) => t.Configuration.JobDispatcherPollingInterval = v,
            i => Patch(i.JobDispatcherPollingInterval),
            (v, n) => Check(SchedulerConfigLimits.TimerInterval, v, n)
        ),
        // Null is a value here: no limit. ClearMaxActiveJobs is how a patch sets it.
        new SchedulerSetting<int?>(
            nameof(SchedulerConfig.MaxActiveJobs),
            r => Set(r.MaxActiveJobs),
            (r, v) => r.MaxActiveJobs = v,
            t => t.Configuration.MaxActiveJobs,
            (t, v) => t.Configuration.MaxActiveJobs = v,
            i =>
                i.ClearMaxActiveJobs ? (true, null)
                : i.MaxActiveJobs is { } v ? (true, v)
                : (false, null),
            SchedulerConfigLimits.AtLeastOne
        ),
        new SchedulerSetting<int>(
            nameof(SchedulerConfig.DefaultMaxRetries),
            r => Set(r.DefaultMaxRetries),
            (r, v) => r.DefaultMaxRetries = v,
            t => t.Configuration.DefaultMaxRetries,
            (t, v) => t.Configuration.DefaultMaxRetries = v,
            i => Patch(i.DefaultMaxRetries),
            (v, n) => Check(SchedulerConfigLimits.NotNegative, v, n)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.DefaultRetryDelay),
            r => Set(r.DefaultRetryDelay),
            (r, v) => r.DefaultRetryDelay = v,
            t => t.Configuration.DefaultRetryDelay,
            (t, v) => t.Configuration.DefaultRetryDelay = v,
            i => Patch(i.DefaultRetryDelay),
            (v, n) => Check(SchedulerConfigLimits.NonNegativeDuration, v, n)
        ),
        new SchedulerSetting<double>(
            nameof(SchedulerConfig.RetryBackoffMultiplier),
            r => Set(r.RetryBackoffMultiplier),
            (r, v) => r.RetryBackoffMultiplier = v,
            t => t.Configuration.RetryBackoffMultiplier,
            (t, v) => t.Configuration.RetryBackoffMultiplier = v,
            i => Patch(i.RetryBackoffMultiplier),
            (v, n) => Check(SchedulerConfigLimits.BackoffMultiplier, v, n)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.MaxRetryDelay),
            r => Set(r.MaxRetryDelay),
            (r, v) => r.MaxRetryDelay = v,
            t => t.Configuration.MaxRetryDelay,
            (t, v) => t.Configuration.MaxRetryDelay = v,
            i => Patch(i.MaxRetryDelay),
            (v, n) => Check(SchedulerConfigLimits.NonNegativeDuration, v, n)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.DefaultJobTimeout),
            r => Set(r.DefaultJobTimeout),
            (r, v) => r.DefaultJobTimeout = v,
            t => t.Configuration.DefaultJobTimeout,
            (t, v) => t.Configuration.DefaultJobTimeout = v,
            i => Patch(i.DefaultJobTimeout),
            (v, n) => Check(SchedulerConfigLimits.PositiveDuration, v, n)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.StalePendingTimeout),
            r => Set(r.StalePendingTimeout),
            (r, v) => r.StalePendingTimeout = v,
            t => t.Configuration.StalePendingTimeout,
            (t, v) => t.Configuration.StalePendingTimeout = v,
            i => Patch(i.StalePendingTimeout),
            (v, n) => Check(SchedulerConfigLimits.PositiveDuration, v, n)
        ),
        new SchedulerSetting<bool>(
            nameof(SchedulerConfig.RecoverStuckJobsOnStartup),
            r => Set(r.RecoverStuckJobsOnStartup),
            (r, v) => r.RecoverStuckJobsOnStartup = v,
            t => t.Configuration.RecoverStuckJobsOnStartup,
            (t, v) => t.Configuration.RecoverStuckJobsOnStartup = v,
            i => Patch(i.RecoverStuckJobsOnStartup)
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.DeadLetterRetentionPeriod),
            r => Set(r.DeadLetterRetentionPeriod),
            (r, v) => r.DeadLetterRetentionPeriod = v,
            t => t.Configuration.DeadLetterRetentionPeriod,
            (t, v) => t.Configuration.DeadLetterRetentionPeriod = v,
            i => Patch(i.DeadLetterRetentionPeriod),
            (v, n) => Check(SchedulerConfigLimits.NonNegativeDuration, v, n)
        ),
        new SchedulerSetting<bool>(
            nameof(SchedulerConfig.AutoPurgeDeadLetters),
            r => Set(r.AutoPurgeDeadLetters),
            (r, v) => r.AutoPurgeDeadLetters = v,
            t => t.Configuration.AutoPurgeDeadLetters,
            (t, v) => t.Configuration.AutoPurgeDeadLetters = v,
            i => Patch(i.AutoPurgeDeadLetters)
        ),
        // A null column leaves the host's configured count; ClearLocalWorkerCount resets the
        // count to the processor count.
        new SchedulerSetting<int>(
            nameof(SchedulerConfig.LocalWorkerCount),
            r => r.LocalWorkerCount is { } v ? (true, v) : (false, 0),
            (r, v) => r.LocalWorkerCount = v,
            t => t.Workers!.WorkerCount,
            (t, v) => t.Workers!.WorkerCount = v,
            i =>
                i.ClearLocalWorkerCount ? (true, Environment.ProcessorCount)
                : i.LocalWorkerCount is { } v ? (true, v)
                : (false, 0),
            (v, n) => Check(SchedulerConfigLimits.WorkerCount, v, n),
            t => t.Workers is not null
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.MetadataCleanupInterval),
            r => r.MetadataCleanupInterval is { } v ? (true, v) : (false, default),
            (r, v) => r.MetadataCleanupInterval = v,
            t => t.Configuration.MetadataCleanup!.CleanupInterval,
            (t, v) => t.Configuration.MetadataCleanup!.CleanupInterval = v,
            i => Patch(i.MetadataCleanupInterval),
            (v, n) => Check(SchedulerConfigLimits.TimerInterval, v, n),
            t => t.Configuration.MetadataCleanup is not null
        ),
        new SchedulerSetting<TimeSpan>(
            nameof(SchedulerConfig.MetadataCleanupRetention),
            r => r.MetadataCleanupRetention is { } v ? (true, v) : (false, default),
            (r, v) => r.MetadataCleanupRetention = v,
            t => t.Configuration.MetadataCleanup!.RetentionPeriod,
            (t, v) => t.Configuration.MetadataCleanup!.RetentionPeriod = v,
            i => Patch(i.MetadataCleanupRetention),
            (v, n) => Check(SchedulerConfigLimits.PositiveDuration, v, n),
            t => t.Configuration.MetadataCleanup is not null
        ),
    ];

    /// <summary>
    /// What the host runs with now, for each setting it has: the baseline the live refresh
    /// returns a setting to when the stored row stops setting it.
    /// </summary>
    public static IReadOnlyDictionary<string, object?> Capture(SchedulerSettingsTarget target) =>
        All.Where(s => s.AppliesTo(target)).ToDictionary(s => s.Name, s => s.ReadLive(target));
}
