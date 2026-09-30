namespace Trax.Scheduler.Configuration;

public partial class SchedulerConfigurationBuilder
{
    // What each schedule or batch declares about its group and its prune, checked at build time.
    private readonly List<GroupDeclaration> _groupDeclarations = [];

    private sealed record GroupDeclaration(
        string Group,
        string DeclaredBy,
        int? Priority,
        bool MaxActiveJobsStated,
        int? MaxActiveJobs,
        bool? IsEnabled
    );

    /// <summary>
    /// Records the group settings a single manifest's options state, for
    /// <see cref="ValidateSeedDeclarations"/>.
    /// </summary>
    private void DeclareGroup(string externalId, ScheduleOptions resolved) =>
        AddGroupDeclaration(
            resolved._groupId ?? externalId,
            $"'{externalId}'",
            resolved,
            ownsGroup: resolved._groupId is null
        );

    /// <summary>
    /// Records a batch's group settings, for <see cref="ValidateSeedDeclarations"/>.
    /// The group is the one the scheduler seeds the batch into: its group name, else its prune
    /// prefix, else its first external ID.
    /// </summary>
    private void DeclareBatchGroup(string firstId, ScheduleOptions resolved)
    {
        var declaredBy = $"the batch starting '{firstId}'";
        var group = resolved._groupId ?? resolved._prunePrefix ?? firstId;
        var ownsGroup = resolved._groupId is null;

        AddGroupDeclaration(group, declaredBy, resolved, ownsGroup);
    }

    private void AddGroupDeclaration(
        string group,
        string declaredBy,
        ScheduleOptions resolved,
        bool ownsGroup
    )
    {
        var options = resolved._groupOptions;
        _groupDeclarations.Add(
            new GroupDeclaration(
                group,
                declaredBy,
                options?._priority ?? (ownsGroup ? resolved._priority : null),
                options?._maxActiveJobsStated ?? false,
                options?._maxActiveJobs,
                options?._isEnabled
            )
        );
    }

    /// <summary>
    /// Fails the build when two members of one manifest group state different values for the same
    /// group setting.
    /// </summary>
    /// <remarks>
    /// A group setting is written whenever a member's options state it, so two members stating
    /// different values would overwrite each other at every start, and which one held would depend
    /// on seeding order.
    /// </remarks>
    private void ValidateSeedDeclarations()
    {
        foreach (var group in _groupDeclarations.GroupBy(d => d.Group))
        {
            var members = group.ToList();
            RequireAgreement(
                group.Key,
                "Priority",
                members.Where(m => m.Priority is not null),
                m => m.Priority
            );
            RequireAgreement(
                group.Key,
                "MaxActiveJobs",
                members.Where(m => m.MaxActiveJobsStated),
                m => m.MaxActiveJobs
            );
            RequireAgreement(
                group.Key,
                "Enabled",
                members.Where(m => m.IsEnabled is not null),
                m => m.IsEnabled
            );
        }
    }

    private static void RequireAgreement<T>(
        string group,
        string setting,
        IEnumerable<GroupDeclaration> stating,
        Func<GroupDeclaration, T> value
    )
    {
        var list = stating.ToList();
        if (list.Count < 2)
            return;

        var first = list[0];
        var conflicting = list.FirstOrDefault(m => !Equals(value(m), value(first)));
        if (conflicting is null)
            return;

        throw new InvalidOperationException(
            $"Manifest group '{group}' is given two different {setting} values: "
                + $"{Describe(value(first))} by {first.DeclaredBy} and "
                + $"{Describe(value(conflicting))} by {conflicting.DeclaredBy}. "
                + "Every start writes a group setting a member states, so the value in force "
                + "would depend on seeding order.\n\n"
                + $"State {setting} on one member of the group, or the same value on each."
        );
    }

    private static string Describe<T>(T value) => value is null ? "none" : value.ToString()!;
}
