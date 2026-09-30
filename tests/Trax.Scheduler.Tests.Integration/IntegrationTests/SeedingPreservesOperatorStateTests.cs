using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Trax.Scheduler.Services.Operations;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Every = Trax.Scheduler.Services.Scheduling.Every;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// Every host start schedules its manifests again. A re-seed writes only the fields the code
/// states; a field it leaves unstated keeps whatever an operator set at runtime.
///
/// <para>Enforces <c>docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md</c>: code wins only for the settings it states.</para>
/// </summary>
[Property("adr", "docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md")]
[TestFixture]
public class SeedingPreservesOperatorStateTests
{
    [Test]
    public async Task A_manifest_disabled_at_runtime_stays_disabled_when_it_is_seeded_again()
    {
        await using var fx = await SchedulerE2EFixture.CreateAsync(_ => { });

        await ScheduleAsync(fx, "kill-switch");
        await fx.Scheduler.DisableAsync("kill-switch");

        await ScheduleAsync(fx, "kill-switch");

        var manifest = await fx
            .DataContext.Manifests.AsNoTracking()
            .FirstAsync(m => m.ExternalId == "kill-switch");
        manifest
            .IsEnabled.Should()
            .BeFalse(
                "the code does not state Enabled, so the operator's switch stands. See docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md."
            );
    }

    [Test]
    public async Task A_manifest_whose_code_states_Enabled_takes_the_code_value_on_a_re_seed()
    {
        await using var fx = await SchedulerE2EFixture.CreateAsync(_ => { });

        await ScheduleAsync(fx, "stated-enabled", o => o.Enabled(true));
        await fx.Scheduler.DisableAsync("stated-enabled");

        await ScheduleAsync(fx, "stated-enabled", o => o.Enabled(true));

        var manifest = await fx
            .DataContext.Manifests.AsNoTracking()
            .FirstAsync(m => m.ExternalId == "stated-enabled");
        manifest
            .IsEnabled.Should()
            .BeTrue(
                "the code states Enabled(true), and code wins. See docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md."
            );
    }

    [Test]
    public async Task Runtime_group_edits_survive_a_re_seed_that_states_no_group_settings()
    {
        await using var fx = await SchedulerE2EFixture.CreateAsync(_ => { });

        var manifest = await ScheduleAsync(fx, "grouped", o => o.Group("ops-edited"));
        var ops = fx.Services.GetRequiredService<IOperationsService>();
        var result = await ops.UpdateManifestGroupAsync(
            manifest.ManifestGroupId,
            new UpdateManifestGroupInput(MaxActiveJobs: 5, Priority: 20, IsEnabled: false),
            CancellationToken.None
        );
        result.Success.Should().BeTrue();

        await ScheduleAsync(fx, "grouped", o => o.Group("ops-edited"));

        var group = await fx
            .DataContext.ManifestGroups.AsNoTracking()
            .FirstAsync(g => g.Name == "ops-edited");
        const string because =
            "the code states no group settings, so runtime edits stand. See docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md.";
        group.MaxActiveJobs.Should().Be(5, because);
        group.Priority.Should().Be(20, because);
        group.IsEnabled.Should().BeFalse(because);
    }

    [Test]
    public async Task Group_settings_the_code_states_are_written_on_a_re_seed()
    {
        await using var fx = await SchedulerE2EFixture.CreateAsync(_ => { });

        var manifest = await ScheduleAsync(
            fx,
            "grouped-stated",
            o => o.Group("code-owned", g => g.MaxActiveJobs(2))
        );
        var ops = fx.Services.GetRequiredService<IOperationsService>();
        await ops.UpdateManifestGroupAsync(
            manifest.ManifestGroupId,
            new UpdateManifestGroupInput(MaxActiveJobs: 9, Priority: 20),
            CancellationToken.None
        );

        await ScheduleAsync(
            fx,
            "grouped-stated",
            o => o.Group("code-owned", g => g.MaxActiveJobs(2))
        );

        var group = await fx
            .DataContext.ManifestGroups.AsNoTracking()
            .FirstAsync(g => g.Name == "code-owned");
        group
            .MaxActiveJobs.Should()
            .Be(
                2,
                "the code states MaxActiveJobs(2). See docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md."
            );
        group
            .Priority.Should()
            .Be(
                20,
                "the code does not state the group priority. See docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md."
            );
    }

    [Test]
    public async Task A_stated_failure_window_is_written_and_an_unstated_one_keeps_the_stored_window()
    {
        await using var fx = await SchedulerE2EFixture.CreateAsync(_ => { });

        var created = await ScheduleAsync(
            fx,
            "windowed",
            o => o.FailureWindow(TimeSpan.FromHours(2))
        );
        created.FailureWindowSeconds.Should().Be(7200);

        await ScheduleAsync(fx, "windowed", o => o.FailureWindow(TimeSpan.FromHours(6)));
        (await LoadAsync(fx, "windowed"))
            .FailureWindowSeconds.Should()
            .Be(21600, "the code states a new window, and code wins");

        await ScheduleAsync(fx, "windowed");
        (await LoadAsync(fx, "windowed"))
            .FailureWindowSeconds.Should()
            .Be(
                21600,
                "the code no longer states a window, so the stored one stands. See docs/adr/0011-a-re-seed-writes-only-the-settings-the-code-states.md."
            );

        (await ScheduleAsync(fx, "unwindowed"))
            .FailureWindowSeconds.Should()
            .BeNull("a manifest that states no window uses the scheduler's");
    }

    private static Task<Effect.Models.Manifest.Manifest> LoadAsync(
        SchedulerE2EFixture fx,
        string externalId
    ) => fx.DataContext.Manifests.AsNoTracking().FirstAsync(m => m.ExternalId == externalId);

    private static Task<Effect.Models.Manifest.Manifest> ScheduleAsync(
        SchedulerE2EFixture fx,
        string externalId,
        Action<Configuration.ScheduleOptions>? options = null
    ) =>
        fx.Scheduler.ScheduleAsync<ISchedulerTestTrain, SchedulerTestInput, Unit>(
            externalId,
            new SchedulerTestInput { Value = externalId },
            Every.Minutes(5),
            options
        );
}
