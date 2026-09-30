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
/// </summary>
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
            .BeFalse("the code does not state Enabled, so the operator's switch stands");
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
        manifest.IsEnabled.Should().BeTrue("the code states Enabled(true), and code wins");
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
        group.MaxActiveJobs.Should().Be(5);
        group.Priority.Should().Be(20);
        group.IsEnabled.Should().BeFalse();
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
        group.MaxActiveJobs.Should().Be(2, "the code states MaxActiveJobs(2)");
        group.Priority.Should().Be(20, "the code does not state the group priority");
    }

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
