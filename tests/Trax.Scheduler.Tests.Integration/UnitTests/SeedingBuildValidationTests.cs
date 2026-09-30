using FluentAssertions;
using LanguageExt;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Trax.Effect.Data.InMemory.Extensions;
using Trax.Effect.Extensions;
using Trax.Mediator.Extensions;
using Trax.Scheduler.Configuration;
using Trax.Scheduler.Extensions;
using Trax.Scheduler.Services.Scheduling;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;

namespace Trax.Scheduler.Tests.Integration.UnitTests;

/// <summary>
/// What the builder refuses about manifest groups and named batches, before any row is written.
/// </summary>
[TestFixture]
public class SeedingBuildValidationTests
{
    [Test]
    public void Two_members_stating_different_limits_for_one_group_fail_the_build_naming_both()
    {
        var act = () =>
            Build(scheduler =>
                scheduler
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "member-a",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("shared", g => g.MaxActiveJobs(2))
                    )
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "member-b",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("shared", g => g.MaxActiveJobs(3))
                    )
            );

        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*shared*MaxActiveJobs*member-a*member-b*");
    }

    [Test]
    public void Two_members_stating_different_group_priorities_fail_the_build()
    {
        var act = () =>
            Build(scheduler =>
                scheduler
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "p-a",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("prio", g => g.Priority(5))
                    )
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "p-b",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("prio", g => g.Priority(9))
                    )
            );

        act.Should().Throw<InvalidOperationException>().WithMessage("*prio*Priority*p-a*p-b*");
    }

    [Test]
    public void Members_that_agree_or_leave_a_group_setting_unstated_build()
    {
        var act = () =>
            Build(scheduler =>
                scheduler
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "agree-a",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("agreed", g => g.MaxActiveJobs(2).Enabled(true))
                    )
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "agree-b",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("agreed", g => g.MaxActiveJobs(2))
                    )
                    .Schedule<ISchedulerTestTrain, SchedulerTestInput, Unit>(
                        "agree-c",
                        new SchedulerTestInput(),
                        Every.Minutes(5),
                        o => o.Group("agreed")
                    )
            );

        act.Should().NotThrow();
    }

    private static void Build(
        Func<SchedulerConfigurationBuilder, SchedulerConfigurationBuilder> configure
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddTrax(trax =>
            trax.AddEffects(effects => effects.UseInMemory())
                .AddMediator(typeof(AssemblyMarker).Assembly)
                .AddScheduler(scheduler =>
                {
                    scheduler.UseInMemoryWorkers();
                    return configure(scheduler);
                })
        );
    }
}
