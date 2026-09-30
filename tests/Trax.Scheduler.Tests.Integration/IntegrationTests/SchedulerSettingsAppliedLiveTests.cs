using FluentAssertions;
using LanguageExt;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Trax.Effect.Enums;
using Trax.Effect.Models.DeadLetter;
using Trax.Effect.Models.DeadLetter.DTOs;
using Trax.Effect.Models.Manifest;
using Trax.Scheduler.Services.Operations;
using Trax.Scheduler.Tests.Integration.Fakes.Trains;
using Trax.Scheduler.Tests.Integration.Fixtures;
using Every = Trax.Scheduler.Services.Scheduling.Every;

namespace Trax.Scheduler.Tests.Integration.IntegrationTests;

/// <summary>
/// A setting changed at runtime changes what the running scheduler does.
/// </summary>
[TestFixture]
public class SchedulerSettingsAppliedLiveTests
{
    [Test]
    public async Task Turning_AutoPurgeDeadLetters_off_stops_resolved_dead_letters_being_deleted()
    {
        await using var fx = await SchedulerE2EFixture.CreateAsync(s =>
            s.Schedule<ISchedulerTestTrain>(
                "purge-target",
                new SchedulerTestInput { Value = "x" },
                Every.Minutes(5)
            )
        );
        await fx.MaterializePendingManifestsAsync();

        var ops = fx.Services.GetRequiredService<IOperationsService>();
        var result = await ops.UpdateSchedulerConfigAsync(
            new UpdateSchedulerConfigInput(AutoPurgeDeadLetters: false),
            CancellationToken.None
        );
        result.Success.Should().BeTrue();
        ops.GetSchedulerConfig().AutoPurgeDeadLetters.Should().BeFalse();

        var manifest = await fx.DataContext.Manifests.FirstAsync(m =>
            m.ExternalId == "purge-target"
        );
        var deadLetter = DeadLetter.Create(
            new CreateDeadLetter
            {
                Manifest = manifest,
                Reason = "kept for investigation",
                RetryCount = 3,
            }
        );
        deadLetter.Acknowledge("resolved long ago");
        deadLetter.ResolvedAt = DateTime.UtcNow.AddDays(-60);
        await fx.DataContext.Track(deadLetter);
        await fx.DataContext.SaveChanges(CancellationToken.None);
        fx.DataContext.Reset();

        await fx.RunDeadLetterCleanupAsync();

        var stillThere = await fx
            .DataContext.DeadLetters.AsNoTracking()
            .AnyAsync(d => d.Id == deadLetter.Id);
        stillThere
            .Should()
            .BeTrue(
                "the operator turned AutoPurgeDeadLetters off, and UpdateSchedulerConfigAsync "
                    + "says a change takes effect immediately"
            );
    }
}
