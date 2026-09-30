# Trax.Scheduler

[![Build](https://github.com/TraxSharp/Trax.Scheduler/actions/workflows/nuget_release.yml/badge.svg?branch=main)](https://github.com/TraxSharp/Trax.Scheduler/actions/workflows/nuget_release.yml?query=branch%3Amain)
[![NuGet](https://img.shields.io/nuget/v/Trax.Scheduler)](https://www.nuget.org/packages/Trax.Scheduler)
[![codecov](https://codecov.io/gh/TraxSharp/Trax.Scheduler/branch/main/graph/badge.svg)](https://codecov.io/gh/TraxSharp/Trax.Scheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/TraxSharp/Trax.Scheduler/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-traxsharp.net-blue)](https://traxsharp.net/docs/scheduler)

> Part of [Trax](https://github.com/TraxSharp): business logic you can call, schedule, or serve as an API, with every
> run recorded in your Postgres. [Docs](https://traxsharp.net/docs) · [Getting started](https://traxsharp.net/docs/getting-started) · [All repos](https://github.com/TraxSharp)

Trax.Scheduler adds cron schedules, retries, dead letters and remote or Lambda workers for Trax trains. It builds on
[Trax.Mediator](https://github.com/TraxSharp/Trax.Mediator), and Trax.Api and Trax.Dashboard read and manage what it
schedules.

## Install

```bash
dotnet add package Trax.Scheduler
dotnet add package Trax.Effect.Data.Postgres    # or Trax.Effect.Data.Sqlite for a single process
```

## Example

Adapted from the game server sample:

```csharp
builder.Services.AddTrax(trax => trax
    .AddEffects(effects => effects.UsePostgres(connectionString))
    .AddMediator(typeof(RecalculateLeaderboardTrain).Assembly)
    .AddScheduler(scheduler => scheduler
        .Schedule<IRecalculateLeaderboardTrain>(
            "leaderboard-na",
            new RecalculateLeaderboardInput { Region = "na" },
            Every.Minutes(5),
            options => options.MaxRetries(3))));
```

Every five minutes the scheduler queues a run with that input and a worker claims it. A failed run retries with backoff,
then lands in the dead-letter queue.

## License

MIT. There is no commercial edition, and there will not be one.

Trax is an independent open-source project and is not affiliated with the Utah Transit Authority, Trax Retail, or any
other organization using the Trax name.
