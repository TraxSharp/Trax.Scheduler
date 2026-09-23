using System.Text.Json;
using System.Text.Json.Serialization;

namespace Trax.Scheduler.Services.RunExecutor;

/// <summary>
/// The JSON options for the remote-run wire, fixed by Trax rather than taken from the host.
/// </summary>
/// <remarks>
/// A worker used to write its response with whatever JSON options its host configured, so a host
/// that serializes enums as strings sent <c>FailureClass</c> as <c>"Transient"</c>, and a scheduler
/// reading with defaults failed on every error response that carried one. The writer now always
/// sends enums as their integers, and the reader accepts either form, so neither end depends on
/// the other's host configuration (see scheduler/0001).
/// </remarks>
internal static class RemoteRunJson
{
    /// <summary>What a worker writes: web defaults, enums as integers.</summary>
    public static readonly JsonSerializerOptions Write = new(JsonSerializerDefaults.Web);

    /// <summary>What a scheduler reads: web defaults, enums as integers or names.</summary>
    public static readonly JsonSerializerOptions Read = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };
}
