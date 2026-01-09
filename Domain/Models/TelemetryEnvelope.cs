using System.Text.Json;

namespace IotEdgeKafkaConnector.Domain.Models;

/// <summary>
/// Represents a telemetry reading captured from Kafka with flexible payload support.
/// </summary>
public sealed record TelemetryEnvelope(
    string NodeId,
    string Source,
    DateTimeOffset Timestamp,
    JsonElement Value,
    string? NodeName = null,
    JsonElement? Original = null,
    string? Topic = null,
    int? Partition = null,
    long? Offset = null)
{
    public bool HasOriginal => Original.HasValue && Original.Value.ValueKind != JsonValueKind.Undefined;
}
