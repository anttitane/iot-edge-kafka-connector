using System.Text.Json;

namespace IotEdgeKafkaConnector.Domain.Models;

public sealed record TelemetryMessage(
    string Source,
    string NodeId,
    string? NodeName,
    DateTimeOffset Timestamp,
    JsonElement Value,
    string Topic);
