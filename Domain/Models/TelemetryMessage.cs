using System.Text.Json;

namespace IotEdgeKafkaConnector.Domain.Models;

public sealed record TelemetryMessage(
    string? NodeName,
    DateTimeOffset Timestamp,
    JsonElement Value,
    string Source);
