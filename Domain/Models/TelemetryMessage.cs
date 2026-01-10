using System.Text.Json;

namespace IotEdgeKafkaConnector.Domain.Models;

public sealed record TelemetryMessage(
    string? NodeName,
    string MeasurementType,
    DateTimeOffset Timestamp,
    JsonElement Value,
    string Source);
