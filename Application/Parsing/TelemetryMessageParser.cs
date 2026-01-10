using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Application.Parsing;

/// <summary>
/// Parses raw Kafka payloads into telemetry messages based on configured interests.
/// Only returns messages for configured source/node combinations; optional measurement
/// keys allow extracting nested values (e.g., temperature, humidity).
/// </summary>
public sealed class TelemetryMessageParser(IOptions<AppConfiguration> config, ILogger<TelemetryMessageParser> logger) : IMessageParser
{
    private readonly AppConfiguration _config = config.Value;
    private readonly ILogger<TelemetryMessageParser> _logger = logger;

    public IEnumerable<TelemetryMessage> Parse(string payload, string? topic = null, string? key = null)
    {
        if (_config.Parsing.Interests is null || _config.Parsing.Interests.Count == 0)
        {
            yield break;
        }

        if (string.IsNullOrWhiteSpace(payload))
        {
            yield break;
        }

        JsonElement root;
        try
        {
            using var document = JsonDocument.Parse(payload);
            root = document.RootElement.Clone();
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Invalid JSON payload from topic {Topic}", topic ?? "unknown");
            yield break;
        }

        var source = TryGetString(root, "source") ?? "unknown";
        var nodeName = TryGetString(root, "nodeName") ?? TryGetString(root, "nodeId") ?? key ?? "unknown";

        var timestamp = DateTimeOffset.UtcNow;
        var timestampString = TryGetString(root, "timestamp_iso") ?? TryGetString(root, "timestamp");
        if (timestampString is not null && DateTimeOffset.TryParse(timestampString, out var parsed))
        {
            timestamp = parsed;
        }

        var valueElement = root.TryGetProperty("value", out var valueProp) ? valueProp.Clone() : root.Clone();

        foreach (var interest in _config.Parsing.Interests)
        {
            if (!string.Equals(interest.Source, source, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!string.Equals(interest.NodeName, nodeName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var measurementType = string.IsNullOrWhiteSpace(interest.MeasurementKey)
                ? interest.NodeName
                : interest.MeasurementKey!;

            JsonElement measurementValue;

            if (string.IsNullOrWhiteSpace(interest.MeasurementKey))
            {
                measurementValue = valueElement;
            }
            else if (valueElement.ValueKind == JsonValueKind.Object && valueElement.TryGetProperty(interest.MeasurementKey!, out var property))
            {
                measurementValue = property.Clone();
            }
            else
            {
                _logger.LogDebug(
                    "Configured measurement key '{MeasurementKey}' not found for node {NodeName} source {Source}",
                    interest.MeasurementKey,
                    interest.NodeName,
                    interest.Source);
                continue;
            }

            yield return new TelemetryMessage(
                nodeName,
                measurementType,
                timestamp,
                measurementValue,
                source);
        }
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        if (element.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String)
        {
            return property.GetString();
        }

        return null;
    }
}
