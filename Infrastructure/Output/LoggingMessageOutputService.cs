using System.Linq;
using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Infrastructure.Output;

/// <summary>
/// Logs telemetry messages as an output channel.
/// </summary>
public sealed class LoggingMessageOutputService(
    ILogger<LoggingMessageOutputService> logger,
    IOptions<OutputOptions> outputOptions) : IMessageOutputService
{
    private readonly ILogger<LoggingMessageOutputService> _logger = logger;
    private readonly OutputOptions _options = outputOptions.Value;
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = true };

    public Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            try
            {
                var valueText = JsonSerializer.Serialize(message.Value, _jsonOptions);
                _logger.LogInformation(
                    "Telemetry message | source={Source} name={NodeName} ts={Timestamp:o} value={Value}",
                    message.Source,
                    message.NodeName,
                    message.Timestamp,
                    valueText);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to format telemetry message for logging");
            }
        }

        if (_options.LogSerializedPayload)
        {
            try
            {
                var serialized = messages.Count == 1
                    ? JsonSerializer.Serialize(messages.First(), _jsonOptions)
                    : JsonSerializer.Serialize(messages, _jsonOptions);
                _logger.LogInformation("Serialized telemetry payload ({Count} message(s)): {Payload}", messages.Count, serialized);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to serialize telemetry payload for logging");
            }
        }

        return Task.CompletedTask;
    }
}
