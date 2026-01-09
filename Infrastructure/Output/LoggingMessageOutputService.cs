using System.Text.Json;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;

namespace IotEdgeKafkaConnector.Infrastructure.Output;

/// <summary>
/// Logs telemetry messages as an output channel.
/// </summary>
public sealed class LoggingMessageOutputService(ILogger<LoggingMessageOutputService> logger) : IMessageOutputService
{
    private readonly ILogger<LoggingMessageOutputService> _logger = logger;
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = true };

    public Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            try
            {
                var valueText = JsonSerializer.Serialize(message.Value, _jsonOptions);
                _logger.LogInformation(
                    "Telemetry message | source={Source} nodeId={NodeId} name={NodeName} ts={Timestamp:o} topic={Topic} value={Value}",
                    message.Source,
                    message.NodeId,
                    message.NodeName,
                    message.Timestamp,
                    message.Topic,
                    valueText);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to format telemetry message for logging");
            }
        }

        return Task.CompletedTask;
    }
}
