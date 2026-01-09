using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace IotEdgeKafkaConnector.Application.Processing;

/// <summary>
/// Default implementation that simply forwards parsed telemetry to downstream handlers.
/// This is a placeholder for passthrough/aggregation strategies.
/// </summary>
public sealed class PassthroughMessageProcessor(ILogger<PassthroughMessageProcessor> logger) : IMessageProcessor
{
    public Task ProcessAsync(TelemetryEnvelope envelope, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Telemetry received | source={Source} nodeId={NodeId} name={NodeName} ts={Timestamp:o} topic={Topic} payload={Payload}",
            envelope.Source,
            envelope.NodeId,
            envelope.NodeName ?? "",
            envelope.Timestamp,
            envelope.Topic ?? "unknown",
            JsonSerializer.Serialize(envelope.Value));

        // Hook for strategy-specific processing (passthrough/aggregation) will be added later.
        return Task.CompletedTask;
    }
}
