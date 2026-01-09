using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;

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
            "Telemetry received from {Source} node {NodeId} at {Timestamp} (topic {Topic})",
            envelope.Source,
            envelope.NodeId,
            envelope.Timestamp,
            envelope.Topic ?? "unknown");

        // Hook for strategy-specific processing (passthrough/aggregation) will be added later.
        return Task.CompletedTask;
    }
}
