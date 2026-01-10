using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Application.Processing;

/// <summary>
/// Default implementation that forwards telemetry to configured output services.
/// </summary>
public sealed class PassthroughMessageProcessor(IMessageOutputService outputService) : IMessageProcessor
{
    public Task ProcessAsync(TelemetryEnvelope envelope, CancellationToken cancellationToken)
    {
        var message = new TelemetryMessage(
            envelope.Source,
            envelope.NodeName ?? string.Empty,
            envelope.Timestamp,
            envelope.Value);

        return outputService.SendAsync(new[] { message }, cancellationToken);
    }
}
