using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Application.Processing;

/// <summary>
/// Default implementation that forwards telemetry to configured output services.
/// </summary>
public sealed class PassthroughMessageProcessor(IMessageOutputService outputService) : IMessageProcessor
{
    public Task ProcessAsync(TelemetryMessage message, CancellationToken cancellationToken) =>
        outputService.SendAsync(new[] { message }, cancellationToken);
}
