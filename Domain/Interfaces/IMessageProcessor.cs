using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Domain.Interfaces;

public interface IMessageProcessor
{
    Task ProcessAsync(TelemetryMessage message, CancellationToken cancellationToken);
}
