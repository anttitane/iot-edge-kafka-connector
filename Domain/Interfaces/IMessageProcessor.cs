using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Domain.Interfaces;

public interface IMessageProcessor
{
    Task ProcessAsync(TelemetryEnvelope envelope, CancellationToken cancellationToken);
}
