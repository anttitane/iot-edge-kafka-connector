using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Domain.Interfaces;

public interface IMessageOutputService
{
    Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken);
}
