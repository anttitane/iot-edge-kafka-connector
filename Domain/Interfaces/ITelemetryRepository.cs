using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Domain.Interfaces;

public interface ITelemetryRepository
{
    Task SendAsync(TelemetryEnvelope telemetry, CancellationToken cancellationToken);
    Task SendBatchAsync(IReadOnlyCollection<TelemetryEnvelope> batch, CancellationToken cancellationToken);
}
