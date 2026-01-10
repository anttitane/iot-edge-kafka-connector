using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Domain.Interfaces;

public interface IMessageParser
{
    IEnumerable<TelemetryMessage> Parse(string payload, string? topic = null, string? key = null);
}
