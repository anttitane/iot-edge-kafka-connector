using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Tests;

public sealed class TestOutputService : IMessageOutputService
{
    private readonly List<IReadOnlyCollection<TelemetryMessage>> _batches = new();

    public IReadOnlyList<IReadOnlyCollection<TelemetryMessage>> Batches
    {
        get
        {
            lock (_batches)
            {
                return _batches.ToList();
            }
        }
    }

    public Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        lock (_batches)
        {
            _batches.Add(messages.ToArray());
        }

        return Task.CompletedTask;
    }
}
