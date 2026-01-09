using System.Linq;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;

namespace IotEdgeKafkaConnector.Application.Processing;

/// <summary>
/// Fans out messages to multiple output services.
/// </summary>
public sealed class CompositeMessageOutputService : IMessageOutputService
{
    private readonly IReadOnlyCollection<IMessageOutputService> _outputs;
    private readonly ILogger<CompositeMessageOutputService> _logger;

    public CompositeMessageOutputService(IEnumerable<IMessageOutputService> outputs, ILogger<CompositeMessageOutputService> logger)
    {
        _outputs = outputs.ToArray();
        _logger = logger;
    }

    public async Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        foreach (var output in _outputs)
        {
            try
            {
                await output.SendAsync(messages, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Message output {OutputType} failed", output.GetType().Name);
            }
        }
    }
}
