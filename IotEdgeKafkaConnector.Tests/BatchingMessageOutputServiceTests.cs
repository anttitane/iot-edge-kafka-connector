using System.Text.Json;
using IotEdgeKafkaConnector.Application.Processing;
using IotEdgeKafkaConnector.Domain.Models;
using IotEdgeKafkaConnector.Infrastructure.Output;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace IotEdgeKafkaConnector.Tests;

public sealed class BatchingMessageOutputServiceTests
{
    [Fact]
    public async Task SendAsync_FlushesWhenBatchSizeReached()
    {
        var inner = new TestOutputService();
        var options = Options.Create(new BatchingOptions
        {
            IsEnabled = true,
            MaxBatchSize = 2,
            BatchIntervalSeconds = 60
        });

        await using var output = new BatchingMessageOutputService(
            inner,
            options,
            NullLogger<BatchingMessageOutputService>.Instance);

        await output.SendAsync(new[] { CreateMessage("nodeA", "m1", "src", "1") }, CancellationToken.None);
        await output.SendAsync(new[] { CreateMessage("nodeA", "m1", "src", "2") }, CancellationToken.None);

        Assert.Single(inner.Batches);
        Assert.Equal(2, inner.Batches[0].Count);
    }

    private static TelemetryMessage CreateMessage(string node, string measurementType, string source, string rawNumber)
    {
        using var document = JsonDocument.Parse(rawNumber);
        return new TelemetryMessage(node, measurementType, DateTimeOffset.UtcNow, document.RootElement.Clone(), source);
    }
}
