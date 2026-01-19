using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Application.Processing;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace IotEdgeKafkaConnector.Tests;

public sealed class AggregatingMessageProcessorTests
{
    [Fact]
    public async Task Aggregation_UsesLeastPreciseScaleWhenRounding()
    {
        var output = new TestOutputService();
        var config = Options.Create(new AppConfiguration
        {
            Processing = new AppConfiguration.ProcessingOptions
            {
                AggregationWindowSeconds = 60
            }
        });

        var processor = new AggregatingMessageProcessor(
            output,
            config,
            NullLogger<AggregatingMessageProcessor>.Instance);

        var baseTime = DateTimeOffset.UtcNow;

        await processor.ProcessAsync(CreateMessage("nodeA", "temp", "src1", "1.2", baseTime), CancellationToken.None);
        await processor.ProcessAsync(CreateMessage("nodeA", "temp", "src1", "1.26", baseTime.AddSeconds(1)), CancellationToken.None);

        await processor.DisposeAsync();

        var sent = output.Batches.SelectMany(batch => batch).ToList();
        Assert.Single(sent);
        var average = sent[0].Value.GetDouble();
        Assert.Equal(1.2, average, 1);
    }

    private static TelemetryMessage CreateMessage(string node, string measurementType, string source, string rawNumber, DateTimeOffset timestamp)
    {
        using var document = JsonDocument.Parse(rawNumber);
        return new TelemetryMessage(node, measurementType, timestamp, document.RootElement.Clone(), source);
    }
}
