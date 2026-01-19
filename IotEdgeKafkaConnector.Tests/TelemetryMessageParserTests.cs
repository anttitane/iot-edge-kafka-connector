using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Application.Parsing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace IotEdgeKafkaConnector.Tests;

public sealed class TelemetryMessageParserTests
{
    [Fact]
    public void Parse_ReturnsMeasurementForMatchingInterestKey()
    {
        var config = Options.Create(new AppConfiguration
        {
            Parsing = new AppConfiguration.ParsingOptions
            {
                Interests = new[]
                {
                    new AppConfiguration.TelemetryInterest
                    {
                        Source = "src1",
                        NodeName = "nodeA",
                        MeasurementKey = "temperature"
                    }
                }
            }
        });

        var parser = new TelemetryMessageParser(config, NullLogger<TelemetryMessageParser>.Instance);

        const string payload = "{\"source\":\"src1\",\"nodeName\":\"nodeA\",\"value\":{\"temperature\":12.34,\"humidity\":50}}";
        var results = parser.Parse(payload).ToList();

        Assert.Single(results);
        var message = results[0];
        Assert.Equal("nodeA", message.NodeName);
        Assert.Equal("temperature", message.MeasurementType);
        Assert.Equal(12.34, message.Value.GetDouble(), 2);
        Assert.Equal("src1", message.Source);
    }

    [Fact]
    public void Parse_UsesValueWhenNoMeasurementKey()
    {
        var config = Options.Create(new AppConfiguration
        {
            Parsing = new AppConfiguration.ParsingOptions
            {
                Interests = new[]
                {
                    new AppConfiguration.TelemetryInterest
                    {
                        Source = "src2",
                        NodeName = "nodeB",
                        MeasurementKey = null
                    }
                }
            }
        });

        var parser = new TelemetryMessageParser(config, NullLogger<TelemetryMessageParser>.Instance);

        const string payload = "{\"source\":\"src2\",\"nodeName\":\"nodeB\",\"value\":42}";
        var results = parser.Parse(payload).ToList();

        Assert.Single(results);
        var message = results[0];
        Assert.Equal("nodeB", message.NodeName);
        Assert.Equal("nodeB", message.MeasurementType);
        Assert.Equal(42, message.Value.GetInt32());
        Assert.Equal("src2", message.Source);
    }
}
