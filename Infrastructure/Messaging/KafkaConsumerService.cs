using System.Text.Json;
using Confluent.Kafka;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Infrastructure.Messaging;

/// <summary>
/// Kafka consumer that parses telemetry payloads and hands them to the processing pipeline.
/// Includes basic retry and error handling suitable for edge deployments.
/// </summary>
public sealed class KafkaConsumerService(
    IOptions<AppConfiguration> options,
    IMessageProcessor messageProcessor,
    ILogger<KafkaConsumerService> logger) : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger = logger;
    private readonly IMessageProcessor _messageProcessor = messageProcessor;
    private readonly AppConfiguration _settings = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Kafka consumer starting with bootstrap={BootstrapServers}, topic={Topic}, group={GroupId}, autoCommit={AutoCommit}, offsetReset={OffsetReset}",
                _settings.Kafka.BootstrapServers,
                _settings.Kafka.Topic,
                _settings.Kafka.GroupId,
                _settings.Kafka.EnableAutoCommit,
                _settings.Kafka.AutoOffsetReset);

            using var consumer = BuildConsumer();

            try
            {
                consumer.Subscribe(_settings.Kafka.Topic);
                _logger.LogInformation("Kafka consumer subscribed to topic {Topic}", _settings.Kafka.Topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result = null;

                    try
                    {
                        result = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException consumeEx)
                    {
                        _logger.LogWarning(consumeEx, "Kafka consume error: {Reason}", consumeEx.Error.Reason);
                        await Task.Delay(TimeSpan.FromSeconds(_settings.Kafka.RetryBackoffSeconds), stoppingToken);
                        continue;
                    }

                    if (result?.Message is null)
                    {
                        continue;
                    }

                    if (!TryParseTelemetry(result, out var telemetry))
                    {
                        continue;
                    }

                    await _messageProcessor.ProcessAsync(telemetry, stoppingToken).ConfigureAwait(false);

                    if (!_settings.Kafka.EnableAutoCommit)
                    {
                        consumer.Commit(result);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // graceful shutdown requested
            }
            catch (KafkaException kafkaEx)
            {
                _logger.LogError(kafkaEx, "Kafka exception; restarting consumer in {DelaySeconds}s", _settings.Kafka.RetryBackoffSeconds);
                await Task.Delay(TimeSpan.FromSeconds(_settings.Kafka.RetryBackoffSeconds), stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled exception in Kafka consumer; restarting in {DelaySeconds}s", _settings.Kafka.RetryBackoffSeconds);
                await Task.Delay(TimeSpan.FromSeconds(_settings.Kafka.RetryBackoffSeconds), stoppingToken);
            }
        }
    }

    private IConsumer<string, string> BuildConsumer()
    {
        var kafka = _settings.Kafka;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = kafka.GroupId,
            EnableAutoCommit = kafka.EnableAutoCommit,
            AutoOffsetReset = kafka.AutoOffsetReset == AppConfiguration.AutoOffsetResetMode.Earliest
                ? Confluent.Kafka.AutoOffsetReset.Earliest
                : Confluent.Kafka.AutoOffsetReset.Latest,
            SessionTimeoutMs = kafka.SessionTimeoutMs,
            EnablePartitionEof = false
        };

        return new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, error) =>
            {
                if (error.IsFatal)
                {
                    _logger.LogError("Fatal Kafka error: {Reason}", error.Reason);
                }
                else
                {
                    _logger.LogWarning("Kafka error: {Reason}", error.Reason);
                }
            })
            .SetStatisticsHandler((_, json) => _logger.LogDebug("Kafka stats: {Stats}", json))
            .Build();
    }

    private bool TryParseTelemetry(ConsumeResult<string, string> result, out TelemetryEnvelope envelope)
    {
        envelope = default!;
        if (string.IsNullOrWhiteSpace(result.Message.Value))
        {
            _logger.LogWarning("Kafka message value was empty (topic {Topic}, offset {Offset})", result.Topic, result.Offset.Value);
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(result.Message.Value);
            var root = document.RootElement;

            var nodeId = TryGetString(root, "nodeId") ?? result.Message.Key ?? "unknown";
            var source = TryGetString(root, "source") ?? "unknown";
            var nodeName = TryGetString(root, "nodeName");

            var timestamp = DateTimeOffset.UtcNow;
            var timestampString = TryGetString(root, "timestamp_iso") ?? TryGetString(root, "timestamp");
            if (timestampString is not null && DateTimeOffset.TryParse(timestampString, out var parsed))
            {
                timestamp = parsed;
            }

            var value = root.TryGetProperty("value", out var valueElement)
                ? valueElement.Clone()
                : root.Clone();

            envelope = new TelemetryEnvelope(
                nodeId,
                source,
                timestamp,
                value,
                nodeName,
                root.Clone(),
                result.Topic,
                result.Partition.Value,
                result.Offset.Value);

            return true;
        }
        catch (JsonException jsonEx)
        {
            _logger.LogWarning(jsonEx, "Invalid JSON payload on topic {Topic} (offset {Offset})", result.Topic, result.Offset.Value);
            return false;
        }
    }

    private static string? TryGetString(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String)
        {
            return property.GetString();
        }

        return null;
    }
}
