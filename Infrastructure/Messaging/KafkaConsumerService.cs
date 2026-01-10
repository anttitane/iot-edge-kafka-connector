using Confluent.Kafka;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
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
    IMessageParser messageParser,
    ILogger<KafkaConsumerService> logger) : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger = logger;
    private readonly IMessageProcessor _messageProcessor = messageProcessor;
    private readonly IMessageParser _messageParser = messageParser;
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

                    foreach (var message in _messageParser.Parse(result.Message.Value, result.Topic, result.Message.Key))
                    {
                        await _messageProcessor.ProcessAsync(message, stoppingToken).ConfigureAwait(false);
                    }

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

}
