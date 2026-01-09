using IotEdgeKafkaConnector.Domain.Models;

namespace IotEdgeKafkaConnector.Application.Configuration;

public record AppConfiguration
{
    public KafkaOptions Kafka { get; init; } = new();
    public IoTHubOptions IoTHub { get; init; } = new();
    public ProcessingOptions Processing { get; init; } = new();
    public BatchingOptions Batching { get; init; } = new();

    public record KafkaOptions
    {
        public string BootstrapServers { get; init; } = string.Empty;
        public string Topic { get; init; } = string.Empty;
        public string GroupId { get; init; } = "iot-edge-connector";
        public bool EnableAutoCommit { get; init; } = true;
        public int SessionTimeoutMs { get; init; } = 10000;
        public AutoOffsetResetMode AutoOffsetReset { get; init; } = AutoOffsetResetMode.Earliest;
        public int RetryBackoffSeconds { get; init; } = 5;
    }

    public record IoTHubOptions
    {
        public string ConnectionString { get; init; } = string.Empty;
        public string TransportType { get; init; } = "Mqtt_Tcp_Only";
    }

    public record ProcessingOptions
    {
        public ProcessingMode Mode { get; init; } = ProcessingMode.Passthrough;
        public int AggregationWindowSeconds { get; init; } = 60;
    }

    public record BatchingOptions
    {
        public bool Enabled { get; init; } = false;
        public int MaxBatchSize { get; init; } = 50;
        public int FlushIntervalSeconds { get; init; } = 10;
        public TransmissionMode Mode => Enabled ? TransmissionMode.Batched : TransmissionMode.Single;
    }

    public enum AutoOffsetResetMode
    {
        Earliest,
        Latest
    }
}
