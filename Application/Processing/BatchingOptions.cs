namespace IotEdgeKafkaConnector.Application.Processing;

public record BatchingOptions
{
    public bool IsEnabled { get; init; } = false;
    public int MaxBatchSize { get; init; } = 10;
    public int BatchIntervalSeconds { get; init; } = 30;
}
