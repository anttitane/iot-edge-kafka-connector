using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Application.Processing;

/// <summary>
/// Buffers telemetry and flushes based on batch size or interval before delegating
/// to configured output services.
/// </summary>
public sealed class BatchingMessageProcessor : IMessageProcessor, IAsyncDisposable
{
    private readonly IMessageOutputService _outputService;
    private readonly BatchingOptions _batching;
    private readonly ILogger<BatchingMessageProcessor> _logger;
    private readonly object _sync = new();
    private readonly List<TelemetryMessage> _buffer = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly PeriodicTimer? _timer;

    public BatchingMessageProcessor(
        IMessageOutputService outputService,
        IOptions<BatchingOptions> batchingOptions,
        ILogger<BatchingMessageProcessor> logger)
    {
        _outputService = outputService;
        _batching = batchingOptions.Value;
        _logger = logger;

        if (_batching.IsEnabled)
        {
            var interval = TimeSpan.FromSeconds(Math.Max(1, _batching.BatchIntervalSeconds));
            _timer = new PeriodicTimer(interval);
            _ = RunPeriodicFlushAsync(_cts.Token);
        }
    }

    public async Task ProcessAsync(TelemetryEnvelope envelope, CancellationToken cancellationToken)
    {
        var message = MapToTelemetryMessage(envelope);

        if (!_batching.IsEnabled)
        {
            await _outputService.SendAsync(new[] { message }, cancellationToken).ConfigureAwait(false);
            return;
        }

        bool shouldFlush;
        lock (_sync)
        {
            _buffer.Add(message);
            shouldFlush = _buffer.Count >= _batching.MaxBatchSize;
        }

        if (shouldFlush)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private TelemetryMessage MapToTelemetryMessage(TelemetryEnvelope envelope) => new(
        envelope.Source,
        envelope.NodeName ?? string.Empty,
        envelope.Timestamp,
        envelope.Value);

    private async Task FlushAsync(CancellationToken cancellationToken)
    {
        List<TelemetryMessage> batch;
        lock (_sync)
        {
            if (_buffer.Count == 0)
            {
                return;
            }

            batch = new List<TelemetryMessage>(_buffer);
            _buffer.Clear();
        }

        try
        {
            await _outputService.SendAsync(batch, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Flushed {Count} telemetry messages", batch.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush telemetry batch; retaining in buffer");
            lock (_sync)
            {
                _buffer.InsertRange(0, batch);
            }
        }
    }

    private async Task RunPeriodicFlushAsync(CancellationToken cancellationToken)
    {
        if (_timer is null)
        {
            return;
        }

        try
        {
            while (await _timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _timer?.Dispose();
        _cts.Dispose();
        try
        {
            await FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // ignore dispose-time errors
        }
    }
}
