using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Application.Processing;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Infrastructure.Output;

/// <summary>
/// Buffers telemetry messages before forwarding to the inner output service, reducing send frequency.
/// Used to batch aggregation results prior to IoT Hub/logging.
/// </summary>
public sealed class BatchingMessageOutputService(
    IMessageOutputService inner,
    IOptions<BatchingOptions> options,
    ILogger<BatchingMessageOutputService> logger) : IMessageOutputService, IAsyncDisposable
{
    private readonly IMessageOutputService _inner = inner;
    private readonly BatchingOptions _options = options.Value;
    private readonly ILogger<BatchingMessageOutputService> _logger = logger;
    private readonly object _sync = new();
    private readonly List<TelemetryMessage> _buffer = new();
    private readonly CancellationTokenSource _cts = new();
    private PeriodicTimer? _timer;
    private Task _background = Task.CompletedTask;
    private readonly object _startSync = new();
    private bool _started;
    

    public async Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        EnsureStarted();

        if (!_options.IsEnabled)
        {
            await _inner.SendAsync(messages, cancellationToken).ConfigureAwait(false);
            return;
        }

        bool shouldFlush;
        lock (_sync)
        {
            _buffer.AddRange(messages);
            shouldFlush = _buffer.Count >= _options.MaxBatchSize;
        }

        if (shouldFlush)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

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
            await _inner.SendAsync(batch, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Flushed {Count} telemetry messages (output batching)", batch.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush output batch; retaining in buffer");
            lock (_sync)
            {
                _buffer.InsertRange(0, batch);
            }
        }
    }

    private async Task RunPeriodicFlushAsync(CancellationToken cancellationToken)
    {
        var timer = _timer;
        if (timer is null)
        {
            return;
        }

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
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

        if (_inner is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void EnsureStarted()
    {
        if (!_options.IsEnabled || _started)
        {
            return;
        }

        lock (_startSync)
        {
            if (_started || !_options.IsEnabled)
            {
                return;
            }

            var interval = TimeSpan.FromSeconds(Math.Max(1, _options.BatchIntervalSeconds));
            _timer = new PeriodicTimer(interval);
            _background = RunPeriodicFlushAsync(_cts.Token);
            _started = true;
        }
    }
}
