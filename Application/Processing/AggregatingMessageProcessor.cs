using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Application.Processing;

/// <summary>
/// Aggregates numeric telemetry per (Source, NodeName, MeasurementType) over a sliding window and emits averages
/// for whatever samples arrived in the window (>=1 sample).
/// </summary>
public sealed class AggregatingMessageProcessor(
    IMessageOutputService outputService,
    IOptions<AppConfiguration> config,
    ILogger<AggregatingMessageProcessor> logger) : IMessageProcessor, IAsyncDisposable
{
    private readonly IMessageOutputService _outputService = outputService;
    private readonly TimeSpan _window = TimeSpan.FromSeconds(Math.Max(1, config.Value.Processing.AggregationWindowSeconds));
    private readonly ILogger<AggregatingMessageProcessor> _logger = logger;
    private readonly ConcurrentDictionary<AggregationKey, Bucket> _buckets = new();
    private readonly CancellationTokenSource _cts = new();
    private PeriodicTimer? _timer;
    private Task _background = Task.CompletedTask;
    private readonly object _startSync = new();
    private bool _started;

    public async Task ProcessAsync(TelemetryMessage message, CancellationToken cancellationToken)
    {
        EnsureStarted();

        if (!TryGetNumericValueAndScale(message.Value, out var numeric, out var scale))
        {
            _logger.LogDebug("Skipped non-numeric value for aggregation: source={Source} node={Node} measurement={Measurement}", message.Source, message.NodeName, message.MeasurementType);
            return;
        }

        var key = new AggregationKey(message.Source, message.NodeName ?? "unknown", message.MeasurementType);
        var bucket = _buckets.GetOrAdd(key, _ => new Bucket(DateTimeOffset.UtcNow));

        bool flushPriorWindow;
        double priorSum = 0;
        int priorCount = 0;
        int priorMinScale = Bucket.UnsetScale;
        DateTimeOffset priorTimestamp = default;

        lock (bucket.Sync)
        {
            if (bucket.Count == 0)
            {
                bucket.WindowStart = message.Timestamp;
            }

            flushPriorWindow = IsWindowExpired(bucket.WindowStart, message.Timestamp);

            if (flushPriorWindow)
            {
                priorSum = bucket.Sum;
                priorCount = bucket.Count;
                priorMinScale = bucket.MinScale;
                priorTimestamp = bucket.LastTimestamp;

                bucket.Sum = 0;
                bucket.Count = 0;
                bucket.MinScale = Bucket.UnsetScale;
                bucket.LastTimestamp = default;
                bucket.WindowStart = message.Timestamp;
            }

            bucket.Sum += numeric;
            bucket.Count += 1;
            bucket.MinScale = Math.Min(bucket.MinScale, scale);
            bucket.LastTimestamp = message.Timestamp;
        }

        if (flushPriorWindow && priorCount > 0)
        {
            await EmitAverageAsync(key, priorSum, priorCount, priorMinScale, priorTimestamp, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RunPeriodicFlushAsync(CancellationToken cancellationToken)
    {
        var timer = _timer ?? throw new InvalidOperationException("Aggregator timer not initialized");

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                var tasks = new List<Task>();

                foreach (var kvp in _buckets)
                {
                    var key = kvp.Key;
                    var bucket = kvp.Value;

                    double sum;
                    int count;
                    int minScale;
                    DateTimeOffset lastTs;

                    lock (bucket.Sync)
                    {
                        if (bucket.Count == 0)
                        {
                            continue;
                        }

                        if (!IsWindowExpired(bucket.WindowStart, DateTimeOffset.UtcNow))
                        {
                            continue;
                        }

                        sum = bucket.Sum;
                        count = bucket.Count;
                        minScale = bucket.MinScale;
                        lastTs = bucket.LastTimestamp;

                        bucket.Sum = 0;
                        bucket.Count = 0;
                        bucket.MinScale = Bucket.UnsetScale;
                        bucket.LastTimestamp = default;
                        bucket.WindowStart = DateTimeOffset.UtcNow;
                    }

                    if (count > 0)
                    {
                        tasks.Add(EmitAverageAsync(key, sum, count, minScale, lastTs, cancellationToken));
                    }
                }

                if (tasks.Count > 0)
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal on shutdown
        }
    }

    private Task EmitAverageAsync(AggregationKey key, double sum, int count, int minScale, DateTimeOffset lastTimestamp, CancellationToken cancellationToken)
    {
        var average = sum / count;
        var scale = minScale == Bucket.UnsetScale ? 0 : minScale;
        var rounded = Math.Round(average, scale, MidpointRounding.AwayFromZero);
        var valueElement = JsonSerializer.SerializeToElement(rounded);

        var output = new TelemetryMessage(
            key.NodeName,
            key.MeasurementType,
            lastTimestamp == default ? DateTimeOffset.UtcNow : lastTimestamp,
            valueElement,
            key.Source);

        _logger.LogInformation("Aggregated {Count} samples for {Node}/{Measurement} (source={Source})", count, key.NodeName, key.MeasurementType, key.Source);
        return _outputService.SendAsync(new[] { output }, cancellationToken);
    }

    private bool IsWindowExpired(DateTimeOffset windowStart, DateTimeOffset now) => now - windowStart >= _window;

    private static bool TryGetNumericValueAndScale(JsonElement element, out double value, out int scale)
    {
        value = 0;
        scale = Bucket.UnsetScale;

        switch (element.ValueKind)
        {
            case JsonValueKind.Number:
                if (!element.TryGetDouble(out value))
                {
                    return false;
                }

                scale = GetScale(element.GetRawText());
                return true;

            case JsonValueKind.String:
                var text = element.GetString();
                if (string.IsNullOrWhiteSpace(text))
                {
                    return false;
                }

                if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
                {
                    return false;
                }

                scale = GetScale(text);
                return true;

            default:
                return false;
        }
    }

    private static int GetScale(string rawText)
    {
        var dotIndex = rawText.IndexOf('.');
        if (dotIndex < 0)
        {
            return 0;
        }

        var endIndex = rawText.IndexOfAny(new[] { 'e', 'E' });
        if (endIndex < 0)
        {
            endIndex = rawText.Length;
        }

        var scale = endIndex - dotIndex - 1;
        return Math.Max(0, scale);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _timer?.Dispose();
        _cts.Dispose();

        var tasks = new List<Task>();
        foreach (var kvp in _buckets)
        {
            var key = kvp.Key;
            var bucket = kvp.Value;
            double sum;
            int count;
            int minScale;
            DateTimeOffset lastTs;

            lock (bucket.Sync)
            {
                sum = bucket.Sum;
                count = bucket.Count;
                minScale = bucket.MinScale;
                lastTs = bucket.LastTimestamp;
                bucket.Sum = 0;
                bucket.Count = 0;
                bucket.MinScale = Bucket.UnsetScale;
                bucket.LastTimestamp = default;
            }

            if (count > 0)
            {
                tasks.Add(EmitAverageAsync(key, sum, count, minScale, lastTs, CancellationToken.None));
            }
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }

    private void EnsureStarted()
    {
        if (_started)
        {
            return;
        }

        lock (_startSync)
        {
            if (_started)
            {
                return;
            }

            _timer = new PeriodicTimer(_window);
            _background = RunPeriodicFlushAsync(_cts.Token);
            _started = true;
        }
    }

    private sealed record AggregationKey(string Source, string NodeName, string MeasurementType);

    private sealed class Bucket
    {
        public const int UnsetScale = int.MaxValue;

        public Bucket(DateTimeOffset windowStart)
        {
            WindowStart = windowStart;
        }

        public object Sync { get; } = new();
        public double Sum { get; set; }
        public int Count { get; set; }
        public int MinScale { get; set; } = UnsetScale;
        public DateTimeOffset WindowStart { get; set; }
        public DateTimeOffset LastTimestamp { get; set; }
    }
}
