using System.Linq;
using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Infrastructure.Output;

/// <summary>
/// Sends telemetry messages to IoT Hub/Edge client.
/// Supports both DeviceClient (for Dev-PC) and ModuleClient (for IoT Edge).
/// </summary>
public sealed class IoTHubMessageOutputService : IMessageOutputService, IAsyncDisposable
{
    private readonly DeviceClient? _deviceClient;
    private readonly ModuleClient? _moduleClient;
    private readonly ILogger<IoTHubMessageOutputService> _logger;
    private readonly AppConfiguration _config;
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = true };

    public IoTHubMessageOutputService(IOptions<AppConfiguration> config, ILogger<IoTHubMessageOutputService> logger)
    {
        _logger = logger;
        _config = config.Value;
        
        (_deviceClient, _moduleClient) = CreateClient(_config);
    }

    public async Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.Count == 0) return;

        // Serialize messages to JSON payload
        byte[] payload = messages.Count == 1
            ? JsonSerializer.SerializeToUtf8Bytes(messages.First(), _jsonOptions)
            : JsonSerializer.SerializeToUtf8Bytes(messages, _jsonOptions);

        try
        {
            // Create Message instance directly
            using var message = new Message(payload);
            
            message.ContentType = "application/json";
            message.ContentEncoding = "utf-8";

            // In IoT Edge (ModuleClient), 'OutputName' is used for routing.
            // On Dev-PC (DeviceClient), messages are sent directly without an output name.
            string? outputName = _config.IoTHub.OutputName;

            if (!string.IsNullOrWhiteSpace(outputName) && _moduleClient is not null)
            {
                await _moduleClient.SendEventAsync(outputName, message, cancellationToken).ConfigureAwait(false);
            }
            else if (_moduleClient is not null)
            {
                await _moduleClient.SendEventAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else if (_deviceClient is not null)
            {
                await _deviceClient.SendEventAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                throw new InvalidOperationException("IoT Hub client is not initialized.");
            }

            _logger.LogInformation("Sent {Count} telemetry message(s) to IoT Hub", messages.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send telemetry to IoT Hub");
            throw;
        }
    }

    private static (DeviceClient? deviceClient, ModuleClient? moduleClient) CreateClient(AppConfiguration config)
    {
        var transport = Enum.Parse<TransportType>(config.IoTHub.TransportType ?? "Mqtt_Tcp_Only", ignoreCase: true);

        ModuleClient? moduleClient = null;
        if (!string.IsNullOrWhiteSpace(config.IoTHub.OutputName))
        {
            try
            {
                moduleClient = ModuleClient.CreateFromEnvironmentAsync().GetAwaiter().GetResult();
                moduleClient.OpenAsync().GetAwaiter().GetResult();
            }
            catch
            {
                moduleClient = null;
            }
        }

        if (moduleClient is not null)
        {
            return (null, moduleClient);
        }

        if (!string.IsNullOrWhiteSpace(config.IoTHub.OutputName))
        {
            try
            {
                moduleClient = ModuleClient.CreateFromConnectionString(config.IoTHub.ConnectionString, transport);
                moduleClient.OpenAsync().GetAwaiter().GetResult();
                return (null, moduleClient);
            }
            catch
            {
                moduleClient = null;
            }
        }

        if (string.IsNullOrWhiteSpace(config.IoTHub.ConnectionString))
        {
            throw new InvalidOperationException("IoTHub:ConnectionString is required when not running under IoT Edge.");
        }

        var deviceClient = DeviceClient.CreateFromConnectionString(config.IoTHub.ConnectionString, transport);
        deviceClient.OpenAsync().GetAwaiter().GetResult();
        return (deviceClient, null);
    }

    public async ValueTask DisposeAsync()
    {
        if (_moduleClient is not null)
        {
            await _moduleClient.DisposeAsync().ConfigureAwait(false);
        }
        else if (_deviceClient is not null)
        {
            await _deviceClient.DisposeAsync().ConfigureAwait(false);
        }
    }
}