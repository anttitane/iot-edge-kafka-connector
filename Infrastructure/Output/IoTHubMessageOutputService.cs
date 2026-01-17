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
/// Service responsible for sending telemetry messages to Azure IoT Hub.
/// Supports both IoT Edge (ModuleClient) and standalone (DeviceClient) modes.
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
        
        (_deviceClient, _moduleClient) = InitializeClient();
    }

    private (DeviceClient?, ModuleClient?) InitializeClient()
    {
        var transport = Enum.Parse<TransportType>(_config.IoTHub.TransportType ?? "Mqtt_Tcp_Only", ignoreCase: true);

        // 1. Try to detect IoT Edge environment (via IOTEDGE_WORKLOADURI environment variable)
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("IOTEDGE_WORKLOADURI")))
        {
            try
            {
                _logger.LogInformation("IoT Edge environment detected. Initializing ModuleClient...");
                var moduleClient = ModuleClient.CreateFromEnvironmentAsync(transport).GetAwaiter().GetResult();
                moduleClient.OpenAsync().GetAwaiter().GetResult();
                return (null, moduleClient);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to initialize ModuleClient from environment. Falling back to ConnectionString logic.");
            }
        }

        // 2. If not in Edge or initialization failed, a ConnectionString is mandatory
        if (string.IsNullOrWhiteSpace(_config.IoTHub.ConnectionString))
        {
            throw new InvalidOperationException("IoT Hub ConnectionString is missing. It is required for local debugging or when IoT Edge environment is not detected.");
        }

        // 3. Determine whether to create DeviceClient or ModuleClient based on the ConnectionString content
        // Typically, if "DeviceId=" is present, it's a DeviceClient identity
        if (_config.IoTHub.ConnectionString.Contains("DeviceId=", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogInformation("Initializing DeviceClient from ConnectionString...");
            var deviceClient = DeviceClient.CreateFromConnectionString(_config.IoTHub.ConnectionString, transport);
            deviceClient.OpenAsync().GetAwaiter().GetResult();
            return (deviceClient, null);
        }
        else
        {
            _logger.LogInformation("Initializing ModuleClient from ConnectionString...");
            var moduleClient = ModuleClient.CreateFromConnectionString(_config.IoTHub.ConnectionString, transport);
            moduleClient.OpenAsync().GetAwaiter().GetResult();
            return (null, moduleClient);
        }
    }

    public async Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.Count == 0) return;

        // Serialize the telemetry batch to JSON
        byte[] payload = messages.Count == 1
            ? JsonSerializer.SerializeToUtf8Bytes(messages.First(), _jsonOptions)
            : JsonSerializer.SerializeToUtf8Bytes(messages, _jsonOptions);

        try
        {
            // Wrap payload in an IoT Hub Message object with proper metadata
            using var message = new Message(payload)
            {
                ContentType = "application/json",
                ContentEncoding = "utf-8"
            };

            string? outputName = _config.IoTHub.OutputName;

            // Route the message based on the initialized client type
            if (_moduleClient is not null)
            {
                // In IoT Edge, we can use specific output names for routing within EdgeHub
                if (!string.IsNullOrWhiteSpace(outputName))
                    await _moduleClient.SendEventAsync(outputName, message, cancellationToken).ConfigureAwait(false);
                else
                    await _moduleClient.SendEventAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else if (_deviceClient is not null)
            {
                // Standard direct telemetry to IoT Hub
                await _deviceClient.SendEventAsync(message, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("Successfully sent {Count} telemetry message(s) to IoT Hub", messages.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send telemetry to IoT Hub");
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Graceful cleanup of SDK clients
        if (_moduleClient is not null) await _moduleClient.DisposeAsync().ConfigureAwait(false);
        if (_deviceClient is not null) await _deviceClient.DisposeAsync().ConfigureAwait(false);
        
        _logger.LogInformation("IoT Hub clients disposed.");
    }
}