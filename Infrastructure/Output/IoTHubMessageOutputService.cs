using System.Linq;
using System.Reflection;
using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Infrastructure.Output;

/// <summary>
/// Sends telemetry messages to IoT Hub/Edge client via reflection to stay resilient to SDK changes.
/// Supports both DeviceClient (for Dev-PC) and ModuleClient (for IoT Edge).
/// </summary>
public sealed class IoTHubMessageOutputService : IMessageOutputService, IAsyncDisposable
{
    private readonly dynamic _client;
    private readonly ILogger<IoTHubMessageOutputService> _logger;
    private readonly AppConfiguration _config;
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = true };
    private readonly Type _messageType;

    public IoTHubMessageOutputService(IOptions<AppConfiguration> config, ILogger<IoTHubMessageOutputService> logger)
    {
        _logger = logger;
        _config = config.Value;
        
        // Retrieve Message type once for performance optimization
        _messageType = Type.GetType("Microsoft.Azure.Devices.Client.Message, Microsoft.Azure.Devices.Client") 
                       ?? throw new InvalidOperationException("IoT Hub Message type not found.");

        _client = CreateClient(_config);
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
            // Create Message instance via reflection to set ContentType and ContentEncoding
            using dynamic message = Activator.CreateInstance(_messageType, new object[] { payload }) 
                                    ?? throw new InvalidOperationException("Failed to create Message instance.");
            
            message.ContentType = "application/json";
            message.ContentEncoding = "utf-8";

            // In IoT Edge (ModuleClient), 'OutputName' is used for routing.
            // On Dev-PC (DeviceClient), messages are sent directly without an output name.
            string? outputName = _config.IoTHub.OutputName;

            if (string.IsNullOrWhiteSpace(outputName))
            {
                await _client.SendEventAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _client.SendEventAsync(outputName, message, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("Sent {Count} telemetry message(s) to IoT Hub", messages.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send telemetry to IoT Hub");
            throw;
        }
    }

    private static dynamic CreateClient(AppConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.IoTHub.ConnectionString))
        {
            throw new InvalidOperationException("IoTHub:ConnectionString is required.");
        }

        // Search for DeviceClient first (Dev-PC / Connection String), then ModuleClient (IoT Edge)
        var clientType = Type.GetType("Microsoft.Azure.Devices.Client.DeviceClient, Microsoft.Azure.Devices.Client")
                       ?? Type.GetType("Microsoft.Azure.Devices.Client.ModuleClient, Microsoft.Azure.Devices.Client")
                       ?? Type.GetType("Microsoft.Azure.Devices.Client.IotHubModuleClient, Microsoft.Azure.Devices.Client")
                       ?? throw new InvalidOperationException("IoT Hub client type not found in Microsoft.Azure.Devices.Client");

        var transportType = Type.GetType("Microsoft.Azure.Devices.Client.TransportType, Microsoft.Azure.Devices.Client")
                          ?? Type.GetType("Microsoft.Azure.Devices.Client.IotHubTransportProtocol, Microsoft.Azure.Devices.Client")
                          ?? throw new InvalidOperationException("Transport type not found.");

        object transport = Enum.Parse(transportType, config.IoTHub.TransportType ?? "Mqtt_Tcp_Only", ignoreCase: true);

        // Retrieve CreateFromConnectionString method
        var createMethod = clientType.GetMethod("CreateFromConnectionString", new[] { typeof(string), transportType })
                         ?? clientType.GetMethod("CreateFromConnectionString", new[] { typeof(string) })
                         ?? throw new InvalidOperationException("CreateFromConnectionString overload not found.");

        var client = createMethod.GetParameters().Length == 2
            ? createMethod.Invoke(null, [config.IoTHub.ConnectionString, transport])
            : createMethod.Invoke(null, [config.IoTHub.ConnectionString!]);

        // Open connection
        var openMethod = clientType.GetMethod("OpenAsync", Array.Empty<Type>()) 
                         ?? clientType.GetMethod("ConnectAsync", Array.Empty<Type>());
        
        if (openMethod is not null && client is not null)
        {
            var task = openMethod.Invoke(client, null) as Task;
            task?.GetAwaiter().GetResult();
        }

        return client ?? throw new InvalidOperationException("Failed to create IoT Hub client instance");
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        }
        else if (_client is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}