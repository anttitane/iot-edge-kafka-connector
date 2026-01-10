using System.Linq;
using System.Text.Json;
using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Domain.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IotEdgeKafkaConnector.Infrastructure.Output;

/// <summary>
/// Sends telemetry messages to IoT Hub/Edge module client via reflection to stay resilient to SDK changes.
/// </summary>
public sealed class IoTHubMessageOutputService(IOptions<AppConfiguration> config, ILogger<IoTHubMessageOutputService> logger) : IMessageOutputService, IAsyncDisposable
{
    private readonly dynamic _moduleClient = CreateModuleClient(config.Value);
    private readonly ILogger<IoTHubMessageOutputService> _logger = logger;
    private readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = true };

    public async Task SendAsync(IReadOnlyCollection<TelemetryMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }

        var topic = string.Empty;
        var payload = messages.Count == 1
            ? JsonSerializer.SerializeToUtf8Bytes(messages.First(), _jsonOptions)
            : JsonSerializer.SerializeToUtf8Bytes(messages, _jsonOptions);

        try
        {
            await ((dynamic)_moduleClient).SendEventAsync(topic, payload, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Sent {Count} telemetry message(s) to IoT Hub", messages.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send telemetry to IoT Hub");
            throw;
        }
    }

    private static dynamic CreateModuleClient(AppConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.IoTHub.ConnectionString))
        {
            throw new InvalidOperationException("IoTHub:ConnectionString is required when Output:Action includes sending.");
        }

        var clientType = Type.GetType("Microsoft.Azure.Devices.Client.IotHubModuleClient, Microsoft.Azure.Devices.Client")
                       ?? Type.GetType("Microsoft.Azure.Devices.Client.ModuleClient, Microsoft.Azure.Devices.Client")
                       ?? throw new InvalidOperationException("IoT Hub module client type not found in Microsoft.Azure.Devices.Client");

        var transportType = Type.GetType("Microsoft.Azure.Devices.Client.TransportType, Microsoft.Azure.Devices.Client")
                          ?? Type.GetType("Microsoft.Azure.Devices.Client.IotHubTransportProtocol, Microsoft.Azure.Devices.Client")
                          ?? throw new InvalidOperationException("Transport type not found in Microsoft.Azure.Devices.Client");

        object transport = Enum.Parse(transportType, config.IoTHub.TransportType ?? "Mqtt_Tcp_Only", ignoreCase: true);

        var createMethod = clientType.GetMethod("CreateFromConnectionString", new[] { typeof(string), transportType })
                         ?? clientType.GetMethod("CreateFromConnectionString", new[] { typeof(string) })
                         ?? throw new InvalidOperationException("CreateFromConnectionString overload not found on IoT Hub module client");

        var client = createMethod.GetParameters().Length == 2
            ? createMethod.Invoke(null, new[] { config.IoTHub.ConnectionString, transport })
            : createMethod.Invoke(null, new object[] { config.IoTHub.ConnectionString! });

        var openMethod = clientType.GetMethod("OpenAsync") ?? clientType.GetMethod("ConnectAsync");
        if (openMethod is not null)
        {
            var task = openMethod.Invoke(client, null) as Task;
            task?.GetAwaiter().GetResult();
        }

        return client ?? throw new InvalidOperationException("Failed to create IoT Hub module client instance");
    }

    public async ValueTask DisposeAsync()
    {
        switch (_moduleClient)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }
}
