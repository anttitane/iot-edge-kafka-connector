# iot-edge-kafka-connector
Azure IoT Edge module (and standalone worker) that ingests telemetry from Kafka, optionally parses and aggregates it, then forwards it to Azure IoT Hub and/or logs it locally.

## Features
- Kafka consumer with retry/backoff and configurable offset behavior.
- Parsing filters (source + node + measurement key) to emit only desired telemetry.
- Processing modes: passthrough or time-window aggregation (average with scale-aware rounding).
- Output options: log only, send only, or log + send.
- Batching either before outputs (passthrough mode) or after aggregation (aggregation mode).
- IoT Hub output supports IoT Edge routing and local DeviceClient testing.

## Architecture overview
1. Kafka consumer reads raw messages from a topic.
2. `TelemetryMessageParser` filters and extracts telemetry based on configured interests.
3. `IMessageProcessor` handles passthrough, batching, or aggregation.
4. `IMessageOutputService` logs and/or sends to IoT Hub.

## Configuration
Configuration is loaded from `appsettings.json`, `appsettings.Development.json`, and environment variables.
Environment variables use the prefix `IOT_EDGE_KAFKA_CONNECTOR_`.

### Kafka
```
Kafka:
	BootstrapServers: "localhost:9092"
	Topic: "telemetry"
	GroupId: "iot-edge-connector"
	EnableAutoCommit: true
	SessionTimeoutMs: 10000
	AutoOffsetReset: "Earliest"   # Earliest | Latest
	RetryBackoffSeconds: 5
```

### IoT Hub
```
IoTHub:
	ConnectionString: ""          # Device connection string for local dev
	TransportType: "Mqtt_Tcp_Only"# Mqtt_Tcp_Only | Mqtt_WebSocket_Only | Amqp_Tcp_Only | Amqp_WebSocket_Only
	OutputName: "output1"          # Optional; used for IoT Edge routing
```

Notes:
- When running on an IoT Edge device, the module connects via environment variables; `OutputName` controls EdgeHub routing.
- For local Dev-PC testing, set a device connection string and leave `OutputName` empty to send direct telemetry.

### Processing
```
Processing:
	Mode: "Passthrough"            # Passthrough | Aggregation
	AggregationWindowSeconds: 60
```

Aggregation averages numeric values per `(Source, NodeName, MeasurementType)` over the configured window. The output is rounded to the least precise scale seen in the window (for example, samples `1.2` and `1.26` produce `1.2`).

### Batching
```
Batching:
	IsEnabled: false
	MaxBatchSize: 10
	BatchIntervalSeconds: 30
```

Behavior:
- **Passthrough mode**: batching happens before outputs (buffered `TelemetryMessage`s).
- **Aggregation mode**: batching is applied to aggregated results before output.

### Output
```
Output:
	Action: "LogOnly"              # LogOnly | SendOnly | LogAndSend
	LogSerializedPayload: false
```

### Parsing
```
Parsing:
	Interests:
		- Source: "mqtt"
			NodeName: "sensor1"
			MeasurementKey: "temperature"
		- Source: "opcua"
			NodeName: "CPUUsagePercent"
			MeasurementKey: ""         # Empty means use the full "value" element
```

Rules:
- Only telemetry matching an interest’s `Source` and `NodeName` is emitted.
- If `MeasurementKey` is provided, the parser extracts `value.<MeasurementKey>`.
- If `MeasurementKey` is empty or null, the entire `value` element is used.

## Input message expectations
The parser expects JSON messages with fields like:
```
{
	"source": "mqtt",
	"nodeName": "sensor1",
	"timestamp": "2025-01-01T12:00:00Z",
	"value": {
		"temperature": 21.7,
		"humidity": 50
	}
}
```

Supported timestamp fields: `timestamp_iso` or `timestamp`. If missing, current time is used.

## Running locally
1. Set Kafka and IoT Hub settings in `appsettings.Development.json` or environment variables.
2. Ensure `IoTHub:ConnectionString` is set to a device connection string for local testing.
3. Run:
```
dotnet run
```

## Running as IoT Edge module
See `deployment.example.json` for a full deployment manifest example.
Key environment variables in Edge:
- `IoTHub__OutputName` (e.g., `output1`)
- Kafka settings under `Kafka__*`
- Parsing interests under `Parsing__Interests__N__*`

## Environment variable mapping
All configuration keys can be overridden using environment variables with prefix `IOT_EDGE_KAFKA_CONNECTOR_`.

Examples:
- `IOT_EDGE_KAFKA_CONNECTOR_Kafka__BootstrapServers=localhost:9092`
- `IOT_EDGE_KAFKA_CONNECTOR_Processing__Mode=Aggregation`
- `IOT_EDGE_KAFKA_CONNECTOR_Parsing__Interests__0__Source=mqtt`

## Tests
```
dotnet test
```

## License
See [LICENSE](LICENSE).
