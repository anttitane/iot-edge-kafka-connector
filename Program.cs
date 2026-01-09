using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Application.Processing;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Infrastructure.Messaging;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddEnvironmentVariables("IOT_EDGE_KAFKA_CONNECTOR_");
builder.Services.Configure<AppConfiguration>(builder.Configuration);

builder.Services.AddSingleton<IMessageProcessor, PassthroughMessageProcessor>();
builder.Services.AddHostedService<KafkaConsumerService>();

var host = builder.Build();
host.Run();
