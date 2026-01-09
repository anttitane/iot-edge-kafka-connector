using IotEdgeKafkaConnector.Application.Configuration;
using IotEdgeKafkaConnector.Application.Processing;
using IotEdgeKafkaConnector.Domain.Interfaces;
using IotEdgeKafkaConnector.Infrastructure.Messaging;
using IotEdgeKafkaConnector.Infrastructure.Output;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddEnvironmentVariables("IOT_EDGE_KAFKA_CONNECTOR_");
builder.Services.Configure<AppConfiguration>(builder.Configuration);
builder.Services.Configure<BatchingOptions>(builder.Configuration.GetSection("Batching"));
builder.Services.Configure<OutputOptions>(builder.Configuration.GetSection("Output"));

builder.Services.AddSingleton<LoggingMessageOutputService>();
builder.Services.AddSingleton<IoTHubMessageOutputService>();
builder.Services.AddSingleton<IMessageOutputService>(sp =>
{
	var outputOptions = sp.GetRequiredService<IOptions<OutputOptions>>().Value;
	var outputs = new List<IMessageOutputService>();

	if (outputOptions.Action is OutputAction.LogOnly or OutputAction.LogAndSend)
	{
		outputs.Add(sp.GetRequiredService<LoggingMessageOutputService>());
	}

	if (outputOptions.Action is OutputAction.SendOnly or OutputAction.LogAndSend)
	{
		outputs.Add(sp.GetRequiredService<IoTHubMessageOutputService>());
	}

	return outputs.Count switch
	{
		0 => sp.GetRequiredService<LoggingMessageOutputService>(),
		1 => outputs[0],
		_ => new CompositeMessageOutputService(outputs, sp.GetRequiredService<ILogger<CompositeMessageOutputService>>())
	};
});

builder.Services.AddSingleton<IMessageProcessor>(sp =>
{
	var appConfig = sp.GetRequiredService<IOptions<AppConfiguration>>().Value;
	var batchingOptions = sp.GetRequiredService<IOptions<BatchingOptions>>();
	var outputService = sp.GetRequiredService<IMessageOutputService>();
	var logger = sp.GetRequiredService<ILogger<BatchingMessageProcessor>>();

	if (appConfig.Batching.IsEnabled)
	{
		return new BatchingMessageProcessor(outputService, batchingOptions, logger);
	}

	return new PassthroughMessageProcessor(outputService);
});
builder.Services.AddHostedService<KafkaConsumerService>();

var host = builder.Build();
host.Run();
