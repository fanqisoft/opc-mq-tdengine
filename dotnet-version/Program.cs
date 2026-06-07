using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpcMqTdengine.Config;
using OpcMqTdengine.Mqtt;
using OpcMqTdengine.OpcUa;
using OpcMqTdengine.Writer;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

// 1. Bind configuration section
builder.Services.Configure<IotOptions>(builder.Configuration.GetSection(IotOptions.Position));

// 2. Register typed HttpClient for TDengine REST Writer
builder.Services.AddHttpClient<TdengineWriter>();

// 3. Register MQTT Service as singleton and hosted service
builder.Services.AddSingleton<MqttService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<MqttService>());

// 4. Register OPC UA Collector background hosted service
builder.Services.AddHostedService<OpcUaCollector>();

var host = builder.Build();
await host.RunAsync();
