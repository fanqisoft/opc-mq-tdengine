using System.Text;
using System.Text.Json;
using System.Buffers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;
using OpcMqTdengine.Config;
using OpcMqTdengine.Writer;

namespace OpcMqTdengine.Mqtt;

public class MqttService : IHostedService
{
    private readonly IMqttClient _publisherClient;
    private readonly IMqttClient _consumerClient;
    private readonly IotOptions _options;
    private readonly TdengineWriter _tdengineWriter;
    private readonly ILogger<MqttService> _logger;
    private readonly SemaphoreSlim _publisherReconnectLock = new(1, 1);
    private readonly SemaphoreSlim _consumerReconnectLock = new(1, 1);
    private MqttClientOptions? _publisherOptions;
    private MqttClientOptions? _consumerOptions;
    private CancellationToken _stoppingToken;

    public MqttService(IOptions<IotOptions> options, TdengineWriter tdengineWriter, ILogger<MqttService> logger)
    {
        _options = options.Value;
        _tdengineWriter = tdengineWriter;
        _logger = logger;

        var factory = new MqttClientFactory();
        _publisherClient = factory.CreateMqttClient();
        _consumerClient = factory.CreateMqttClient();

        ConfigureClients();
    }

    private void ConfigureClients()
    {
        _publisherOptions = CreateClientOptions(_options.Mqtt.ProducerClientId, cleanSession: true);
        _consumerOptions = CreateClientOptions(_options.Mqtt.ConsumerClientId, cleanSession: false);

        _consumerClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;

        _publisherClient.DisconnectedAsync += async _ =>
        {
            if (_stoppingToken.IsCancellationRequested) return;

            _logger.LogWarning("MQTT publisher disconnected. Reconnecting in 5 seconds...");
            await Task.Delay(TimeSpan.FromSeconds(5), _stoppingToken);
            await ConnectPublisherAsync(_stoppingToken);
        };

        _consumerClient.DisconnectedAsync += async _ =>
        {
            if (_stoppingToken.IsCancellationRequested) return;

            _logger.LogWarning("MQTT consumer disconnected. Reconnecting in 5 seconds...");
            await Task.Delay(TimeSpan.FromSeconds(5), _stoppingToken);
            await ConnectConsumerAndSubscribeAsync(_stoppingToken);
        };
    }

    private MqttClientOptions CreateClientOptions(string clientId, bool cleanSession)
    {
        var builder = new MqttClientOptionsBuilder()
            .WithConnectionUri(_options.Mqtt.BrokerUrl)
            .WithClientId(clientId)
            .WithCleanSession(cleanSession);

        if (!string.IsNullOrEmpty(_options.Mqtt.Username))
        {
            builder.WithCredentials(_options.Mqtt.Username, _options.Mqtt.Password);
        }

        return builder.Build();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _stoppingToken = cancellationToken;
        _logger.LogInformation("Starting MQTT publisher and consumer clients...");

        await ConnectPublisherAsync(cancellationToken);
        await ConnectConsumerAndSubscribeAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping MQTT clients...");

        if (_consumerClient.IsConnected)
        {
            await _consumerClient.DisconnectAsync(cancellationToken: cancellationToken);
        }

        if (_publisherClient.IsConnected)
        {
            await _publisherClient.DisconnectAsync(cancellationToken: cancellationToken);
        }
    }

    public Task PublishAsync(string payload)
    {
        return PublishAsync(payload, _options.Mqtt.Topic);
    }

    public async Task PublishAsync(string payload, string topic)
    {
        if (!_publisherClient.IsConnected)
        {
            await ConnectPublisherAsync(_stoppingToken);
        }

        if (!_publisherClient.IsConnected)
        {
            _logger.LogWarning("MQTT publisher is disconnected. Message was not published: {Payload}", payload);
            return;
        }

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        await _publisherClient.PublishAsync(message, _stoppingToken);
    }

    private async Task ConnectPublisherAsync(CancellationToken cancellationToken)
    {
        if (_publisherClient.IsConnected || _publisherOptions == null) return;

        await _publisherReconnectLock.WaitAsync(cancellationToken);
        try
        {
            if (_publisherClient.IsConnected) return;

            await _publisherClient.ConnectAsync(_publisherOptions, cancellationToken);
            _logger.LogInformation("MQTT publisher connected with client id {ClientId}.", _options.Mqtt.ProducerClientId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MQTT publisher connection failed.");
        }
        finally
        {
            _publisherReconnectLock.Release();
        }
    }

    private async Task ConnectConsumerAndSubscribeAsync(CancellationToken cancellationToken)
    {
        if (_consumerOptions == null) return;

        await _consumerReconnectLock.WaitAsync(cancellationToken);
        try
        {
            if (!_consumerClient.IsConnected)
            {
                await _consumerClient.ConnectAsync(_consumerOptions, cancellationToken);
                _logger.LogInformation("MQTT consumer connected with client id {ClientId}.", _options.Mqtt.ConsumerClientId);
            }

            var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(f => f.WithTopic(_options.Mqtt.Topic).WithAtLeastOnceQoS())
                .Build();

            await _consumerClient.SubscribeAsync(subscribeOptions, cancellationToken);
            _logger.LogInformation("MQTT consumer subscribed to topic {Topic}.", _options.Mqtt.Topic);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MQTT consumer connection or subscription failed.");
        }
        finally
        {
            _consumerReconnectLock.Release();
        }
    }

    private Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        var payloadSequence = e.ApplicationMessage.Payload;
        var payload = payloadSequence.IsSingleSegment
            ? Encoding.UTF8.GetString(payloadSequence.FirstSpan)
            : Encoding.UTF8.GetString(BuffersExtensions.ToArray(payloadSequence));
        try
        {
            var data = JsonSerializer.Deserialize<TdengineWriter.MetricData>(payload, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            if (data != null)
            {
                _tdengineWriter.Write(data.Timestamp, data.Value, data.DeviceName, data.MetricName);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse MQTT JSON payload: {Payload}", payload);
        }

        return Task.CompletedTask;
    }
}
