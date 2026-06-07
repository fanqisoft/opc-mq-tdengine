namespace OpcMqTdengine.Config;

public class IotOptions
{
    public const string Position = "Iot";

    public OpcUaOptions OpcUa { get; set; } = new();
    public MqttOptions Mqtt { get; set; } = new();
    public TdengineOptions Tdengine { get; set; } = new();
}

public class OpcUaOptions
{
    public string ServerUrl { get; set; } = string.Empty;
    public string NodeId { get; set; } = string.Empty;
    public string ClientName { get; set; } = string.Empty;
    public bool HistoryBackfillEnabled { get; set; } = true;
    public int MaxLookbackHours { get; set; } = 24;
    public int SubscriptionLifetimeCount { get; set; } = 600;
    public int SubscriptionMaxKeepAliveCount { get; set; } = 10;
}

public class MqttOptions
{
    public string BrokerUrl { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string ProducerClientId { get; set; } = string.Empty;
    public string ConsumerClientId { get; set; } = string.Empty;
}

public class TdengineOptions
{
    public string Url { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 500;
    public int FlushIntervalMs { get; set; } = 1000;
    public int MaxQueueSize { get; set; } = 10000;
    public int RetryBaseDelayMs { get; set; } = 1000;
    public int RetryMaxDelayMs { get; set; } = 30000;
}
