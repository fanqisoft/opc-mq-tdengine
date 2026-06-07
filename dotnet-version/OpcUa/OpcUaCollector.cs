using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using OpcMqTdengine.Config;
using OpcMqTdengine.Mqtt;
using OpcMqTdengine.Writer;
using System.Text.Json;
using static System.Net.Mime.MediaTypeNames;

namespace OpcMqTdengine.OpcUa;

public class OpcUaCollector : BackgroundService
{
    private readonly IotOptions _options;
    private readonly MqttService _mqttService;
    private readonly TdengineWriter _tdengineWriter;
    private readonly ILogger<OpcUaCollector> _logger;
    private ISession? _session;

    public OpcUaCollector(
        IOptions<IotOptions> options,
        MqttService mqttService,
        TdengineWriter tdengineWriter,
        ILogger<OpcUaCollector> logger)
    {
        _options = options.Value;
        _mqttService = mqttService;
        _tdengineWriter = tdengineWriter;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("启动 OPC UA 采集器...");
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("尝试连接到 OPC UA 服务器: {ServerUrl}", _options.OpcUa.ServerUrl);
                var config = await CreateOpcUaConfiguration();
                var selectedEndpoint = await CoreClientUtils.SelectEndpointAsync(
                    config,
                    _options.OpcUa.ServerUrl,
                    false,
                    15000,
                    stoppingToken);
                var endpointConfiguration = EndpointConfiguration.Create(config);
                var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
                var sessionFactory = new DefaultSessionFactory();

                _session = await sessionFactory.CreateAsync(
                    config,
                    endpoint,
                    false,
                    false,
                    _options.OpcUa.ClientName,
                    60000,
                    new UserIdentity(new AnonymousIdentityToken()),
                    null,
                    stoppingToken
                );

                _logger.LogInformation("OPC UA 客户端连接成功!");

                // 1.5 历史数据自动补数
                await BackfillHistoryDataIfEnabledAsync(stoppingToken);

                // 2. 订阅节点
                await SubscribeToNode();

                // Wait here until cancelled or session closed
                while (!stoppingToken.IsCancellationRequested && _session.Connected)
                {
                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "OPC UA 连接或订阅失败，10秒后重试");
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }
        }
    }

    private async Task<ApplicationConfiguration> CreateOpcUaConfiguration()
    {
        // 定义证书存储的基础路径（这里直接放在程序运行目录下的 CertificateStores 文件夹中，避免权限问题）
        string certStorePath = Path.Combine(AppContext.BaseDirectory, "CertificateStores", "MachineDefault");

        var config = new ApplicationConfiguration
        {
            ApplicationName = _options.OpcUa.ClientName,
            ApplicationType = ApplicationType.Client,
            ApplicationUri = $"urn:{System.Net.Dns.GetHostName()}:{_options.OpcUa.ClientName}", // 显式指定 URI 避免部分库校验失败
            SecurityConfiguration = new SecurityConfiguration
            {
                // 1. 核心修复：指定应用证书的本地存储方式和路径
                ApplicationCertificate = new CertificateIdentifier
                {
                    StoreType = "Directory",
                    StorePath = certStorePath,
                    SubjectName = $"CN={_options.OpcUa.ClientName}, O=YourCompany"
                },

                // 可选：指定信任和拒绝证书的存储路径，完善安全链路
                TrustedPeerCertificates = new CertificateTrustList { StoreType = "Directory", StorePath = Path.Combine(AppContext.BaseDirectory, "CertificateStores", "UA Applications") },
                TrustedIssuerCertificates = new CertificateTrustList { StoreType = "Directory", StorePath = Path.Combine(AppContext.BaseDirectory, "CertificateStores", "UA Certificate Authorities") },
                RejectedCertificateStore = new CertificateStoreIdentifier { StoreType = "Directory", StorePath = Path.Combine(AppContext.BaseDirectory, "CertificateStores", "RejectedCertificates") },

                AutoAcceptUntrustedCertificates = true,
                RejectSHA1SignedCertificates = false
            },
            TransportConfigurations = new TransportConfigurationCollection(),
            TransportQuotas = new TransportQuotas { OperationTimeout = 15000 },
            ClientConfiguration = new ClientConfiguration { DefaultSessionTimeout = 60000 }
        };
        
        await config.ValidateAsync(ApplicationType.Client);
        if (config.SecurityConfiguration.AutoAcceptUntrustedCertificates)
        {
            config.CertificateValidator.CertificateValidation += (s, e) => { e.Accept = true; };
        }

        return config;
    }

    private async Task BackfillHistoryDataIfEnabledAsync(CancellationToken cancellationToken)
    {
        if (!_options.OpcUa.HistoryBackfillEnabled)
        {
            _logger.LogInformation("历史数据自动补回功能已禁用。");
            return;
        }

        _logger.LogInformation("开始检查并执行 OPC UA 历史数据自动补数...");
        string deviceName = "simulation_server";
        string metricName = "counter";

        // 1. 查询数据库中最新记录的时间戳
        long? latestTs = _tdengineWriter.GetLatestTimestamp(deviceName, metricName);
        DateTime startTime;
        DateTime endTime = DateTime.UtcNow;

        if (latestTs.HasValue)
        {
            startTime = DateTimeOffset.FromUnixTimeMilliseconds(latestTs.Value + 1).UtcDateTime;
            _logger.LogInformation("检测到数据库中最新数据时间戳: {Latest}，将拉取该时间点之后的历史数据。", startTime.ToLocalTime());
        }
        else
        {
            startTime = endTime.AddHours(-_options.OpcUa.MaxLookbackHours);
            _logger.LogInformation("数据库中无历史数据，将回溯最近 {Hours} 小时的数据作为历史补充。", _options.OpcUa.MaxLookbackHours);
        }

        if (startTime >= endTime)
        {
            _logger.LogInformation("起止时间不满足补数条件 (startTime >= endTime)，跳过补数。");
            return;
        }

        try
        {
            var nodeId = new NodeId(_options.OpcUa.NodeId);
            var readDetails = new ReadRawModifiedDetails
            {
                IsReadModified = false,
                StartTime = startTime,
                EndTime = endTime,
                NumValuesPerNode = 1000,
                ReturnBounds = true
            };

            var nodesToRead = new HistoryReadValueIdCollection
            {
                new HistoryReadValueId
                {
                    NodeId = nodeId,
                    IndexRange = null,
                    DataEncoding = null,
                    ContinuationPoint = null
                }
            };

            int totalCount = 0;
            bool hasMore = true;
            byte[]? continuationPoint = null;

            while (hasMore && _session != null)
            {
                if (continuationPoint != null)
                {
                    nodesToRead[0].ContinuationPoint = continuationPoint;
                }

                var response = await _session.HistoryReadAsync(
                    null,
                    new ExtensionObject(readDetails),
                    TimestampsToReturn.Both,
                    false,
                    nodesToRead,
                    cancellationToken
                );

                if (response == null || response.Results == null || response.Results.Count == 0)
                {
                    break;
                }

                var result = response.Results[0];
                if (StatusCode.IsBad(result.StatusCode))
                {
                    _logger.LogWarning("读取历史数据返回异常状态码: {Code}", result.StatusCode);
                    break;
                }

                var historyData = ExtensionObject.ToEncodeable(result.HistoryData) as HistoryData;
                if (historyData != null && historyData.DataValues.Count > 0)
                {
                    foreach (var value in historyData.DataValues)
                    {
                        if (value.Value == null) continue;

                        double doubleVal;
                        if (value.Value is IConvertible convertible)
                        {
                            doubleVal = convertible.ToDouble(null);
                        }
                        else
                        {
                            if (!double.TryParse(value.Value.ToString(), out doubleVal))
                            {
                                continue;
                            }
                        }

                        // Determine historical timestamp (use SourceTimestamp or ServerTimestamp or UtcNow)
                        var valTime = value.SourceTimestamp != DateTime.MinValue ? value.SourceTimestamp : 
                                      (value.ServerTimestamp != DateTime.MinValue ? value.ServerTimestamp : DateTime.UtcNow);
                        
                        var ts = new DateTimeOffset(valTime).ToUnixTimeMilliseconds();

                        _tdengineWriter.Write(ts, doubleVal, deviceName, metricName);
                        totalCount++;
                    }
                }

                continuationPoint = result.ContinuationPoint;
                hasMore = continuationPoint != null && continuationPoint.Length > 0;
            }

            _tdengineWriter.Flush();
            _logger.LogInformation("历史数据自动补数完成，共成功补充写入 {Count} 条数据。", totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "执行历史数据补数发生异常（若 OPC UA 服务端不支持 HA 历史服务属正常现象，将直接开启实时订阅）");
        }
    }

    private async Task SubscribeToNode()
    {
        if (_session == null) return;

        // 创建订阅，设置自定义的生命周期参数增强订阅耐久性
        var subscription = new Subscription(_session.DefaultSubscription)
        {
            PublishingInterval = 1000,
            LifetimeCount = (uint)_options.OpcUa.SubscriptionLifetimeCount,
            KeepAliveCount = (uint)_options.OpcUa.SubscriptionMaxKeepAliveCount,
            PublishingEnabled = true
        };

        var monitoredItem = new MonitoredItem(subscription.DefaultItem)
        {
            StartNodeId = new NodeId(_options.OpcUa.NodeId),
            AttributeId = Attributes.Value,
            DisplayName = "CounterItem",
            SamplingInterval = 1000,
            QueueSize = 10,
            DiscardOldest = true
        };

        monitoredItem.Notification += OnDataChanged;
        subscription.AddItem(monitoredItem);
        
        _session.AddSubscription(subscription);
        await subscription.CreateAsync();

        _logger.LogInformation("已成功订阅 OPC UA 节点: {NodeId}", _options.OpcUa.NodeId);
    }

    private void OnDataChanged(MonitoredItem monitoredItem, MonitoredItemNotificationEventArgs e)
    {
        foreach (var value in monitoredItem.DequeueValues())
        {
            if (value.Value == null) continue;

            _logger.LogInformation("【收到原始变动通知】NodeId={NodeId}, RawValue={Value}", 
                monitoredItem.StartNodeId, value.Value);

            double doubleVal;
            if (value.Value is IConvertible convertible)
            {
                doubleVal = convertible.ToDouble(null);
            }
            else
            {
                if (!double.TryParse(value.Value.ToString(), out doubleVal))
                {
                    _logger.LogWarning("无法解析的数值类型: {Type} = {Value}", 
                        value.Value.GetType().Name, value.Value);
                    continue;
                }
            }

            // SourceTimestamp or ServerTimestamp or UtcNow
            var valTime = value.ServerTimestamp != DateTime.MinValue ? value.ServerTimestamp : 
                          (value.SourceTimestamp != DateTime.MinValue ? value.SourceTimestamp : DateTime.UtcNow);
            
            var ts = new DateTimeOffset(valTime).ToUnixTimeMilliseconds();

            try
            {
                var metricData = new
                {
                    timestamp = ts,
                    value = doubleVal,
                    deviceName = "simulation_server",
                    metricName = "counter"
                };

                string jsonPayload = JsonSerializer.Serialize(metricData);
                _logger.LogInformation("采集到数据: {Payload}，并发布到 MQTT Topic: {Topic}", 
                    jsonPayload, _options.Mqtt.Topic);

                // Publish async (fire and forget pattern or wait, using Task.Run to not block OPC UA callback thread)
                Task.Run(() => _mqttService.PublishAsync(jsonPayload));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "处理 OPC UA 变化数据并发布至 MQTT 失败");
            }
        }
    }

    public override void Dispose()
    {
        if (_session != null)
        {
            _logger.LogInformation("关闭 OPC UA 客户端连接...");
            _session.CloseAsync();
        }
        base.Dispose();
    }
}
