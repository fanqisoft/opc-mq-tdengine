package cn.coreqi.opcmq.opcua;

import cn.coreqi.opcmq.config.IotProperties;
import cn.coreqi.opcmq.config.MqttConfig;
import cn.coreqi.opcmq.writer.TdengineWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.HistoryData;
import org.eclipse.milo.opcua.stack.core.types.structured.HistoryReadDetails;
import org.eclipse.milo.opcua.stack.core.types.structured.HistoryReadResult;
import org.eclipse.milo.opcua.stack.core.types.structured.HistoryReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadRawModifiedDetails;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte;

@Slf4j
@Component
public class OpcUaCollector implements CommandLineRunner {

    @Autowired
    private TdengineWriter tdengineWriter;

    @Autowired
    private IotProperties iotProperties;

    @Autowired
    private MqttConfig.MqttGateway mqttGateway;

    @Autowired
    private ObjectMapper objectMapper;

    private OpcUaClient client;

    @Override
    public void run(String... args) {
        log.info("启动 OPC UA 采集器...");
        connectAndSubscribe();
    }

    private void connectAndSubscribe() {
        CompletableFuture.runAsync(() -> {
            boolean connected = false;
            while (!connected) {
                try {
                    log.info("尝试连接到 OPC UA 服务器: {}", iotProperties.getOpcua().getServerUrl());
                    
                    // 1. 发现端点并配置客户端
                    List<EndpointDescription> endpoints = DiscoveryClient.getEndpoints(iotProperties.getOpcua().getServerUrl()).get();
                    EndpointDescription endpoint = endpoints.stream()
                            .filter(e -> e.getSecurityPolicyUri().equals("http://opcfoundation.org/UA/SecurityPolicy#None"))
                            .findFirst()
                            .orElse(endpoints.get(0));

                    OpcUaClientConfigBuilder config = new OpcUaClientConfigBuilder();
                    config.setEndpoint(endpoint)
                            .setApplicationName(LocalizedText.english(iotProperties.getOpcua().getClientName()))
                            .setRequestTimeout(uint(5000));

                    client = OpcUaClient.create(config.build());
                    client.connect().get();
                    log.info("OPC UA 客户端连接成功!");

                    // 1.5 历史数据自动补数
                    backfillHistoryDataIfEnabled();

                    // 2. 订阅节点
                    subscribeToNode();
                    connected = true;
                } catch (Exception e) {
                    log.error("OPC UA 连接或订阅失败，10秒后重试: {}", e.getMessage());
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
    }

    private void backfillHistoryDataIfEnabled() {
        if (!iotProperties.getOpcua().isHistoryBackfillEnabled()) {
            log.info("历史数据自动补回功能已禁用。");
            return;
        }

        log.info("开始检查并执行 OPC UA 历史数据自动补数...");
        String deviceName = "simulation_server";
        String metricName = "counter";

        // 1. 查询数据库中最新记录的时间戳
        Long latestTs = tdengineWriter.getLatestTimestamp(deviceName, metricName);
        long startTimeMs;
        long endTimeMs = System.currentTimeMillis();

        if (latestTs != null) {
            startTimeMs = latestTs + 1; // 从最新一条数据的下一毫秒开始
            log.info("检测到数据库中最新数据时间戳: {} (millis={})，将拉取该时间点之后的历史数据。", new java.util.Date(latestTs), latestTs);
        } else {
            // 如果库中无数据，回溯 maxLookbackHours 小时
            int lookbackHours = iotProperties.getOpcua().getMaxLookbackHours();
            startTimeMs = endTimeMs - TimeUnit.HOURS.toMillis(lookbackHours);
            log.info("数据库中无历史数据，将回溯最近 {} 小时的数据作为历史补充。", lookbackHours);
        }

        if (startTimeMs >= endTimeMs) {
            log.info("起止时间不满足补数条件 (startTime >= endTime)，跳过补数。");
            return;
        }

        try {
            NodeId nodeId = NodeId.parse(iotProperties.getOpcua().getNodeId());
            // 2. 构造历史读取参数
            ReadRawModifiedDetails readDetails = new ReadRawModifiedDetails(
                    false, // 读取原始数据
                    new DateTime(new java.util.Date(startTimeMs)),
                    new DateTime(new java.util.Date(endTimeMs)),
                    uint(1000), // 每页最大条数
                    true  // 返回边界
            );

            HistoryReadValueId valueId = new HistoryReadValueId(
                    nodeId,
                    null,
                    null,
                    null // 初始 ContinuationPoint 为 null
            );

            int totalCount = 0;
            boolean hasMore = true;
            org.eclipse.milo.opcua.stack.core.types.builtin.ByteString continuationPoint = null;

            while (hasMore) {
                if (continuationPoint != null) {
                    valueId = new HistoryReadValueId(
                            nodeId,
                            null,
                            null,
                            continuationPoint
                    );
                }

                org.eclipse.milo.opcua.stack.core.types.structured.HistoryReadResponse response = client.historyRead(
                        readDetails,
                        TimestampsToReturn.Both,
                        false,
                        Collections.singletonList(valueId)
                ).get();

                if (response == null || response.getResults() == null || response.getResults().length == 0) {
                    break;
                }

                HistoryReadResult result = response.getResults()[0];
                if (result.getStatusCode().isBad()) {
                    log.warn("读取历史数据返回异常状态码: {}", result.getStatusCode());
                    break;
                }

                HistoryData historyData = (HistoryData) result.getHistoryData().decode(
                        client.getStaticSerializationContext()
                );

                DataValue[] dataValues = historyData.getDataValues();
                if (dataValues != null && dataValues.length > 0) {
                    for (DataValue value : dataValues) {
                        if (value.getValue().getValue() == null) {
                            continue;
                        }

                        Object val = value.getValue().getValue();
                        double doubleVal;
                        if (val instanceof Number) {
                            doubleVal = ((Number) val).doubleValue();
                        } else {
                            try {
                                doubleVal = Double.parseDouble(val.toString());
                            } catch (NumberFormatException e) {
                                continue;
                            }
                        }

                        // 历史数据的时间戳
                        long timestamp = value.getSourceTime() != null ? 
                                value.getSourceTime().getJavaTime() : 
                                (value.getServerTime() != null ? value.getServerTime().getJavaTime() : System.currentTimeMillis());

                        // 直接调用 tdengineWriter 写入数据库，保证效率和数据的即时性
                        tdengineWriter.write(timestamp, doubleVal, deviceName, metricName);
                        totalCount++;
                    }
                }

                continuationPoint = result.getContinuationPoint();
                hasMore = continuationPoint != null && continuationPoint.length() > 0;
            }

            // 冲刷一下写入器缓冲区，保证补回的数据立即入库
            tdengineWriter.flush();
            log.info("历史数据自动补数完成，共成功补充写入 {} 条数据。", totalCount);

        } catch (Exception e) {
            log.error("执行历史数据补数发生异常（若 OPC UA 服务端不支持 HA 历史服务属正常现象，将直接开启实时订阅）: ", e);
        }
    }

    private void subscribeToNode() throws InterruptedException, ExecutionException {
        // 创建订阅，设置自定义的生命周期参数增强订阅耐久性
        UaSubscription subscription = client.getSubscriptionManager().createSubscription(
                1000.0, // 采样周期 1000ms
                uint(iotProperties.getOpcua().getSubscriptionLifetimeCount()),
                uint(iotProperties.getOpcua().getSubscriptionMaxKeepAliveCount()),
                uint(0),
                true,
                ubyte(0)
        ).get();

        NodeId nodeId = NodeId.parse(iotProperties.getOpcua().getNodeId());
        ReadValueId readValueId = new ReadValueId(
                nodeId,
                AttributeId.Value.uid(),
                null,
                null
        );

        UInteger clientHandle = subscription.nextClientHandle();
        MonitoringParameters parameters = new MonitoringParameters(
                clientHandle,
                1000.0,     // 采样间隔
                null,       // 过滤器
                uint(10),   // 队列深度
                true        // 丢弃最老的数据
        );

        MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
                readValueId,
                MonitoringMode.Reporting,
                parameters
        );

        // 1. 创建时不传那个不靠谱的第三参数 Lambda
        List<UaMonitoredItem> items = subscription.createMonitoredItems(
                TimestampsToReturn.Both,
                Collections.singletonList(request)
        ).get();

        // 2. 拿到成功创建的实体后，显式进行事件消费绑定
        for (UaMonitoredItem item : items) {
            if (item.getStatusCode().isGood()) {
                log.info("监控项创建成功: NodeId={}", item.getReadValueId().getNodeId());
                // 核心：直接对已经存在的 item 对象设置值变化消费者
                item.setValueConsumer(this::onDataChanged);
            } else {
                log.error("创建监控项失败: NodeId={}, StatusCode={}",
                        item.getReadValueId().getNodeId(), item.getStatusCode());
            }
        }

        log.info("已成功订阅 OPC UA 节点: {}", iotProperties.getOpcua().getNodeId());
    }

    /**
     * 当监控的节点数据发生变化时回调
     */
    private void onDataChanged(UaMonitoredItem item, DataValue value) {
        // 只要进来了，不管对错，立刻先打印一条原始日志。如果这条能打出来，说明订阅没问题
        log.info("【收到原始变动通知】NodeId={}, RawValue={}", item.getReadValueId().getNodeId(), value.getValue());
        Object val = value.getValue().getValue();
        if (val == null) {
            return;
        }

        double doubleVal;
        if (val instanceof Number) {
            doubleVal = ((Number) val).doubleValue();
        } else {
            try {
                doubleVal = Double.parseDouble(val.toString());
            } catch (NumberFormatException e) {
                log.warn("无法解析的数值类型: {} = {}", val.getClass().getName(), val);
                return;
            }
        }

        // 使用服务器时间戳，若无则使用系统当前时间
        long timestamp = value.getServerTime() != null ? 
                value.getServerTime().getJavaTime() : System.currentTimeMillis();

        try {
            // 构造消息数据
            // 设备名称可根据节点解析或固定值，这里取 OPC UA 服务器端点或配置的 clientName
            String deviceName = "simulation_server";
            String metricName = "counter";
            
            TdengineWriter.MetricData metricData = new TdengineWriter.MetricData(
                    timestamp,
                    doubleVal,
                    deviceName,
                    metricName
            );

            String jsonPayload = objectMapper.writeValueAsString(metricData);
            log.info("采集到数据: {}，并发布到 MQTT Topic: {}", jsonPayload, iotProperties.getMqtt().getTopic());

            // 发送到 MQTT 队列
            mqttGateway.sendToMqtt(jsonPayload);
        } catch (Exception e) {
            log.error("处理 OPC UA 变化数据或发送至 MQTT 失败: ", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        if (client != null) {
            log.info("关闭 OPC UA 客户端连接...");
            client.disconnect();
        }
    }
}
