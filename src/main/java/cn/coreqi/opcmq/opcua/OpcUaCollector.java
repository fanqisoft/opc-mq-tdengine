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

@Slf4j
@Component
public class OpcUaCollector implements CommandLineRunner {

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

    private void subscribeToNode() throws InterruptedException, ExecutionException {
        // 创建订阅，采样周期设置为 1000ms
        UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

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

        /* * ======= 🔧 修改这里的挂载逻辑 =======
         */
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
