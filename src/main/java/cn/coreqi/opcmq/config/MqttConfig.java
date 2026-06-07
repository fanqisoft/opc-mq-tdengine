package cn.coreqi.opcmq.config;

import cn.coreqi.opcmq.writer.TdengineWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.Header;

@Slf4j
@Configuration
@IntegrationComponentScan
public class MqttConfig {

    @Autowired
    private IotProperties iotProperties;

    @Autowired
    private TdengineWriter tdengineWriter;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * MQTT 客户端工厂
     */
    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{iotProperties.getMqtt().getBrokerUrl()});
        if (iotProperties.getMqtt().getUsername() != null && !iotProperties.getMqtt().getUsername().isEmpty()) {
            options.setUserName(iotProperties.getMqtt().getUsername());
            options.setPassword(iotProperties.getMqtt().getPassword().toCharArray());
        }
        // 自动重连与清除会话设置，保障掉线重连后能继续订阅
        options.setAutomaticReconnect(true);
        options.setCleanSession(false);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(60);
        factory.setConnectionOptions(options);
        return factory;
    }

    // ================== MQTT 生产者 (发送端) 配置 ==================

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttOutboundChannel")
    public MessageHandler mqttOutbound() {
        MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler(
                iotProperties.getMqtt().getProducerClientId(),
                mqttClientFactory()
        );
        messageHandler.setAsync(true);
        messageHandler.setDefaultTopic(iotProperties.getMqtt().getTopic());
        messageHandler.setDefaultQos(1); // QOS 1，确保至少送达一次
        return messageHandler;
    }

    /**
     * MQTT 消息发送网关接口
     */
    @MessagingGateway(defaultRequestChannel = "mqttOutboundChannel")
    public interface MqttGateway {
        void sendToMqtt(String data);
        void sendToMqtt(String data, @Header("mqtt_topic") String topic);
    }

    // ================== MQTT 消费者 (接收端) 配置 ==================

    @Bean
    public MessageChannel mqttInboundChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageProducerSupport mqttInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(
                iotProperties.getMqtt().getConsumerClientId(),
                mqttClientFactory(),
                iotProperties.getMqtt().getTopic()
        );
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(1); // 同样采用 QOS 1，避免丢消息
        adapter.setOutputChannel(mqttInboundChannel());
        return adapter;
    }

    /**
     * MQTT 消费消息处理器
     */
    @Bean
    @ServiceActivator(inputChannel = "mqttInboundChannel")
    public MessageHandler handler() {
        return message -> {
            String payload = message.getPayload().toString();
            try {
                // 解析 JSON 数据
                TdengineWriter.MetricData data = objectMapper.readValue(payload, TdengineWriter.MetricData.class);
                // 写入批量缓冲池
                tdengineWriter.write(data.getTimestamp(), data.getValue(), data.getDeviceName(), data.getMetricName());
            } catch (Exception e) {
                log.error("MQTT 消费消息并解析 JSON 失败: " + payload, e);
            }
        };
    }
}
