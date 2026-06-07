package cn.coreqi.opcmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "iot")
public class IotProperties {

    private OpcUa opcua = new OpcUa();
    private Mqtt mqtt = new Mqtt();
    private Tdengine tdengine = new Tdengine();

    @Data
    public static class OpcUa {
        private String serverUrl;
        private String nodeId;
        private String clientName;
    }

    @Data
    public static class Mqtt {
        private String brokerUrl;
        private String username;
        private String password;
        private String topic;
        private String producerClientId;
        private String consumerClientId;
    }

    @Data
    public static class Tdengine {
        private int batchSize;
        private long flushIntervalMs;
    }
}
