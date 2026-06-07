package cn.coreqi.opcmq.writer;

import cn.coreqi.opcmq.config.IotProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class TdengineWriter {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private IotProperties iotProperties;

    // 内存队列，用作缓冲区
    private final BlockingQueue<MetricData> bufferQueue = new LinkedBlockingQueue<>();
    private ScheduledExecutorService scheduler;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MetricData {
        private long timestamp;
        private double value;
        private String deviceName;
        private String metricName;
    }

    @PostConstruct
    public void init() {
        // 1. 初始化数据库及超级表
        initDatabase();

        // 2. 启动定时器，定期冲刷缓冲区，确保即使数据量少时也能及时写入
        scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r, "tdengine-flush-thread");
            thread.setDaemon(true);
            return thread;
        });

        long interval = iotProperties.getTdengine().getFlushIntervalMs();
        scheduler.scheduleWithFixedDelay(this::flush, interval, interval, TimeUnit.MILLISECONDS);
        log.info("TDengine 批量写入器初始化成功，冲刷间隔：{} ms，批量大小：{}", interval, iotProperties.getTdengine().getBatchSize());
    }

    /**
     * 初始化 TDengine 数据库和超级表
     */
    private void initDatabase() {
        log.info("开始初始化 TDengine 数据库与超级表...");
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // 1. 创建数据库 (如果不存在)
            stmt.execute("CREATE DATABASE IF NOT EXISTS iot_data KEEP 3650");
            log.info("成功创建/确认数据库: iot_data");

            // 2. 使用数据库
            stmt.execute("USE iot_data");

            // 3. 创建超级表 (meters)
            // ts: 时间戳, val: 采集值
            // 标签：device_name 设备名称, metric_name 指标名称
            stmt.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, val DOUBLE) TAGS (device_name VARCHAR(50), metric_name VARCHAR(50))");
            log.info("成功创建/确认超级表: meters");

        } catch (Exception e) {
            log.error("初始化 TDengine 数据库失败: ", e);
        }
    }

    /**
     * 将采集数据添加到写入缓冲区
     */
    public void write(long timestamp, double value, String deviceName, String metricName) {
        bufferQueue.offer(new MetricData(timestamp, value, deviceName, metricName));

        // 如果队列大小达到阈值，立即异步/同步触发一次写入
        if (bufferQueue.size() >= iotProperties.getTdengine().getBatchSize()) {
            flush();
        }
    }

    /**
     * 冲刷缓冲区，将数据批量写入数据库
     */
    public synchronized void flush() {
        if (bufferQueue.isEmpty()) {
            return;
        }

        List<MetricData> drainList = new ArrayList<>();
        // 每次最多取出配置的 batchSize 条数据进行批量插入
        bufferQueue.drainTo(drainList, iotProperties.getTdengine().getBatchSize());

        if (drainList.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        // 拼接批量插入 SQL
        // TDengine 支持多条语句/多个子表一次性写入：
        // INSERT INTO t1 USING stable TAGS(...) VALUES(...) t2 USING stable TAGS(...) VALUES(...)
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        for (MetricData data : drainList) {
            // 子表名称规范化：避免特殊字符，使用 deviceName + metricName 的组合
            String tableName = "d_" + data.getDeviceName().toLowerCase().replaceAll("[^a-z0-9_]", "_")
                    + "_" + data.getMetricName().toLowerCase().replaceAll("[^a-z0-9_]", "_");

            sqlBuilder.append("`").append(tableName).append("` ")
                    .append("USING `meters` TAGS ('")
                    .append(data.getDeviceName()).append("', '")
                    .append(data.getMetricName()).append("') ")
                    .append("VALUES (")
                    .append(data.getTimestamp()).append(", ")
                    .append(data.getValue()).append(") ");
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 使用当前数据库
            stmt.execute("USE iot_data");
            // 执行批量写入
            stmt.execute(sqlBuilder.toString());
            log.debug("成功批量写入 TDengine {} 条数据，耗时：{} ms", drainList.size(), (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            log.error("写入 TDengine 失败，重新存入队列：", e);
            // 写入失败时，为了保证数据不丢失，重新塞回队列
            bufferQueue.addAll(drainList);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("正在关闭 TDengine 写入服务，冲刷剩余数据...");
        if (scheduler != null) {
            scheduler.shutdown();
        }
        // 最后冲刷一次
        flush();
    }
}
