package com.ecommerce.order;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Profile("bench")
class ProducerBenchmark implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ProducerBenchmark.class);
    private static final String TOPIC = "producer-bench";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int records;
    private final String payload;
    private final long pauseMicros;

    ProducerBenchmark(KafkaTemplate<String, String> kafkaTemplate,
                      @Value("${bench.records:100000}") int records,
                      @Value("${bench.payload-bytes:200}") int payloadBytes,
                      @Value("${bench.pause-micros:0}") long pauseMicros) {
        this.kafkaTemplate = kafkaTemplate;
        this.records = records;
        this.payload = "x".repeat(payloadBytes);
        this.pauseMicros = pauseMicros;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Warm up: open connections and fetch metadata before timing
        kafkaTemplate.send(TOPIC, "warmup", payload).join();

        long start = System.nanoTime();
        for (int i = 0; i < records; i++) {
            kafkaTemplate.send(TOPIC, "order-" + (i % 1000), payload);
            if (pauseMicros > 0) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(pauseMicros));
            }
        }
        kafkaTemplate.flush();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        Map<MetricName, ? extends Metric> metrics = kafkaTemplate.metrics();
        log.info("BENCH records={} elapsedMs={} records/s={} batch-size-avg={} records-per-request-avg={} request-total={} request-latency-avg={}",
                records, elapsedMs, records * 1000L / Math.max(elapsedMs, 1),
                metric(metrics, "batch-size-avg"), metric(metrics, "records-per-request-avg"),
                metric(metrics, "request-total"), metric(metrics, "request-latency-avg"));
    }

    private static String metric(Map<MetricName, ? extends Metric> metrics, String name) {
        return metrics.entrySet().stream()
                .filter(e -> e.getKey().group().equals("producer-metrics") && e.getKey().name().equals(name))
                .map(e -> String.format("%.1f", ((Number) e.getValue().metricValue()).doubleValue()))
                .findFirst()
                .orElse("n/a");
    }
}
