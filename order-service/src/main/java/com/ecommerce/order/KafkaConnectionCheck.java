package com.ecommerce.order;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
class KafkaConnectionCheck implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectionCheck.class);
    private static final long TIMEOUT_SECONDS = 10;

    private final KafkaAdmin kafkaAdmin;

    KafkaConnectionCheck(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = admin.describeCluster();
            Collection<Node> nodes = cluster.nodes().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Cluster id: {}", cluster.clusterId().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            log.info("Controller: {}", cluster.controller().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).id());

            // A broker config request is sent to that broker directly, so this proves
            // every advertised listener is reachable from this host.
            List<ConfigResource> brokers = nodes.stream()
                    .map(node -> new ConfigResource(ConfigResource.Type.BROKER, node.idString()))
                    .toList();
            Map<ConfigResource, Config> configs =
                    admin.describeConfigs(brokers).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (Node node : nodes) {
                Config config = configs.get(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
                log.info("Broker {} at {}:{} reachable, min.insync.replicas={}",
                        node.id(), node.host(), node.port(), config.get("min.insync.replicas").value());
            }

            log.info("Topics: {}", admin.listTopics().names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
    }
}
