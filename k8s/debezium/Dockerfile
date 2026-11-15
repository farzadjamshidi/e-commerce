# Strimzi's Kafka Connect image has no connectors. This adds Debezium's Postgres connector,
# the same version as the connect container in docker-compose.yml.
FROM quay.io/strimzi/kafka:1.3.1-kafka-4.2.2

ARG DEBEZIUM_VERSION=3.7.0.Final

USER root
RUN mkdir -p /opt/kafka/plugins \
    && curl -sfL "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
       | tar -xz -C /opt/kafka/plugins
# Strimzi runs Kafka images as this user
USER 1001
