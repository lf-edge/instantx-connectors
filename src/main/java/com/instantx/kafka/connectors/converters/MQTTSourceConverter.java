/*-
 * ========================LICENSE_START=================================
 * kafka-connect
 * %%
 * Copyright (C) 2024 - 2025 Vodafone
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package com.instantx.kafka.connectors.converters;

import com.instantx.kafka.connectors.config.MQTTSourceConnectorConfig;
import com.instantx.kafka.connectors.utils.TopicUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * Converts a MQTT message to a Kafka message
 */
public class MQTTSourceConverter {

    private final MQTTSourceConnectorConfig config;

    private final Logger log = LoggerFactory.getLogger(MQTTSourceConverter.class);

    public MQTTSourceConverter(MQTTSourceConnectorConfig mqttSourceConnectorConfig) {
        this.config = mqttSourceConnectorConfig;
    }

    public SourceRecord convert(String originTopic, MqttMessage mqttMessage) {
        log.info("[MQTTSourceConverter] Converting MQTT message to Kafka, received on topic ({})", originTopic);

        // Define target metadata (kafka headers)
        ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("mqtt.message.id", mqttMessage.getId());
        headers.addInt("mqtt.message.qos", mqttMessage.getQos());
        headers.addBoolean("mqtt.message.duplicate", mqttMessage.isDuplicate());
        headers.addString("src", "mqtt");
        headers.addString("synced", "true");

        // Mapping (MQTT) user properties to (Kafka) headers
        log.debug("[MQTTSourceConverter] Mapping metadata....");
        List<UserProperty> userPropertyList = mqttMessage.getProperties().getUserProperties();
        if (!userPropertyList.isEmpty()) {
            userPropertyList.forEach(up -> {
                log.debug("[MQTTSourceConverter] MQTT User Property (key: {} | Value: {}) to Kafka Header.",
                        up.getKey(), up.getValue());
                headers.addString(up.getKey(), up.getValue());
            });
        }

        try {
            log.debug("[MQTTSourceConverter] Custom Value Schema: {}", config.getMqttValueSchemaType());
            log.debug("[MQTTSourceConverter] Value Schema: {}", config.getGenericProperty("value.converter"));
        } catch (Exception e) {
            log.debug("[MQTTSourceConverter] Failed to extract configurations. Error: {}", e.getMessage());
        }

        // E.g. schemas used, list of mapped metadata
        SourceRecord sourceRecord = new SourceRecord(
                new HashMap<>(), // Source Partition
                new HashMap<>(), // Source Offset
                TopicUtils.getKafkaTopicFromMqtt(originTopic, originTopic), // (Target) Kafka Topic
                (Integer) null, // Partition
                Schema.STRING_SCHEMA, // Key Schema
                originTopic, // Key (Kafka key = Mqtt Topic)
                (MQTTSourceConnectorConfig.MQTT_VALUE_SCHEMA_DEFAULT.equals(config.getMqttValueSchemaType())
                        ? Schema.STRING_SCHEMA
                        : Schema.BYTES_SCHEMA),
                (MQTTSourceConnectorConfig.MQTT_VALUE_SCHEMA_DEFAULT.equals(config.getMqttValueSchemaType())
                        ? new String(mqttMessage.getPayload())
                        : mqttMessage.getPayload()),
                System.currentTimeMillis(), // Timestamp
                headers); // Kafka Headers

        log.debug("[MQTTSourceConverter] Converted MQTT Message: " + sourceRecord);
        return sourceRecord;
    }
}
