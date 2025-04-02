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

import com.instantx.kafka.connectors.utils.TopicUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.instantx.kafka.connectors.config.MQTTSinkConnectorConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a MQTT message to a Kafka message
 */
public class MQTTSinkConverter {

    private final MQTTSinkConnectorConfig config;

    private final Logger log = LoggerFactory.getLogger(MQTTSinkConverter.class);

    public MQTTSinkConverter(MQTTSinkConnectorConfig config) {
        this.config = config;
    }

    public MqttMessage convert(String topic, SinkRecord record) {
        log.info("[MQTTSinkConverter] Converting (Kafka) SinkRecord to MQTT message, received on topic ({})", topic);

        // Define target metadata (Mqtt UserProperties)
        String targetTopic = TopicUtils.getMqttTopicFromKafka(topic, record.key().toString());
        List<UserProperty> userProperties = new ArrayList<>();
        userProperties.add(new UserProperty("targetTopic", targetTopic));
        userProperties.add(new UserProperty("src", "kafka"));
        userProperties.add(new UserProperty("synced", "true"));
        log.info("[MQTTSinkConverter] Mapping from kafka topic ({}) and key ({}) -> to Mqtt topic ({})", topic, record.key().toString(), targetTopic);

        // Mapping (Kafka) headers to (MQTT) User properties
        if (!record.headers().isEmpty()) {
            log.debug("[MQTTSinkConverter] Mapping metadata....");
            record.headers().forEach(h -> {
                log.debug("[MQTTSinkConverter] Kafka header (key: {} | Value: {}) to Mqtt User Property.", h.key(), h.value());
                // TODO: include a filter to the accepted/expected headers to be copied, or list of exceptions
                userProperties.add(new UserProperty(h.key(), h.value().toString()));
            });
        }
        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.setUserProperties(userProperties);

        // TODO: define some of the converter inputs based on configurations
        // E.g. schemas used, list of mapped metadata
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(record.value().toString().getBytes());
        mqttMessage.setQos(config.getMqttQos());
        mqttMessage.setDuplicate(true);
        mqttMessage.setProperties(mqttProperties);

        log.debug("[MQTTSinkConverter] Converted to MQTT Message");
        return mqttMessage;
    }
}
