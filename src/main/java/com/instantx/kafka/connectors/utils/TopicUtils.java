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
package com.instantx.kafka.connectors.utils;

import java.util.Arrays;
import java.util.stream.Collectors;

public class TopicUtils {

    public static String getMqttTopicFromKafka(String kafkaTopic, String kafkaKey) {
        // Default behavior, kafka key = mqtt topic
        return kafkaKey;
    }

    public static String getKafkaTopicFromMqtt(String mqttTopic, String defaultTopic) {
        // Depends on the mappings received
        String[] topicLevels = mqttTopic.replaceAll("\\s+", "").split("/");
        if (topicLevels.length <= 2)
            return defaultTopic;
        else if ("public".equals(topicLevels[2]))
            return (String) Arrays.<CharSequence>stream((CharSequence[])topicLevels).limit(3L).collect(Collectors.joining(".")) + ".incoming";
        return (String)Arrays.<CharSequence>stream((CharSequence[])topicLevels).limit(2L).collect(Collectors.joining(".")) + ".private.incoming";
    }
}
