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
package com.instantx.kafka.connectors.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;


public class MQTTSinkConnectorConfig extends AbstractConfig {

  // -----------------------------------------------------------------------
  // MQTT Related configurations
  public static final String MQTT_BROKER = "mqtt.broker";
  public static final String MQTT_BROKER_DOC = "Host and port of the MQTT broker, eg: tcp://192.168.1.1:1883";

  public static final String MQTT_CLIENT_ID = "mqtt.clientID";
  public static final String MQTT_CLIENT_ID_DOC = "clientID";

//  public static final String MQTT_TOPICS = "mqtt.topic";
//  public static final String MQTT_TOPICS_DOC = "List of topic names to subscribe to";

  public static final String MQTT_QOS = "mqtt.qos";
  public static final String MQTT_QOS_DOC = "Quality of service MQTT messaging, default is 1 (at least once)";

  public static final String MQTT_AUTOMATIC_RECONNECT = "mqtt.automaticReconnect";
  public static final String MQTT_AUTOMATIC_RECONNECT_DOC = "set Automatic reconnect, default true";

  public static final String MQTT_KEEP_ALIVE_INTERVAL = "mqtt.keepAliveInterval";
  public static final String MQTT_KEEP_ALIVE_INTERVAL_DOC = "set the keepalive interval, default is 60 seconds";

  public static final String MQTT_CLEAN_SESSION = "mqtt.cleanSession";
  public static final String MQTT_CLEAN_SESSION_DOC = "Sets whether the client and server should remember state across restarts and reconnects, default is true";

  public static final String MQTT_CONNECTION_TIMEOUT = "mqtt.connectionTimeout";
  public static final String MQTT_CONNECTION_TIMEOUT_DOC = "Sets the connection timeout, default is 30";

  public static final String MQTT_AUTH_ENABLE = "mqtt.connector.auth";
  public static final String MQTT_AUTH_ENABLE_DOC = "Sets the auth toggle (enable = true/disable = false)";
  public static final String MQTT_USERNAME = "mqtt.connector.auth.userName";
  public static final String MQTT_USERNAME_DOC = "Sets the username for the MQTT connection timeout, default is \"\"";
  public static final String MQTT_PASSWORD = "mqtt.connector.auth.password";
  public static final String MQTT_PASSWORD_DOC = "Sets the password for the MQTT connection timeout, default is \"\"";


  public static final String MQTT_SSL_ENABLE = "mqtt.connector.ssl";
  public static final String MQTT_SSL_ENABLE_DOC = "Sets the ssl toggle (enable = true/disable = false)";

  // SSL Configurations
  public static final String MQTT_SSL_CA = "mqtt.connector.ssl.ca";
  public static final String MQTT_SSL_CA_DOC = "Sets the ssl CA file path";
//  public static final String MQTT_SSL_CRT = "mqtt.connector.ssl.crt";
//  public static final String MQTT_SSL_CRT_DOC = "Sets the ssl CRT file path";
//  public static final String MQTT_SSL_KEY = "mqtt.connector.ssl.key";
//  public static final String MQTT_SSL_KEY_DOC = "Sets the ssl Key file path";

  // -----------------------------------------------------------------------
  // Kafka Related configurations
  //public static final String KAFKA_TOPICS = "kafka.topic";
  //public static final String KAFKA_TOPICS_DOC = "List of kafka topics to publish to";

  public static final String BUFFER_ENABLE = "buffer.enable";
  public static final String BUFFER_ENABLE_DOC = "Sets the Buffer toggle (enable = true/disable = false)";
  public static final String BATCH_SIZE = "buffer.size";
  public static final String BATCH_SIZE_DOC = "Sets the Buffer size";

  public static final String FLUSH_FREQUENCY = "buffer.flush.frequency";
  public static final String FLUSH_FREQUENCY_DOC = "Frequency in milliseconds in which the queued messages are flushed";

  // Internal Configurations
  public static final String IS_RUNNING = "task.running";

  public MQTTSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public MQTTSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    ConfigDef configDef = new ConfigDef();
    addParams(configDef);
    return configDef;
  }

  private static void addParams(final ConfigDef configDef) {
    configDef
            .defineInternal(IS_RUNNING,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    Importance.LOW)
            .define(MQTT_BROKER,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    MQTT_BROKER_DOC)
            .define(MQTT_CLIENT_ID,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    MQTT_CLIENT_ID_DOC)
//            .define(MQTT_TOPICS,
//                    ConfigDef.Type.STRING,
//                    ConfigDef.Importance.HIGH,
//                    MQTT_TOPICS_DOC)
            .define(MQTT_QOS,
                    ConfigDef.Type.INT,
                    1,
                    ConfigDef.Range.between(1,3),
                    ConfigDef.Importance.MEDIUM,
                    MQTT_QOS_DOC)
            .define(MQTT_AUTOMATIC_RECONNECT,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    MQTT_AUTOMATIC_RECONNECT_DOC)
            .define(MQTT_KEEP_ALIVE_INTERVAL,
                    ConfigDef.Type.INT,
                    60,
                    ConfigDef.Importance.LOW,
                    MQTT_KEEP_ALIVE_INTERVAL_DOC)
            .define(MQTT_CLEAN_SESSION,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    MQTT_CLEAN_SESSION_DOC)
            .define(MQTT_CONNECTION_TIMEOUT,
                    ConfigDef.Type.INT,
                    30,
                    ConfigDef.Importance.LOW,
                    MQTT_CONNECTION_TIMEOUT_DOC)
            .define(MQTT_AUTH_ENABLE,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    Importance.MEDIUM,
                    MQTT_AUTH_ENABLE_DOC)
            .define(MQTT_USERNAME,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    MQTT_USERNAME_DOC)
            .define(MQTT_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.LOW,
                    MQTT_PASSWORD_DOC)

            // SSL Configurations
            .define(MQTT_SSL_ENABLE,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    Importance.MEDIUM,
                    MQTT_SSL_ENABLE_DOC)
            .define(MQTT_SSL_CA,
                    ConfigDef.Type.STRING,
                    "",
                    Importance.MEDIUM,
                    MQTT_SSL_CA_DOC)
//            .define(MQTT_SSL_CRT,
//                    ConfigDef.Type.STRING,
//                    "",
//                    Importance.MEDIUM,
//                    MQTT_SSL_CRT_DOC)
//            .define(MQTT_SSL_KEY,
//                    ConfigDef.Type.STRING,
//                    "",
//                    Importance.MEDIUM,
//                    MQTT_SSL_KEY_DOC)

            // Kafka Configurations
//            .define(KAFKA_TOPICS,
//                    ConfigDef.Type.LIST,
//                    ConfigDef.Importance.HIGH,
//                    KAFKA_TOPICS_DOC)
            .define(BUFFER_ENABLE,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    Importance.MEDIUM,
                    BUFFER_ENABLE_DOC)
            .define(BATCH_SIZE,
                    Type.INT,
                    2,
                    Importance.MEDIUM,
                    BATCH_SIZE_DOC)
            .define(FLUSH_FREQUENCY,
                    Type.INT,
                    1000,
                    Importance.MEDIUM,
                    FLUSH_FREQUENCY_DOC)
            ;
  }

  public String getMqttBroker(){
    return this.getString(MQTT_BROKER);
  }
  public String getMqttClientId(){
    return this.getString(MQTT_CLIENT_ID);
  }
//  public String getMqttTopics(){
//    return this.getString(MQTT_TOPICS);
//  }
  public int getMqttQos(){
    return this.getInt(MQTT_QOS);
  }
  public boolean getMqttAutomaticReconnect(){
    return this.getBoolean(MQTT_AUTOMATIC_RECONNECT);
  }
  public int getMqttKeepAliveInterval(){
    return this.getInt(MQTT_KEEP_ALIVE_INTERVAL);
  }
  public boolean getMqttCleanSession(){
    return this.getBoolean(MQTT_CLEAN_SESSION);
  }
  public int getMqttConnectionTimeout(){
    return this.getInt(MQTT_CONNECTION_TIMEOUT);
  }
  public String getMqttUsername(){
    return this.getString(MQTT_USERNAME);
  }
  public Password getMqttPassword(){
    return this.getPassword(MQTT_PASSWORD);
  }

  public boolean isMqttAuthEnabled(){
    return this.getBoolean(MQTT_AUTH_ENABLE);
  }
  public boolean isMqttSslEnabled(){
    return this.getBoolean(MQTT_SSL_ENABLE);
  }
  public String getMqttSslCa(){
    return this.getString(MQTT_SSL_CA);
  }

//  public String getMqttSslCrt(){
//    return this.getString(MQTT_SSL_CRT);
//  }
//
//  public String getMqttSslKey(){
//    return this.getString(MQTT_SSL_KEY);
//  }
//
//  public List<String> getKafkaTopics(){
//    return this.getList(KAFKA_TOPICS);
//  }

  public boolean isBufferEnabled(){
    return this.getBoolean(BUFFER_ENABLE);
  }
  public int getBatchSize() { return this.getInt(BATCH_SIZE);}
  public int getFlushFrequency() { return this.getInt(FLUSH_FREQUENCY);}
  public boolean isTaskRunning() { return this.getBoolean(IS_RUNNING); }
}
