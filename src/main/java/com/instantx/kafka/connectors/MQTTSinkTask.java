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
package com.instantx.kafka.connectors;

import com.instantx.kafka.connectors.utils.PropertyUtils;
import com.instantx.kafka.connectors.utils.SSLUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSecurityException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.instantx.kafka.connectors.config.MQTTSinkConnectorConfig;
import com.instantx.kafka.connectors.converters.MQTTSinkConverter;

import javax.net.ssl.SSLSocketFactory;
import java.util.*;
import java.util.concurrent.*;

public class MQTTSinkTask extends SinkTask {
  static final Logger log = LoggerFactory.getLogger(MQTTSinkTask.class);

  private MQTTSinkConnectorConfig config;
  private MQTTSinkConverter converter;
  private IMqttAsyncClient mqttClient;

  // Queue for buffering messages
  private BlockingQueue<MqttMessage> buffer;
  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    log.info("[MQTTSinkConnector] Starting task...");
    this.config = new MQTTSinkConnectorConfig(map);

    log.info("[MQTTSinkConnector] Starting Mqtt (Sink) Converter...");
    this.converter = new MQTTSinkConverter(this.config);

    log.info("[MQTTSinkConnector] Starting Mqtt (Async) Client...");
    startMqttClient();

    log.info("[MQTTSinkConnector] Initializing Buffer queue...");
    this.buffer = new LinkedBlockingQueue<>(config.getBatchSize());

    log.info("[MQTTSinkConnector] Scheduling Flush...");
    startScheduler();

    log.info("[MQTTSinkConnector] Task started!");
  }

  private void startScheduler() {
    try {
      if (!this.scheduler.isShutdown()) {
        this.scheduler.shutdown();
        Thread.sleep(5000);
      }
    } catch (InterruptedException e) {
      log.warn("[MQTTSinkConnector] Shutting down");
    }
    if (this.scheduler.isShutdown() && this.scheduler.isTerminated()) {
      this.scheduler = Executors.newScheduledThreadPool(2);
    } else {
      log.info("-------------> {}, {}", this.scheduler.isShutdown(), this.scheduler.isTerminated());
    }
    log.info("#-------------> {}, {}", this.scheduler.isShutdown(), this.scheduler.isTerminated());

    // (Re)Starting the Fixed Scheduled (Flush)Task
    this.scheduler.scheduleAtFixedRate(new FlushTask(this), 0, config.getFlushFrequency(), TimeUnit.MILLISECONDS);
  }

  private void startMqttClient() {
    try {
      // Create a persistence mechanism
      log.info("[MQTTSinkConnector] Memory persistence creation...");
      MqttClientPersistence persistence = new MemoryPersistence();

      // Create an MQTT client (asynchronous)
      log.info("[MQTTSinkConnector] Creating Async Mqtt client...");
      mqttClient = new MqttAsyncClient(config.getMqttBroker(), config.getMqttClientId(), persistence);

      // Configure connection options (optional)
      log.info("[MQTTSinkConnector] Defining Mqtt connection options...");
      MqttConnectionOptions options = new MqttConnectionOptions();
      options.setServerURIs(new String[] { config.getMqttBroker() });
      options.setCleanStart(config.getMqttCleanSession());
      options.setConnectionTimeout(config.getMqttConnectionTimeout());
      options.setKeepAliveInterval(config.getMqttKeepAliveInterval());
      options.setAutomaticReconnect(config.getMqttAutomaticReconnect());

      if (config.isMqttAuthEnabled()) {
        options.setUserName(config.getMqttUsername());
        options.setPassword(config.getMqttPassword().value().getBytes());
      }

      if (config.isMqttSslEnabled()) {
        log.info("[MQTTSinkConnector] SSL/TLS configurations enabled");
        String caCrtFilePath = config.getMqttSslCa();
        try {
          if ("".equalsIgnoreCase(caCrtFilePath)) {
            // If MQTT Broker is hosted in a trusted server and the server verification is not required => don't define CA File
            log.info("[MQTTSinkConnector] Setting Default SSLSocketFactory");
            options.setSocketFactory(SSLSocketFactory.getDefault());
          } else {
            // If the MQTT Broker has Server Certificate issued from a Trusted CA, then the Server Certificate can be verified using:
            log.info("[MQTTSinkConnector] Setting SSLSocketFactory using CA '{}'", caCrtFilePath);
            options.setSocketFactory(SSLUtils.createSSLSocketFactory(caCrtFilePath, ""));
          }
        } catch (Exception e) {
          log.error("[MQTTSinkConnector] Not able to create SSLSocketFactory using CA: '{}' for MQTT client: ('){})", caCrtFilePath, config.getMqttClientId());
          log.error("[MQTTSinkConnector]  - ", e);
        }
      } else {
        log.info("[MQTTSinkConnector] SSL/TLS configurations disabled");
      }

      // Optional
      //options.setWill("disconnected/reason", new MqttMessage("Client disconnected abnormally".getBytes()));

      // Set callback listener (optional)
      mqttClient.setCallback(new MqttCallback() {

        @Override
        public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {
          log.error("[MQTT Callback] Connection for MQTT Sink connector disconnected, running client: ({})", config.getMqttClientId());
        }

        @Override
        public void mqttErrorOccurred(MqttException e) {
          log.error("[MQTT Callback] Error occurred on MQTT Sink Connector, with message ({})", e.getMessage());
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
          log.info("[MQTT Callback] Message arrived on topic ({}) - bypassing for Sink Connector", topic);
        }

        @Override
        public void deliveryComplete(IMqttToken iMqttToken) {
          log.info("[MQTT Callback] Delivery Complete for topic ({})", Arrays.stream(iMqttToken.getTopics()).findFirst());
        }

        @Override
        public void connectComplete(boolean reconnect, String serverUri) {
          log.info("[MQTT Callback] Connect Complete on Server ({}) reconnect flag set to ({})", serverUri, reconnect);
        }

        @Override
        public void authPacketArrived(int reasonCode, MqttProperties mqttProperties) {
          log.info("[MQTT Callback] Auth Packet arrived with reason code ({}), running client: ({}), with method ({})",
                  reasonCode,
                  mqttProperties.getAssignedClientIdentifier(),
                  mqttProperties.getAuthenticationMethod());
        }
      });

      IMqttToken token = mqttClient.connect(options, options, new MqttActionListener() {
        @Override
        public void onSuccess(IMqttToken token) {
          log.info("[MQTTSinkConnector] Connection successful with client Id ({}) on server(s) ({})", token.getClient().getClientId(), token.getClient().getServerURI());
        }

        @Override
        public void onFailure(IMqttToken token, Throwable exception) {
          log.error("[MQTTSinkConnector] Connection failed: " + exception.getMessage());
          log.error("[MQTTSinkConnector]  - ", exception);
          throw new RuntimeException("[MQTTSinkConnector] Connection failed." + exception.getMessage());
        }
      });
      log.info("[MQTTSinkConnector] Mqtt connection for client ({}) being created", config.getMqttClientId());

    } catch (MqttSecurityException e) {
      log.error("[MQTTSinkConnector] Security Exception while connecting to Mqtt with client ({}) and username ({})", config.getMqttClientId(), config.getMqttUsername());
      log.error("[MQTTSinkConnector]  - ", e);
      throw new RuntimeException("[MQTTSinkConnector] MqttSecurityException received. " + e.getMessage());
    } catch (MqttException e) {
      log.error("[MQTTSinkConnector] Failed to establish an Mqtt connection for client ({})", config.getMqttClientId());
      log.error("[MQTTSinkConnector]  - ", e);
      throw new RuntimeException("[MQTTSinkConnector] MqttException received. " + e.getMessage());
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    try {
      log.info("[MQTTSinkConnector] Running Put and receiving ({}) records", records.size());
      if (mqttClient.isConnected()) {
        for (SinkRecord record : records) {
          log.info("[MQTTSinkConnector] Received kafka message with partition ({}) and offset ({})", record.kafkaPartition(), record.kafkaOffset());

          String synced = PropertyUtils.findHeader(record.headers(), PropertyUtils.SYNCED);
          log.info("[MQTTSinkConnector] Checking for (already synced header: {}", synced);
          if (!"true".equalsIgnoreCase(synced)) {

            // Convert record to MQTT message
            MqttMessage mqttMessage = converter.convert(record.topic(), record);

            // Confirm if publishing should be executed directly, or messages should be buffered first.
            if (config.isBufferEnabled()) {

              // Check if buffer is full before adding
              if (buffer.remainingCapacity() < 1) {
                scheduler.execute(new FlushTask(this));
              }

              // Buffer converted message
              buffer.put(mqttMessage);

            } else {

              // Publish directly
              publishMqttMessage(mqttMessage);

            }

          } else {
            log.debug("  duplicated message...");
          }
        }
      } else {
        log.warn("[MQTTSinkConnector] Receiving messages, but Mqtt Client not yet connected, so bypassing messages");
      }

    } catch (InterruptedException e) {
      log.error("[MQTTSinkConnector] InterruptedException while saving record to Queue. {}", e.getMessage());
      //throw new RuntimeException(e);
    }
  }

  private void publishMqttMessage(MqttMessage message) {
    try {
      String topic = PropertyUtils.findProperty(message.getProperties().getUserProperties(), "targetTopic");
      log.info("[MQTTSinkConnector] Publishing Mqtt Message to topic ({})", topic);
      IMqttToken token = mqttClient.publish(topic, message, null, new MqttActionListener() {
        @Override
        public void onSuccess(IMqttToken token) {
          log.info("[MQTTSinkConnector] Publishing successful with client Id ({}) on server(s) ({}) for topic ({})", token.getClient().getClientId(), token.getClient().getServerURI(), topic);
        }

        @Override
        public void onFailure(IMqttToken token, Throwable exception) {
          log.error("[MQTTSinkConnector] Publishing failed on topic ({}): " + exception.getMessage(), topic);
          throw new RuntimeException("[MQTTSinkConnector] Publishing failed." + exception.getMessage());
        }
      });

    } catch (MqttException e) {
      log.error("[MQTTSinkConnector] Error while publishing Mqtt Message. {}", e.getMessage());
      throw new RuntimeException("[MQTTSinkConnector] Error while publishing Mqtt Message. " + e.getMessage());
    }
  }

  @Override
  public void stop() {
    // Stopping task
    log.info("[MQTTSinkConnector] Stopping task...");
    try {

      // Disconnecting MQTT Client
      if (mqttClient != null && mqttClient.isConnected()) {
        log.info(" - disconnecting gracefully from MQTT Broker ({})", config.getMqttBroker());
        mqttClient.disconnect();
      }

      // Stop all running schedulers
      this.scheduler.shutdown();

    } catch (MqttException e) {
      log.error(" - Mqtt Exception thrown while disconnecting client. {}", e.getMessage());
      log.error(" - ", e);
    } catch (Exception e) {
      log.error(" - Exception thrown while disconnecting client. {}", e.getMessage());
      log.error(" - ", e);
    }
    log.info("[MQTTSinkConnector] Task stopped.");
  }

  private static class FlushTask extends TimerTask {

    private final MQTTSinkTask task;

    public FlushTask(MQTTSinkTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      log.info("[MQTTSinkConnector] Flushing buffer....");
      try {
        if (task.mqttClient.isConnected()) {
          log.info("[MQTTSinkConnector] Buffer with ({}) records", task.buffer.size());
          while (!task.buffer.isEmpty()) {
            task.publishMqttMessage(task.buffer.take());
          }
        } else {
          log.warn("[MQTTSinkConnector] Trying to flush sink records, but Mqtt Client not connected.");
          // Shouldn't continue and acknowledge the kafka read (offset)
        }
      } catch (InterruptedException e) {
        log.error("[MQTTSinkConnector] InterruptedException while flushing queue. {}", e.getMessage());
        log.error("[MQTTSinkConnector]  - ", e);
        //throw new RuntimeException(e); or do nothing
      } catch (Exception e) {
        log.error("[MQTTSinkConnector] Exception while flushing queue. {}", e.getMessage());
        log.error("[MQTTSinkConnector]  - ", e);
        //throw new RuntimeException(e); or do nothing
      }
    }
  }
}
