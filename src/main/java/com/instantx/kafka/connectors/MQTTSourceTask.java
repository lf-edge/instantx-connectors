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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttSecurityException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.instantx.kafka.connectors.config.MQTTSourceConnectorConfig;
import com.instantx.kafka.connectors.converters.MQTTSourceConverter;
import com.instantx.kafka.connectors.utils.PropertyUtils;
import com.instantx.kafka.connectors.utils.SSLUtils;

import javax.net.ssl.SSLSocketFactory;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MQTTSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MQTTSourceTask.class);
  private MQTTSourceConnectorConfig config;
  private MQTTSourceConverter converter;
  private IMqttAsyncClient mqttClient;
  private BlockingQueue<SourceRecord> buffer;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    log.info("[MQTTSourceConnector] Starting task...");
    this.config = new MQTTSourceConnectorConfig(map);

    log.info("[MQTTSourceConnector] Starting Mqtt (Source) Converter...");
    this.converter = new MQTTSourceConverter(this.config);

    log.info("[MQTTSourceConnector] Initializing Buffer queue....");
    this.buffer = new LinkedBlockingQueue<>();

    log.info("[MQTTSourceConnector] Starting Mqtt (Async) Client...");
    startMqttClient();

    log.info("[MQTTSourceConnector] Task started!");
  }

  private void startMqttClient() {
    try {
      // Create a persistence mechanism
      log.info("[MQTTSourceConnector] Memory persistence creation...");
      MqttClientPersistence persistence = new MemoryPersistence();

      // Create an MQTT client (asynchronous)
      log.info("[MQTTSourceConnector] Creating Async Mqtt client...");
      mqttClient = new MqttAsyncClient(config.getMqttBroker(), config.getMqttClientId() + Thread.currentThread().getId(), persistence);

      // Configure connection options (optional)
      log.info("[MQTTSourceConnector] Defining Mqtt connection options...");
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
        log.info("[MQTTSourceConnector] SSL/TLS configurations enabled");
        String caCrtFilePath = config.getMqttSslCa();
        try {
          if ("".equalsIgnoreCase(caCrtFilePath)) {
            // If MQTT Broker is hosted in a trusted server and the server verification is not required => don't define CA File
            log.info("[MQTTSourceConnector] Setting Default SSLSocketFactory");
            options.setSocketFactory(SSLSocketFactory.getDefault());
          } else {
            // If the MQTT Broker has Server Certificate issued from a Trusted CA, then the Server Certificate can be verified using:
            log.info("[MQTTSourceConnector] Setting SSLSocketFactory using CA '{}'", caCrtFilePath);
            options.setSocketFactory(SSLUtils.createSSLSocketFactory(caCrtFilePath, ""));
          }
        } catch (Exception e) {
          log.error("[MQTTSourceConnector] Not able to create SSLSocketFactory using CA: '{}' for MQTT client: ('){})", caCrtFilePath, config.getMqttClientId());
          log.error("[MQTTSourceConnector]  - ", e);
        }
      } else {
        log.info("[MQTTSourceConnector] Ssl configurations disabled");
      }

      // Optional
      //options.setWill("disconnected/reason", new MqttMessage("Client disconnected abnormally".getBytes()));

      // Set callback listener (optional)
      mqttClient.setCallback(new MqttCallback() {

        @Override
        public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {
          log.error("[MQTT Callback] Connection for MQTT Source connector disconnected, running client: ({}), lost to topic: ({})", config.getMqttClientId(), config.getMqttTopics());
        }

        @Override
        public void mqttErrorOccurred(MqttException e) {
          log.error("[MQTT Callback] Error occurred on MQTT Source Connector, with message ({})", e.getMessage());
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
          log.info("[MQTT Callback] Message arrived on topic ({})", topic);
          try {
            log.info("[MQTT Callback] Checking for duplicated flag: {}", message.isDuplicate());
            if (!message.isDuplicate()) {
              MqttProperties mqttProperties = message.getProperties();
              String synced = PropertyUtils.findProperty(mqttProperties != null ? mqttProperties.getUserProperties() : new ArrayList<>(), PropertyUtils.SYNCED);
              log.info("[MQTT Callback] Checking for (already synced property: {}", synced);
              if (!"true".equalsIgnoreCase(synced)) {
                log.info("[MQTT Callback] Adding mqtt source message to queue...");
                buffer.put(converter.convert(topic, message));
              } else {
                log.debug("  duplicated message...");
              }
            }
          } catch (Exception e) {
            log.error("[MQTT Callback] Error while processing arrived message. {}", e.getMessage());
            log.error("[MQTT Callback]  - ", e);
          }
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
          log.info("[MQTTSourceConnector] Connection successful with client Id ({}) on server(s) ({})", token.getClient().getClientId(), token.getClient().getServerURI());

          log.info("[MQTTSourceConnector] Subscribing to configured Mqtt (source) topics...");
          startMqttClientSubscriptions();
        }

        @Override
        public void onFailure(IMqttToken token, Throwable exception) {
          log.error("[MQTTSourceConnector] Connection failed: " + exception.getMessage());
          log.error("[MQTTSourceConnector]  - ", exception);
          throw new RuntimeException("[MQTTSourceConnector] Connection failed." + exception.getMessage());
        }
      });
      log.info("[MQTTSourceConnector] Successful Mqtt connection for client ({})", config.getMqttClientId());

    } catch (MqttSecurityException e) {
      log.error("[MQTTSourceConnector] Security Exception while connecting to Mqtt with client ({}) on topic(s) ({}) and username ({})", config.getMqttClientId(), config.getMqttTopics(), config.getMqttUsername());
      log.error("[MQTTSourceConnector]  - ", e);
      throw new RuntimeException("[MQTTSourceConnector] MqttSecurityException received. " + e.getMessage());
    } catch (MqttException e) {
      log.error("[MQTTSourceConnector] Failed to establish an Mqtt connection for client ({}) on topic(s) ({})", config.getMqttClientId(), config.getMqttTopics());
      log.error("[MQTTSourceConnector]  - ", e);
      throw new RuntimeException("[MQTTSourceConnector] MqttException received." + e.getMessage());
    }
  }

  private void startMqttClientSubscriptions() {
    try {
      if (mqttClient != null && mqttClient.isConnected()) {
        log.info("[MQTTSourceConnector] Subscribing to ({}) with Qos: ({})", String.join(" - ", config.getMqttTopics()), new int[]{config.getMqttQos()});
        MqttSubscription[] subscriptions = config.getMqttTopics().stream().map(topic -> new MqttSubscription(topic, config.getMqttQos())).toArray(MqttSubscription[]::new);

        // Input arguments: subscriptions, userContext, actionListener, properties
        IMqttToken token = mqttClient.subscribe(subscriptions, null, new MqttActionListener() {
          @Override
          public void onSuccess(IMqttToken token) { log.info("[MQTTSourceConnector] Subscribed successfully."); }

          @Override
          public void onFailure(IMqttToken token, Throwable exception) {
            log.error("[MQTTSourceConnector] Failed to subscribe: " + exception.getMessage());
            throw new RuntimeException("[MQTTSourceConnector] Could not Subscribe to defined topic patterns (" + String.join(" - ", config.getMqttTopics()) + "). " + exception.getMessage());
          }
        }, null);
      } else {
        log.error("[MQTTSourceConnector] Expecting mqtt client connecting to start subscriptions, but found it disconnected...");
      }
    } catch (MqttException e) {
      log.error("[MQTTSourceConnector] Failed to subscribe to Mqtt topics. {}", e.getMessage());
      throw new RuntimeException("Connector won't work without subscriptions. " + e.getMessage());
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.info("[MQTTSourceConnector] Polling records (from Queue)...");
    List<SourceRecord> records = new ArrayList<>();
    records.add(buffer.take());

    log.info("[MQTTSourceConnector] Returning ({}) records from poll()", records.size());
    return records;
  }

  @Override
  public void stop() {
    // Stopping task
    log.info("[MQTTSourceConnector] Stopping MQTT Source Connector task...");
    try {
      if (mqttClient != null && mqttClient.isConnected()) {
        log.info(" - disconnecting gracefully from MQTT Broker ({})", config.getString(config.getMqttBroker()));
        mqttClient.disconnect();
      }
    } catch (MqttException e) {
      log.error(" - Mqtt Exception thrown while disconnecting client. {}", e.getMessage());
    } catch (Exception e) {
      log.error(" - Exception thrown while disconnecting client. {}", e.getMessage());
    }
    log.info("[MQTTSourceConnector] MQTT Source Connector task stopped.");
  }
}
