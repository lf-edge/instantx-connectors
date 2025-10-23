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

import java.util.*;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.instantx.kafka.connectors.config.MQTTSinkConnectorConfig;

/**
 * Implementation of a MQTT Sink connector (with specific custom adaptations to
 * STEP project)
 */
public class MQTTSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(MQTTSinkConnector.class);
  private MQTTSinkConnectorConfig config;
  private boolean isRunning;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  // This will be executed once per connector. This can be used to handle
  // connector level setup.
  @Override
  public void start(Map<String, String> map) {
    log.info("[MQTTSinkConnector] Starting...");
    this.config = new MQTTSinkConnectorConfig(map);
    this.isRunning = true;
    log.info("[MQTTSinkConnector] Started");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MQTTSinkTask.class;
  }

  // This is used to schedule the number of tasks that will be running. This
  // should not exceed maxTasks.
  // TODO: analyze possibility to launch multiple "listeners" one for each
  // subscription....
  // Segregate tasks based on topic patterns (e.g.) or another possible split.
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("[MQTTSinkConnector] Defining TaskConfigs with {} max task", maxTasks);

    if (maxTasks > 1) {
      log.info("[MQTTSinkConnector] MaxTasks > 1 is not supported in this connector.");
    }

    List<Map<String, String>> taskConfigs = new ArrayList<>(1);
    Map<String, String> map = new HashMap<>(config.originalsStrings());
    map.put(MQTTSinkConnectorConfig.IS_RUNNING, String.valueOf(this.isRunning));
    taskConfigs.add(map);

    return taskConfigs;
  }

  // Include any additional checks on the configurations received.
  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    log.info("[MQTTSinkConnector] Validating connector configurations...");

    // E.g. check for accepted values, complex combinations of input parameters,
    // etc...

    return super.validate(connectorConfigs);
  }

  // Do things that are necessary to stop your connector.
  @Override
  public void stop() {
    log.info("[MQTTSinkConnector] Stopping Connector");

    // To trigger config update and flag signalling Tasks to Stop
    this.isRunning = false;
    this.context().requestTaskReconfiguration();
  }

  @Override
  public ConfigDef config() {
    return MQTTSinkConnectorConfig.conf();
  }
}
