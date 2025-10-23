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
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.instantx.kafka.connectors.config.MQTTSourceConnectorConfig;

/**
 * Implementation of a MQTT Source connector (with specific custom adaptations
 * to STEP project)
 */
public class MQTTSourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(MQTTSourceConnector.class);
  private MQTTSourceConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  // This will be executed once per connector. This can be used to handle
  // connector level setup.
  @Override
  public void start(Map<String, String> map) {
    log.info("[MQTTSourceConnector] MQTT Source connector - Starting...");
    config = new MQTTSourceConnectorConfig(map);
    log.info("[MQTTSourceConnector] MQTT Source connector - Started");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MQTTSourceTask.class;
  }

  // This is used to schedule the number of tasks that will be running. This
  // should not exceed maxTasks.
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("[MQTTSourceConnector] Executing TaskConfigs with {} max task", maxTasks);

    // TODO: analyze possibility to launch multiple "listeners" one for each
    // subscription....
    // ## Define taskConfigs based on number of worker nodes and create a distinct
    // name for each!!!
    // ## different name | consumer | group ...

    // if (maxTasks > 1) { log.info("maxTasks is " + maxTasks + ". MaxTasks > 1 is
    // not supported in this connector."); }
    List<Map<String, String>> taskConfigs = new ArrayList<>(1);
    taskConfigs.add(new HashMap<>(config.originalsStrings()));

    return taskConfigs;
  }

  // Include any additional checks on the configurations received.
  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    log.info("[MQTTSourceConnector] Validating connector configurations...");

    // E.g. check for accepted values, complex combinations of input parameters,
    // etc...

    return super.validate(connectorConfigs);
  }

  // Do things that are necessary to stop your connector.
  @Override
  public void stop() {
    log.info("[MQTTSourceConnector] Stopping MQTT Source Connector");
  }

  @Override
  public ConfigDef config() {
    return MQTTSourceConnectorConfig.conf();
  }
}
