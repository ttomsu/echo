/*
 * Copyright 2019 Armory, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.echo.telemetry;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.google.protobuf.util.JsonFormat;
import com.netflix.spinnaker.echo.config.TelemetryConfig;
import com.netflix.spinnaker.echo.events.EchoEventListener;
import com.netflix.spinnaker.echo.model.Event;
import com.netflix.spinnaker.kork.proto.stats.Application;
import com.netflix.spinnaker.kork.proto.stats.Execution;
import com.netflix.spinnaker.kork.proto.stats.SpinnakerInstance;
import com.netflix.spinnaker.kork.proto.stats.Stage;
import com.netflix.spinnaker.kork.proto.stats.Status;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty("telemetry.enabled")
public class TelemetryEventListener implements EchoEventListener {

  private static final Set<String> LOGGABLE_DETAIL_TYPES =
      ImmutableSet.of(
          "orca:orchestration:complete",
          "orca:orchestration:failed",
          "orca:pipeline:complete",
          "orca:pipeline:failed");

  private final TelemetryService telemetryService;

  private final TelemetryConfig.TelemetryConfigProps telemetryConfigProps;

  @Autowired
  public TelemetryEventListener(
      TelemetryService telemetryService,
      TelemetryConfig.TelemetryConfigProps telemetryConfigProps) {
    this.telemetryService = telemetryService;
    this.telemetryConfigProps = telemetryConfigProps;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void processEvent(Event event) {
    try {
      if (event.getDetails() == null || event.getContent() == null) {
        log.debug("Telemetry not sent: Details or content not found in event");
        return;
      }

      String eventType = event.getDetails().getType();
      if (!LOGGABLE_DETAIL_TYPES.contains(eventType)) {
        log.debug("Telemetry not sent: type '{}' not whitelisted ", eventType);
        return;
      }

      Map<String, Object> execution = (Map<String, Object>) event.content.get("execution");
      if (execution == null) {
        return;
      }

      String executionId = execution.getOrDefault("id", "").toString();
      Execution.Type executionType =
          Execution.Type.valueOf(
              // TODO(ttomsu, louisjimenez): Add MPTv1 and v2 execution type detection.
              execution.getOrDefault("type", "").toString().toUpperCase());

      Map trigger = (Map) execution.getOrDefault("trigger", new HashMap());
      Execution.Trigger.Type triggerType =
          Execution.Trigger.Type.valueOf(
              trigger.getOrDefault("type", "UNKNOWN").toString().toUpperCase());

      List<Map> stages = (List<Map>) execution.getOrDefault("stages", new ArrayList<>());
      List<Stage> protoStages = stages.stream().map(this::toStage).collect(Collectors.toList());

      Execution executionProto =
          Execution.newBuilder()
              .setId(executionId)
              .setType(executionType)
              .setTrigger(Execution.Trigger.newBuilder().setType(triggerType))
              .addAllStages(protoStages)
              .build();

      Application application =
          Application.newBuilder()
              .setId(hash(event.details.getApplication()))
              .setExecution(executionProto)
              .build();

      SpinnakerInstance spinnakerInstance =
          SpinnakerInstance.newBuilder()
              .setId(telemetryConfigProps.getInstanceId())
              .setApplication(application)
              .build();

      com.netflix.spinnaker.kork.proto.stats.Event loggedEvent =
          com.netflix.spinnaker.kork.proto.stats.Event.newBuilder()
              .setSpinnakerInstance(spinnakerInstance)
              .build();

      telemetryService.log(JsonFormat.printer().print(loggedEvent));
      log.debug("Telemetry sent!");
    } catch (Exception e) {
      log.warn("Could not send Telemetry event {}", event, e);
    }
  }

  private Stage toStage(Map stage) {
    return Stage.newBuilder()
        .setType(stage.get("type").toString())
        .setStatus(Status.valueOf(stage.get("status").toString().toUpperCase()))
        .build();
  }

  private String hash(String clearText) {
    return Hashing.sha256().hashString(clearText, StandardCharsets.UTF_8).toString();
  }
}
