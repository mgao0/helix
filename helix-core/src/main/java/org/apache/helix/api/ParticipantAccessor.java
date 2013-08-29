package org.apache.helix.api;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class ParticipantAccessor {
  private static final Logger LOG = Logger.getLogger(ParticipantAccessor.class);

  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;
  private final ClusterId _clusterId;

  public ParticipantAccessor(ClusterId clusterId, HelixDataAccessor accessor) {
    _clusterId = clusterId;
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * @param participantId
   * @param isEnabled
   */
  void enableParticipant(ParticipantId participantId, boolean isEnabled) {
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) == null) {
      throw new HelixException("Config for participant: " + participantId
          + " does NOT exist in cluster: " + _clusterId);
    }

    InstanceConfig config = new InstanceConfig(participantId.stringify());
    config.setInstanceEnabled(isEnabled);
    _accessor.updateProperty(_keyBuilder.instanceConfig(participantId.stringify()), config);

  }

  /**
   * disable participant
   */
  public void disableParticipant(ParticipantId participantId) {
    enableParticipant(participantId, false);
  }

  /**
   * enable participant
   */
  public void enableParticipant(ParticipantId participantId) {
    enableParticipant(participantId, false);
  }

  /**
   * create messages for participant
   * @param msgs
   */
  public void insertMessagesToParticipant(ParticipantId participantId, Map<MessageId, Message> msgMap) {
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    List<Message> msgs = new ArrayList<Message>();
    for (MessageId msgId : msgMap.keySet()) {
      msgKeys.add(_keyBuilder.message(participantId.stringify(), msgId.stringify()));
      msgs.add(msgMap.get(msgId));
    }

    _accessor.createChildren(msgKeys, msgs);
  }

  /**
   * set messages of participant
   * @param msgs
   */
  public void setMessagesOfParticipant(ParticipantId participantId, Map<MessageId, Message> msgMap) {
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    List<Message> msgs = new ArrayList<Message>();
    for (MessageId msgId : msgMap.keySet()) {
      msgKeys.add(_keyBuilder.message(participantId.stringify(), msgId.stringify()));
      msgs.add(msgMap.get(msgId));
    }
    _accessor.setChildren(msgKeys, msgs);
  }

  /**
   * delete messages from participant
   * @param msgIdSet
   */
  public void deleteMessagesFromParticipant(ParticipantId participantId, Set<MessageId> msgIdSet) {
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    for (MessageId msgId : msgIdSet) {
      msgKeys.add(_keyBuilder.message(participantId.stringify(), msgId.stringify()));
    }

    // TODO impl batch remove
    for (PropertyKey msgKey : msgKeys) {
      _accessor.removeProperty(msgKey);
    }
  }

  /**
   * @param enabled
   * @param participantId
   * @param resourceId
   * @param partitionIdSet
   */
  void enablePartitionsForParticipant(final boolean enabled, final ParticipantId participantId,
      final ResourceId resourceId, final Set<PartitionId> partitionIdSet) {
    // check instanceConfig exists
    PropertyKey instanceConfigKey = _keyBuilder.instanceConfig(participantId.stringify());
    if (_accessor.getProperty(instanceConfigKey) == null) {
      throw new HelixException("Config for participant: " + participantId
          + " does NOT exist in cluster: " + _clusterId);
    }

    // check resource exist. warn if not
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify()));
    if (idealState == null) {
      LOG.warn("Disable partitions: " + partitionIdSet + " but Cluster: " + _clusterId
          + ", resource: " + resourceId
          + " does NOT exists. probably disable it during ERROR->DROPPED transtition");

    } else {
      // check partitions exist. warn if not
      for (PartitionId partitionId : partitionIdSet) {
        if ((idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO && idealState
            .getPreferenceList(partitionId.stringify()) == null)
            || (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED && idealState
                .getInstanceStateMap(partitionId.stringify()) == null)) {
          LOG.warn("Cluster: " + _clusterId + ", resource: " + resourceId + ", partition: "
              + partitionId + ", partition does not exist in ideal state");
        }
      }
    }

    // TODO merge list logic should go to znrecord updater
    // update participantConfig
    // could not use ZNRecordUpdater since it doesn't do listField merge/subtract
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    final List<String> partitionNames = new ArrayList<String>();
    for (PartitionId partitionId : partitionIdSet) {
      partitionNames.add(partitionId.stringify());
    }
    baseAccessor.update(instanceConfigKey.getPath(), new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + _clusterId + ", instance: " + participantId
              + ", participant config is null");
        }

        // TODO: merge with InstanceConfig.setInstanceEnabledForPartition
        List<String> list =
            currentData.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
        Set<String> disabledPartitions = new HashSet<String>();
        if (list != null) {
          disabledPartitions.addAll(list);
        }

        if (enabled) {
          disabledPartitions.removeAll(partitionNames);
        } else {
          disabledPartitions.addAll(partitionNames);
        }

        list = new ArrayList<String>(disabledPartitions);
        Collections.sort(list);
        currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list);
        return currentData;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * @param disablePartitionSet
   */
  public void disablePartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> disablePartitionIdSet) {
    enablePartitionsForParticipant(false, participantId, resourceId, disablePartitionIdSet);
  }

  /**
   * @param enablePartitionSet
   */
  public void enablePartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> enablePartitionIdSet) {
    enablePartitionsForParticipant(true, participantId, resourceId, enablePartitionIdSet);
  }

  /**
   * create live instance for the participant
   * @param participantId
   */
  public void startParticipant(ParticipantId participantId) {
    // TODO impl this
  }

  /**
   * read participant related data
   * @param participantId
   * @return
   */
  public Participant readParticipant(ParticipantId participantId) {
    // read physical model
    String participantName = participantId.stringify();
    InstanceConfig instanceConfig = _accessor.getProperty(_keyBuilder.instance(participantName));
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(participantName));

    Map<String, Message> instanceMsgMap = Collections.emptyMap();
    Map<String, CurrentState> instanceCurStateMap = Collections.emptyMap();
    if (liveInstance != null) {
      SessionId sessionId = liveInstance.getSessionId();

      instanceMsgMap = _accessor.getChildValuesMap(_keyBuilder.messages(participantName));
      instanceCurStateMap =
          _accessor.getChildValuesMap(_keyBuilder.currentStates(participantName,
              sessionId.stringify()));
    }

    // convert to logical model
    String hostName = instanceConfig.getHostName();

    int port = -1;
    try {
      port = Integer.parseInt(instanceConfig.getPort());
    } catch (IllegalArgumentException e) {
      // keep as -1
    }
    if (port < 0 || port > 65535) {
      port = -1;
    }
    boolean isEnabled = instanceConfig.getInstanceEnabled();

    List<String> disabledPartitions = instanceConfig.getDisabledPartitions();
    Set<PartitionId> disabledPartitionIdSet;
    if (disabledPartitions == null) {
      disabledPartitionIdSet = Collections.emptySet();
    } else {
      disabledPartitionIdSet = new HashSet<PartitionId>();
      for (String partitionId : disabledPartitions) {
        disabledPartitionIdSet.add(new PartitionId(PartitionId.extractResourceId(partitionId),
            PartitionId.stripResourceId(partitionId)));
      }
    }

    Set<String> tags = new HashSet<String>(instanceConfig.getTags());

    RunningInstance runningInstance = null;
    if (liveInstance != null) {
      runningInstance =
          new RunningInstance(new SessionId(liveInstance.getSessionIdString()), new HelixVersion(
              liveInstance.getHelixVersionString()), new ProcId(liveInstance.getLiveInstance()));
    }

    Map<MessageId, Message> msgMap = new HashMap<MessageId, Message>();
    for (String msgId : instanceMsgMap.keySet()) {
      Message message = instanceMsgMap.get(msgId);
      msgMap.put(new MessageId(msgId), message);
    }

    // TODO convert current state
    // Map<ParticipantId, CurState> curStateMap = new HashMap<ParticipantId, CurState>();
    // if (currentStateMap != null) {
    // for (String participantId : currentStateMap.keySet()) {
    // CurState curState =
    // new CurState(_id, new ParticipantId(participantId), currentStateMap.get(participantId));
    // curStateMap.put(new ParticipantId(participantId), curState);
    // }
    // }

    return new Participant(participantId, hostName, port, isEnabled, disabledPartitionIdSet, tags,
        runningInstance, null, msgMap);
  }

  /**
   * update resource current state of a participant
   * @param curStateUpdate current state change delta
   */
  public void updateParticipantCurrentState(ParticipantId participantId, SessionId sessionId,
      ResourceId resourceId, CurrentState curStateUpdate) {
    _accessor.updateProperty(
        _keyBuilder.currentState(participantId.stringify(), sessionId.stringify(),
            resourceId.stringify()), curStateUpdate);
  }

  /**
   * drop resource current state of a participant
   */
  public void dropParticipantCurrentState(ParticipantId participantId, SessionId sessionId,
      ResourceId resourceId) {
    _accessor.removeProperty(_keyBuilder.currentState(participantId.stringify(),
        sessionId.stringify(), resourceId.stringify()));
  }
}
