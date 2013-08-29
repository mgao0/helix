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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;

public class ResourceAccessor {
  private final ClusterId _clusterId;
  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;

  public ResourceAccessor(ClusterId clusterId, HelixDataAccessor accessor) {
    _clusterId = clusterId;
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * save resource assignment
   */
  public void setRresourceAssignment(ResourceId resourceId, RscAssignment resourceAssignment) {
    // TODO impl this
  }

  /**
   * set ideal-state
   */
  public void setResourceIdealState(ResourceId resourceId, IdealState idealState) {
    _accessor.setProperty(_keyBuilder.idealStates(resourceId.stringify()), idealState);
  }

  /**
   * set external view of a resource
   * @param extView
   */
  public void setResourceExternalView(ResourceId resourceId, ExternalView extView) {
    _accessor.setProperty(_keyBuilder.idealStates(resourceId.stringify()), extView);
  }

  /**
   * drop external view of a resource
   */
  public void dropResourceExternalView(ResourceId resourceId) {
    _accessor.removeProperty(_keyBuilder.idealStates(resourceId.stringify()));
  }
}
