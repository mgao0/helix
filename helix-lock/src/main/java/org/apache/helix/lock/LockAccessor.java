/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.helix.lock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.istack.internal.Nullable;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters")
public class LockAccessor {
  private static Logger _logger = LoggerFactory.getLogger(LockAccessor.class.getName());
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String ZNRECORD_NOT_VALID = "Input is not a valid ZNRecord!";
  private static final String SCOPE_NOT_VALID = "Input is not a valid ZNRecord!";
  private static final String COMMAND_NOT_VALID = "Invalid command received.";

  public enum Command {
    acquire, release, getInfo, isOwner,
  }

  private Command getCommand(String commandStr) throws HelixException {
    if (commandStr == null) {
      throw new HelixException("Command string is null!");
    }
    try {
      return Command.valueOf(commandStr);
    } catch (IllegalArgumentException ex) {
      throw new HelixException("Unknown command: " + commandStr);
    }
  }

  private static Response notFound() {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  private static Response serverError() {
    return Response.serverError().build();
  }

  private static Response OK(Object entity) {
    return Response.ok(entity, MediaType.APPLICATION_JSON_TYPE).build();
  }

  private static Response ok() {
    return Response.ok().build();
  }

  private static Response badRequest(String errorMsg) {
    return Response.status(Response.Status.BAD_REQUEST).entity(errorMsg).type(MediaType.TEXT_PLAIN)
        .build();
  }

  private static String toJson(Object object) throws IOException {
    SerializationConfig serializationConfig = OBJECT_MAPPER.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);

    StringWriter sw = new StringWriter();
    OBJECT_MAPPER.writeValue(sw, object);
    sw.append('\n');

    return sw.toString();
  }

  private static Response JSONRepresentation(Object entity) {
    try {
      String jsonStr = toJson(entity);
      return OK(jsonStr);
    } catch (IOException e) {
      _logger.error("Failed to convert " + entity + " to JSON response", e);
      return serverError();
    }
  }

  private static ZNRecord toZNRecord(String content) throws IOException {
    return OBJECT_MAPPER.reader(ZNRecord.class).readValue(content);
  }

  private HelixLockScope buildLockScope(String clusterId, String instanceName, String resourceName,
      String partitionName) {
    List<String> pathKeys = new ArrayList<>();

    if (clusterId == null) {
      return null;
    }
    pathKeys.add(clusterId);

    if (instanceName == null) {
      return new HelixLockScope(HelixLockScope.LockScopeProperty.CLUSTER, pathKeys);
    }
    pathKeys.add(instanceName);

    if (resourceName == null) {
      return new HelixLockScope(HelixLockScope.LockScopeProperty.PARTICIPANT, pathKeys);
    }
    pathKeys.add(resourceName);

    if (partitionName == null) {
      return new HelixLockScope(HelixLockScope.LockScopeProperty.RESOURCE, pathKeys);
    }
    pathKeys.add(partitionName);

    return new HelixLockScope(HelixLockScope.LockScopeProperty.PARTITION, pathKeys);
  }

  private Response zkPost(String clusterId, String instanceName, String resourceName,
      String partitionName, String commandStr, String zkAddress, String content) {
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }

    ZNRecord userInput;
    try {
      userInput = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest(ZNRECORD_NOT_VALID);
    }

    HelixLockScope scope = buildLockScope(clusterId, instanceName, resourceName, partitionName);
    if (scope == null) {
      return badRequest(SCOPE_NOT_VALID);
    }

    HelixLock lock = new ZKHelixNonblockingLock(scope, zkAddress, userInput);

    switch (command) {
      case acquire:
        if (lock.acquireLock()) {
          return ok();
        }
        return badRequest("Failed to acquire the lock");
      case release:
        if (lock.releaseLock()) {
          return ok();
        }
        return badRequest("Failed to release the lock");
      default:
        return badRequest(COMMAND_NOT_VALID);
    }
  }

  private Response zkGet(String clusterId, String instanceName, String resourceName,
      String partitionName, String commandStr, String zkAddress, String content) {
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }

    ZNRecord userInput;
    try {
      userInput = toZNRecord(content);
    } catch (IOException e) {
      _logger.error("Failed to deserialize user's input " + content + ", Exception: " + e);
      return badRequest(ZNRECORD_NOT_VALID);
    }

    HelixLockScope scope = buildLockScope(clusterId, instanceName, resourceName, partitionName);
    if (scope == null) {
      return badRequest(SCOPE_NOT_VALID);
    }

    HelixLock lock = new ZKHelixNonblockingLock(scope, zkAddress, userInput);

    switch (command) {
      case getInfo:
        Object lockInfo = lock.getLockInfo();
        if (lockInfo == null) {
          return notFound();
        }
        return JSONRepresentation(lockInfo);
      case isOwner:
        return OK(JSONRepresentation(lock.isOwner()));
      default:
        return badRequest(COMMAND_NOT_VALID);
    }
  }

  @POST
  @Path("{clusterId}/LOCK")
  public Response callZKLockPOST(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @QueryParam("content") String content) {
    return zkPost(clusterId, null, null, null, commandStr, zkAddress, content);
  }

  @POST
  @Path("{clusterId}/LOCK/{instanceName}")
  public Response callZKLockPOST(@PathParam("clusterId") String clusterId,
      @Nullable @PathParam("instanceName") String instanceName,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @QueryParam("content") String content) {
    return zkPost(clusterId, instanceName, null, null, commandStr, zkAddress, content);
  }

  @POST
  @Path("{clusterId}/LOCK/{instanceName}/{resourceName}")
  public Response callZKLockPOST(@PathParam("clusterId") String clusterId,
      @Nullable @PathParam("instanceName") String instanceName,
      @Nullable @PathParam("resourceName") String resourceName,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @QueryParam("content") String content) {
    return zkPost(clusterId, instanceName, resourceName, null, commandStr, zkAddress, content);
  }

  @POST
  @Path("{clusterId}/LOCK/{instanceName}/{resourceName}/{partitionName}")
  public Response callZKLockPOST(@PathParam("clusterId") String clusterId,
      @Nullable @PathParam("instanceName") String instanceName,
      @Nullable @PathParam("resourceName") String resourceName,
      @Nullable @PathParam("partitionName") String partitionName,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @QueryParam("content") String content) {
    return zkPost(clusterId, instanceName, resourceName, partitionName, commandStr, zkAddress,
        content);
  }

  @GET
  @Path("{clusterId}/LOCK")
  public Response callZKLockGET(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @Nullable @QueryParam("content") String content) {
    return zkGet(clusterId, null, null, null, commandStr, zkAddress, content);
  }

  @GET
  @Path("{clusterId}/LOCK/{instanceName}")
  public Response callZKLockGET(@PathParam("clusterId") String clusterId,
      @Nullable @PathParam("instanceName") String instanceName,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @Nullable @QueryParam("content") String content) {
    return zkGet(clusterId, instanceName, null, null, commandStr, zkAddress, content);
  }

  @GET
  @Path("{clusterId}/LOCK/{instanceName}/{resourceName}")
  public Response callZKLockGET(@PathParam("clusterId") String clusterId,
      @Nullable @PathParam("instanceName") String instanceName,
      @Nullable @PathParam("resourceName") String resourceName,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @Nullable @QueryParam("content") String content) {
    return zkGet(clusterId, instanceName, resourceName, null, commandStr, zkAddress, content);
  }

  @GET
  @Path("{clusterId}/LOCK/{instanceName}/{resourceName}/{partitionName}")
  public Response callZKLockGET(@PathParam("clusterId") String clusterId,
      @Nullable @PathParam("instanceName") String instanceName,
      @Nullable @PathParam("resourceName") String resourceName,
      @Nullable @PathParam("partitionName") String partitionName,
      @QueryParam("command") String commandStr, @QueryParam("zkAddress") String zkAddress,
      @Nullable @QueryParam("content") String content) {
    return zkGet(clusterId, instanceName, resourceName, partitionName, commandStr, zkAddress,
        content);
  }
}
