/**
 * Copyright 2011 The Apache Software Foundation
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface GroupInfoManager {
  public static final String GROUP_TABLE_NAME = "_GROUP_";
  public static final byte[] GROUP_TABLE_NAME_BYTES = Bytes.toBytes(GROUP_TABLE_NAME);
  public static final byte[] SERVER_FAMILY_BYTES = Bytes.toBytes("servers");
  public static final byte[] INFO_FAMILY_BYTES = Bytes.toBytes("info");

  /**
   * Adds the group.
   *
   * @param groupInfo the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void addGroup(GroupInfo groupInfo) throws IOException;

  /**
   * Remove a region server group.
   *
   * @param groupName the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void removeGroup(String groupName) throws IOException;

  boolean moveServers(Set<String> hostPort, String srcGroup, String dstGroup) throws IOException;

  /**
   * Gets the group info of server.
   *
   * @param hostPort the server
   * @return An instance of GroupInfo.
   */
  GroupInfo getGroupOfServer(String hostPort) throws IOException;

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   */
  GroupInfo getGroup(String groupName) throws IOException;

  List<GroupInfo> listGroups() throws IOException;
}
