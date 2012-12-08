/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.group;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface GroupInfoManager {
  public static final String GROUP_TABLE_NAME = "_GROUP_";
  public static final byte[] GROUP_TABLE_NAME_BYTES = Bytes.toBytes(GROUP_TABLE_NAME);
  public static final byte[] SERVER_FAMILY_BYTES = Bytes.toBytes("servers");
  public static final byte[] TABLE_FAMILY_BYTES = Bytes.toBytes("tables");
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

  /**
   * move servers to a new group.
   * @param hostPort list of servers, must be part of the same group
   * @param srcGroup
   * @param dstGroup
   * @return
   * @throws IOException
   */
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

  /**
   * Get the group membership of a table
   * @param tableName
   * @return
   * @throws IOException
   */
	String getGroupOfTable(String tableName) throws IOException;

  /**
   * Set the group membership of a table
   *
   *
   * @param tableName
   * @param groupName
   * @return
   * @throws IOException
   */
  void moveTables(Set<String> tableName, String groupName) throws IOException;

  /**
   * List the groups
   *
   * @return
   * @throws IOException
   */
  List<GroupInfo> listGroups() throws IOException;

  /**
   * Refresh/reload the group information from
   * the persistent store
   *
   * @throws IOException
   */
  void refresh() throws IOException;

  /**
   * Wether the manager is able to fully
   * return group metadata
   *
   * @return
   */
  boolean isOnline();
}
