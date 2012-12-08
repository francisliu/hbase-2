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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Group user API interface used between client and server.
 */
@InterfaceAudience.Private
public interface GroupAdmin {
  /**
   * Get member tables of a group.
   *
   *
   * @param groupName the name of the group
   * @return list of table names
   */
  NavigableSet<String> listTablesOfGroup(String groupName) throws IOException;

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   */
  GroupInfo getGroupInfo(String groupName) throws IOException;

  /**
   * Gets the group info of table.
   *
   * @param tableName the table name
   * @return An instance of GroupInfo.
   */
  GroupInfo getGroupInfoOfTable(String tableName) throws IOException;

  /**
   * Move a set of serves to another group
   *
   *
   * @param servers set of servers, must be in the form HOST:PORT
   * @param targetGroup the target group
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void moveServers(Set<String> servers, String targetGroup) throws IOException;

  /**
   * Move tables to a new group.
   * This will unassign all of a table's region so it can be reassigned to the correct group.
   * @param tables list of tables to move
   * @param targetGroup target group
   * @throws IOException
   */
  void moveTables(Set<String> tables, String targetGroup) throws IOException;

  /**
   * Add a new group
   * @param name name of the group
   * @throws IOException
   */
  void addGroup(String name) throws IOException;

  /**
   * Remove a group
   * @param name name of the group
   * @throws IOException
   */
  void removeGroup(String name) throws IOException;

  /**
   * Lists the existing groups.
   *
   * @return Collection of GroupInfo.
   */
  List<GroupInfo> listGroups() throws IOException;

  /**
   * Retrieve the GroupInfo a server is affiliated to
   * @param hostPort
   * @return
   * @throws IOException
   */
  GroupInfo getGroupOfServer(String hostPort) throws IOException;

  /**
   * List servers that are currently being moved to a new group
   * @return a map containing server=>targetGroup KV pairs
   * @throws IOException
   */
  Map<String, String> listServersInTransition() throws IOException;
}
