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

import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GroupAdmin {
  /**
   * Get online regions of a region server group.
   *
   * @param groupName the name of the group
   * @return list of online regions this group contains
   */
  List<HRegionInfo> listOnlineRegionsOfGroup(String groupName) throws IOException;

  /**
   * Get member tables of a group.
   *
   * @param groupName the name of the group
   * @return list of table names
   */
  Collection<String> listTablesOfGroup(String groupName) throws IOException;

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
  GroupInfo getGroupInfoOfTable(byte[] tableName) throws IOException;

  /**
   * Move a set of serves to another group
   *
   * @param server the server
   * @param targetGroup the target group
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  void moveServers(Set<String> server, String targetGroup)
      throws IOException, InterruptedException;


  /**
   * Add a new group
   * @param name name of the group
   * @throws IOException
   */
  void addGroup(String name) throws IOException;

  /**
   * Remove a new group
   * @param name name of the group
   * @throws IOException
   */
  void removeGroup(String name) throws IOException;

  /**
   * Gets the existing groups.
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
   * @return
   * @throws IOException
   */
  Map<String, String> listServersInTransition() throws IOException;
}
