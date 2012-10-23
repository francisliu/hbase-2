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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * This class is responsible for managing region server group information.
 */
public class GroupAdminClient implements GroupAdmin {
  private GroupAdmin proxy;
	private static final Log LOG = LogFactory.getLog(GroupAdminClient.class);
  private int operationTimeout;

  public GroupAdminClient(Configuration conf) throws ZooKeeperConnectionException, MasterNotRunningException {
    proxy = new HBaseAdmin(conf).coprocessorProxy(GroupAdminProtocol.class);
    operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  @Override
  public List<HRegionInfo> listOnlineRegionsOfGroup(String groupName) throws IOException {
    return proxy.listOnlineRegionsOfGroup(groupName);
  }

  @Override
  public Collection<String> listTablesOfGroup(String groupName) throws IOException {
    return proxy.listTablesOfGroup(groupName);
  }

  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    return proxy.getGroup(groupName);
  }

  @Override
  public GroupInfo getGroupInfoOfTable(byte[] tableName) throws IOException {
    return proxy.getGroupInfoOfTable(tableName);
  }

  @Override
  public void moveServers(Set<String> servers, String targetGroup) throws IOException, InterruptedException {
    proxy.moveServers(servers, targetGroup);
    waitForTransitions(servers);
  }

  @Override
  public void addGroup(String groupName) throws IOException {
    proxy.addGroup(groupName);
  }

  @Override
  public void removeGroup(String name) throws IOException {
    proxy.removeGroup(name);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return proxy.listGroups();
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return proxy.getGroupOfServer(hostPort);
  }

  @Override
  public Map<String, String> listServersInTransition() throws IOException {
    return proxy.listServersInTransition();
  }

  /**
   * Retrieve a table's group membership from the table descriptor
   * @param desc
   * @return
   * @throws IOException
   */
  public String getGroupPropertyOfTable(HTableDescriptor desc) throws IOException {
    return GroupInfo.getGroupString(desc);
  }

  /**
   * Set's a table's group membership
   * @param groupName group name the table is being assigned to
   * @param desc table descriptor of the table
   * @throws IOException
   */
  public void setGroupPropertyOfTable(String groupName, HTableDescriptor desc) throws IOException {
    GroupInfo.setGroupString(groupName, desc);
  }

  private void waitForTransitions(Set<String> servers) throws IOException, InterruptedException {
    long endTime = System.currentTimeMillis()+operationTimeout;
    boolean found;
    do {
      found = false;
      for(String server: proxy.listServersInTransition().keySet()) {
        found = found || servers.contains(server);
      }
      Thread.sleep(1000);
    } while(found && System.currentTimeMillis() <= endTime);
  }
}
