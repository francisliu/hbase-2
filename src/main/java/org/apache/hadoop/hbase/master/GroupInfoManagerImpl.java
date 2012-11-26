/**
 * Copyright 2009 The Apache Software Foundation
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupInfoManagerImpl implements GroupInfoManager {
	private static final Log LOG = LogFactory.getLog(GroupInfoManagerImpl.class);

	//Access to this map should always be synchronized.
	private Map<String, GroupInfo> groupMap;
  private Map<String, GroupInfo> tableMap;
  private MasterServices master;
  private HTable table;
  private ZooKeeperWatcher watcher;
  private GroupStartupWorker groupStartupWorker;
  private HBaseAdmin admin;

  public GroupInfoManagerImpl(MasterServices master) throws IOException {
		this.groupMap = new ConcurrentHashMap<String, GroupInfo>();
		this.tableMap = new ConcurrentHashMap<String, GroupInfo>();
    this.master = master;
    this.watcher = master.getZooKeeper();
    this.admin = new HBaseAdmin(master.getConfiguration());
    groupStartupWorker = new GroupStartupWorker(this, master, GroupInfoManager.GROUP_TABLE_NAME_BYTES);
    groupStartupWorker.start();
  }

	/**
	 * Adds the group.
	 *
	 * @param groupInfo the group name
	 * @throws java.io.IOException Signals that an I/O exception has occurred.
	 */
  @Override
  public synchronized void addGroup(GroupInfo groupInfo) throws IOException {
		if (groupMap.get(groupInfo.getName()) != null ||
        groupInfo.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: "+groupInfo.getName());
    }
    groupMap.put(groupInfo.getName(), groupInfo);
    try {
      flushConfig();
    } catch (IOException e) {
      groupMap.remove(groupInfo.getName());
      throw e;
    }
	}

  @Override
  public synchronized boolean moveServers(Set<String> hostPort, String srcGroup, String dstGroup) throws IOException {
    GroupInfo src = new GroupInfo(getGroup(srcGroup));
    GroupInfo dst = new GroupInfo(getGroup(dstGroup));
    boolean foundOne = false;
    for(String el: hostPort) {
      foundOne = foundOne || src.removeServer(el);
      dst.addServer(el);
    }

    Map<String,GroupInfo> newMap = Maps.newHashMap(groupMap);
    if(!src.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      newMap.put(src.getName(), src);
    }
    if(!dst.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      newMap.put(dst.getName(), dst);
    }
    flushConfig(newMap);
    groupMap = newMap;
    return foundOne;
  }

  /**
	 * Gets the group info of server.
	 *
	 * @param hostPort the server
	 * @return An instance of GroupInfo.
	 */
  @Override
  public synchronized GroupInfo getGroupOfServer(String hostPort) throws IOException {
		for(GroupInfo info : groupMap.values()){
			if(info.containsServer(hostPort)){
				return info;
			}
		}
		return getGroup(GroupInfo.DEFAULT_GROUP);
	}

	/**
	 * Gets the group information.
	 *
	 * @param groupName the group name
	 * @return An instance of GroupInfo
	 */
  @Override
  public synchronized GroupInfo getGroup(String groupName) throws IOException {
		if (groupName.equalsIgnoreCase(GroupInfo.DEFAULT_GROUP)) {
			GroupInfo defaultInfo = new GroupInfo(GroupInfo.DEFAULT_GROUP);
      List<ServerName> unassignedServers =
          difference(getOnlineRS(),getAssignedServers());
      for(ServerName serverName: unassignedServers) {
        defaultInfo.addServer(serverName.getHostAndPort());
      }
      for(String tableName: master.getTableDescriptors().getAll().keySet()) {
        if(!tableMap.containsKey(tableName)) {
          defaultInfo.addTable(tableName);
        }
      }
			return defaultInfo;
		} else {
			return this.groupMap.get(groupName);
		}
	}

  @Override
  public synchronized String getGroupOfTable(String tableName) throws IOException {
    if(tableMap.containsKey(tableName)) {
      return tableMap.get(tableName).getName();
    }
    return GroupInfo.DEFAULT_GROUP;
  }

  @Override
  public synchronized void moveTables(Set<String> tableNames, String groupName) throws IOException {
    for(String tableName: tableNames) {
      moveTable(tableName, groupName);
    }
    flushConfig(groupMap);
  }

  private synchronized void moveTable(String tableName, String groupName) throws IOException {
    if(!GroupInfo.DEFAULT_GROUP.equals(groupName) && !groupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group does not exist");
    }
    if(tableMap.containsKey(tableName)) {
      tableMap.get(tableName).removeTable(tableName);
      tableMap.remove(tableName);
    }
    if(!GroupInfo.DEFAULT_GROUP.equals(groupName)) {
      groupMap.get(groupName).addTable(tableName);
      tableMap.put(tableName, groupMap.get(groupName));
    }
  }


  /**
	 * Delete a region server group.
	 *
	 * @param groupName the group name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
  @Override
  public synchronized void removeGroup(String groupName) throws IOException {
    GroupInfo group = null;
    if(!groupMap.containsKey(groupName) || groupName.equals(GroupInfo.DEFAULT_GROUP)) {
      throw new IllegalArgumentException("Group "+groupName+" does not exist or is default group");
    }
    try {
      group = groupMap.remove(groupName);
      table.delete(new Delete(Bytes.toBytes(groupName)));
    } catch (IOException e) {
      groupMap.put(groupName, group);
      throw e;
    }
	}

  @Override
  public synchronized List<GroupInfo> listGroups() throws IOException {
    List<GroupInfo> list = Lists.newLinkedList(groupMap.values());
    list.add(getGroup(GroupInfo.DEFAULT_GROUP));
    return list;
  }

  @Override
  public boolean isOnline() {
    return groupStartupWorker.isOnline();
  }

  @Override
  public synchronized void refresh() throws IOException {
    refresh(false);
  }

  private synchronized void refresh(boolean skipCheck) throws IOException {
    if(skipCheck || isOnline()) {
      if(table == null) {
        table = new HTable(master.getConfiguration(), GROUP_TABLE_NAME_BYTES);
      }
      List<GroupInfo> groupList = new LinkedList<GroupInfo>();
      for(Result result: table.getScanner(new Scan())) {
        GroupInfo group =
            new GroupInfo(Bytes.toString(result.getRow()));
        for(byte[] server: result.getFamilyMap(SERVER_FAMILY_BYTES).keySet()) {
          group.addServer(Bytes.toString(server));
        }
        for(byte[] table: result.getFamilyMap(TABLE_FAMILY_BYTES).keySet()) {
          group.addTable(Bytes.toString(table));
        }
        groupList.add(group);
      }
      this.groupMap.clear();
      this.tableMap.clear();
      for (GroupInfo group : groupList) {
        groupMap.put(group.getName(), group);
        for(String table: group.getTables()) {
          tableMap.put(table, group);
        }
      }
    }
	}

	/**
	 * Write the configuration to HDFS.
	 *
	 * @throws IOException
	 */
	private synchronized void flushConfig() throws IOException {
    flushConfig(groupMap);
	}

	private synchronized void flushConfig(Map<String,GroupInfo> map) throws IOException {
    List<Put> puts = new LinkedList<Put>();
    for(GroupInfo groupInfo : map.values()) {
      Put put = new Put(Bytes.toBytes(groupInfo.getName()));
      put.add(INFO_FAMILY_BYTES, Bytes.toBytes("created"),
          Bytes.toBytes(System.currentTimeMillis()));
      for(String server: groupInfo.getServers()) {
        put.add(SERVER_FAMILY_BYTES, Bytes.toBytes(server), new byte[0]);
      }
      if(put.size() > 0) {
        puts.add(put);
      }
    }
    if(puts.size() > 0) {
      table.put(puts);
    }
	}

  private List<ServerName> getOnlineRS() throws IOException{
    if(master != null) {
      return master.getServerManager().getOnlineServersList();
    }
    try {
      List<ServerName> servers = new LinkedList<ServerName>();
      for (String el: ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
      return servers;
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list for zookeeper", e);
    }
  }

  private List<ServerName> getAssignedServers(){
    List<ServerName> assignedServers = Lists.newArrayList();
    for(GroupInfo gInfo : groupMap.values()){
      for(String hostPort: gInfo.getServers()) {
        assignedServers.add(ServerName.parseServerName(hostPort));
      }
    }
    return assignedServers;
  }

	List<ServerName> difference(Collection<ServerName> onlineServers,
			Collection<ServerName> servers) {
		if(servers.size() == 0){
			return Lists.newArrayList(onlineServers);
		} else {
			ArrayList<ServerName> finalList = new ArrayList<ServerName>();
			for (ServerName olServer : onlineServers) {
				ServerName actual = ServerName.findServerWithSameHostnamePort(
						servers, olServer);
				if (actual == null) {
					finalList.add(olServer);
				}
			}
			return finalList;
		}
	}

  private static class GroupStartupWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(GroupStartupWorker.class);

    private Configuration conf;
    private volatile boolean isOnline = false;
    private byte[] tableName;
    private MasterServices masterServices;
    private GroupInfoManagerImpl groupInfoManager;

    public GroupStartupWorker(GroupInfoManagerImpl groupInfoManager,
                              MasterServices masterServices, byte[] tableName) {
      this.conf = masterServices.getConfiguration();
      this.tableName = tableName;
      this.masterServices = masterServices;
      this.groupInfoManager = groupInfoManager;
      setName(GroupStartupWorker.class.getName()+"-"+masterServices.getServerName());
    }

    @Override
    public void run() {
      waitForGroupTableOnline();
      isOnline = true;
      LOG.info("GroupBasedLoadBalancer is now online");
      //balance cluster to correct the random assignments if needed
      while(masterServices.getAssignmentManager().getRegionsInTransition().size() > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.info("Sleep Interrupted", e);
        }
      }
      //TODO correct assignment for special tables here?
    }

    public void waitForGroupTableOnline() {
      final AtomicInteger regionCount = new AtomicInteger(0);
      final AtomicBoolean found = new AtomicBoolean(false);
      int assignCount = 0;
      while(!found.get()) {
        regionCount.set(0);
        found.set(true);
        try {
          MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitorBase() {
            @Override
            public boolean processRow(Result row) throws IOException {
              byte[] value = row.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.REGIONINFO_QUALIFIER);
              HRegionInfo info = Writables.getHRegionInfoOrNull(value);
              if (info != null) {
                if (Bytes.equals(tableName, info.getTableName())) {
                  value = row.getValue(HConstants.CATALOG_FAMILY,
                      HConstants.SERVER_QUALIFIER);
                  if (value == null) {
                    found.set(false);
                    return false;
                  }
                  regionCount.incrementAndGet();
                }
              }
              return true;
            }
          };
          MetaScanner.metaScan(conf, visitor);
          assignCount =
              masterServices.getAssignmentManager().getRegionsOfTable(GroupInfoManager.GROUP_TABLE_NAME_BYTES).size();
          if(regionCount.get() < 1) {
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(GroupInfoManager.SERVER_FAMILY_BYTES));
            desc.addFamily(new HColumnDescriptor(GroupInfoManager.INFO_FAMILY_BYTES));
            admin.createTable(desc);
          }
          LOG.info("isOnline: "+found.get()+", regionCount: "+regionCount.get()+", assignCount: "+assignCount);
          found.set(found.get() && assignCount == regionCount.get() && regionCount.get() > 0);
          if(found.get()) {
            groupInfoManager.refresh(true);
          }
        } catch(Exception e) {
          found.set(false);
          LOG.info("Failed to perform check",e);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.info("Sleep interrupted", e);
        }
      }
    }

    public boolean isOnline() {
      return isOnline;
    }
  }

}
