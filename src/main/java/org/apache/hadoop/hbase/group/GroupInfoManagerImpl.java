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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
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
  private Set<String> prevGroups;


  public GroupInfoManagerImpl(MasterServices master) throws IOException {
		this.groupMap = new ConcurrentHashMap<String, GroupInfo>();
		this.tableMap = new ConcurrentHashMap<String, GroupInfo>();
    this.master = master;
    this.watcher = master.getZooKeeper();
    groupStartupWorker = new GroupStartupWorker(this, master, GROUP_TABLE_NAME_BYTES);
    prevGroups = new HashSet<String>();
    refresh();
    groupStartupWorker.start();
  }

	/**
	 * Adds the group.
	 *
	 * @param groupInfo the group name
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
      refresh();
      throw e;
    }
	}

  @Override
  public synchronized boolean moveServers(Set<String> hostPort, String srcGroup, String dstGroup) throws IOException {
    GroupInfo src = new GroupInfo(getGroup(srcGroup));
    GroupInfo dst = new GroupInfo(getGroup(dstGroup));
    boolean foundOne = false;
    for(String el: hostPort) {
      foundOne = src.removeServer(el) || foundOne;
      dst.addServer(el);
    }

    Map<String,GroupInfo> newMap = Maps.newHashMap(groupMap);
    if (!src.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      newMap.put(src.getName(), src);
    }
    if (!dst.getName().equals(GroupInfo.DEFAULT_GROUP)) {
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
			if (info.containsServer(hostPort)){
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
        if (!tableMap.containsKey(tableName)) {
          defaultInfo.addTable(tableName);
        }
      }
      for(String tableName: GroupBasedLoadBalancer.SPECIAL_TABLES) {
        if (!tableMap.containsKey(tableName)) {
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
    if (tableMap.containsKey(tableName)) {
      return tableMap.get(tableName).getName();
    }
    return GroupInfo.DEFAULT_GROUP;
  }

  @Override
  public synchronized void moveTables(Set<String> tableNames, String groupName) throws IOException {
    for(String tableName: tableNames) {
      moveTable(tableName, groupName);
    }
    try {
      flushConfig();
    } catch(IOException e) {
      LOG.error("Failed to update store", e);
      refresh();
      throw e;
    }
  }

  private synchronized void moveTable(String tableName, String groupName) throws IOException {
    if (!GroupInfo.DEFAULT_GROUP.equals(groupName) && !groupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is default group");
    }
    if (tableMap.containsKey(tableName)) {
      tableMap.get(tableName).removeTable(tableName);
      tableMap.remove(tableName);
    }
    if (!GroupInfo.DEFAULT_GROUP.equals(groupName)) {
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
    if (!groupMap.containsKey(groupName) || groupName.equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is default group");
    }
    GroupInfo groupInfo = null;
    try {
      groupInfo = groupMap.remove(groupName);
      flushConfig();
    } catch(IOException e) {
      groupMap.put(groupName, groupInfo);
      refresh();
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

  private synchronized void refresh(boolean forceOnline) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    List<GroupInfo> groupList = new LinkedList<GroupInfo>();

    //if online read from GROUP table
    if (forceOnline || isOnline()) {
      if (table == null) {
        table = new HTable(master.getConfiguration(), GROUP_TABLE_NAME_BYTES);
      }
      Result result = table.get(new Get(ROW_KEY));
      if(!result.isEmpty()) {
        NavigableMap<byte[],NavigableMap<byte[],byte[]>> dataMap = result.getNoVersionMap();
        for(byte[] groupName: dataMap.get(SERVER_FAMILY_BYTES).keySet()) {
          NavigableSet<String> servers =
              mapper.readValue(Bytes.toString(dataMap.get(SERVER_FAMILY_BYTES).get(groupName)),
                  new TypeReference<TreeSet<String>>() {});
          NavigableSet<String> tables =
              mapper.readValue(Bytes.toString(dataMap.get(TABLE_FAMILY_BYTES).get(groupName)),
                  new TypeReference<TreeSet<String>>() {});
          GroupInfo group = new GroupInfo(Bytes.toString(groupName), servers, tables);
          groupList.add(group);
        }
      }
    }
    //Overwrite and info stored by table, this takes precedence
    String groupPath = ZKUtil.joinZNode(watcher.baseZNode,groupZNode);
    try {
      if(ZKUtil.checkExists(watcher,groupPath) != -1) {
        byte[] data = ZKUtil.getData(watcher, groupPath);
        LOG.debug("Reading ZK GroupInfo:" + Bytes.toString(data));
        groupList.addAll(
            (List<GroupInfo>) mapper.readValue(data,new TypeReference<List<GroupInfo>>(){}));
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to write to groupZNode",e);
    }
    //populate the data
    this.groupMap.clear();
    this.tableMap.clear();
    for (GroupInfo group : groupList) {
      if(!(group.getName().equals(GroupInfo.OFFLINE_DEFAULT_GROUP) && (isOnline() || forceOnline))) {
        groupMap.put(group.getName(), group);
        for(String table: group.getTables()) {
          tableMap.put(table, group);
        }
      }
    }
    prevGroups.clear();
    prevGroups.addAll(groupMap.keySet());
	}

	/**
	 * Write the configuration to HDFS.
	 *
	 * @throws IOException
	 */
	private synchronized void flushConfig() throws IOException {
    flushConfig(groupMap);
	}

	private synchronized void flushConfig(Map<String,GroupInfo> newGroupMap) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    List<GroupInfo> zGroup = new LinkedList<GroupInfo>();
    Put put = new Put(ROW_KEY);
    Delete delete = new Delete(ROW_KEY);

    //populate deletes
    for(String groupName : prevGroups) {
      if(!newGroupMap.containsKey(groupName)) {
        delete.deleteColumns(TABLE_FAMILY_BYTES, Bytes.toBytes(groupName));
        delete.deleteColumns(SERVER_FAMILY_BYTES, Bytes.toBytes(groupName));
      }
    }

    //populate puts
    for(GroupInfo groupInfo : newGroupMap.values()) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      mapper.writeValue(bos, groupInfo.getServers());
      put.add(SERVER_FAMILY_BYTES,
          Bytes.toBytes(groupInfo.getName()),
          bos.toByteArray());
      bos = new ByteArrayOutputStream();
      mapper.writeValue(bos, groupInfo.getTables());
      put.add(TABLE_FAMILY_BYTES,
          Bytes.toBytes(groupInfo.getName()),
          bos.toByteArray());
      for(String special: GroupBasedLoadBalancer.SPECIAL_TABLES) {
        if (groupInfo.getTables().contains(special)) {
          zGroup.add(groupInfo);
          break;
        }
      }
    }

    //copy default group to offline group
    GroupInfo defaultGroup = getGroup(GroupInfo.DEFAULT_GROUP);
    GroupInfo offlineGroup = new GroupInfo(GroupInfo.OFFLINE_DEFAULT_GROUP);
    offlineGroup.addAllServers(defaultGroup.getServers());
    offlineGroup.addAllTables(defaultGroup.getTables());
    zGroup.add(offlineGroup);
    //Write zk data first since that's what we'll read first
    String groupPath = ZKUtil.joinZNode(watcher.baseZNode,groupZNode);
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      mapper.writeValue(bos, zGroup);
      LOG.debug("Writing ZK GroupInfo:" + Bytes.toString(bos.toByteArray()));
      ZKUtil.createSetData(watcher, groupPath, bos.toByteArray());
    } catch (KeeperException e) {
      throw new IOException("Failed to write to groupZNode",e);
    }

    RowMutations rowMutations = new RowMutations(ROW_KEY);
    if(put.size() > 0) {
      rowMutations.add(put);
    }
    if(delete.size() > 0) {
      rowMutations.add(delete);
    }
    if(rowMutations.getMutations().size() > 0) {
      table.mutateRow(rowMutations);
    }

    prevGroups.clear();
    prevGroups.addAll(newGroupMap.keySet());
  }

  private List<ServerName> getOnlineRS() throws IOException{
    if (master != null) {
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
		if (servers.size() == 0){
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
      setDaemon(true);
    }

    @Override
    public void run() {
      if(waitForGroupTableOnline()) {
        isOnline = true;
        LOG.info("GroupBasedLoadBalancer is now online");
      }
    }

    public boolean waitForGroupTableOnline() {
      final List<HRegionInfo> foundRegions = new LinkedList<HRegionInfo>();
      final AtomicBoolean found = new AtomicBoolean(false);
      while(!found.get() && isMasterRunning()) {
        foundRegions.clear();
        found.set(true);
        try {
          if(masterServices.getCatalogTracker().verifyRootRegionLocation(1) &&
              masterServices.getCatalogTracker().verifyMetaRegionLocation(1)) {
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
                    }
                    foundRegions.add(info);
                  }
                }
                return true;
              }
            };
            MetaScanner.metaScan(conf, visitor);
            List<HRegionInfo> assignedRegions
                = masterServices.getAssignmentManager().getRegionsOfTable(GROUP_TABLE_NAME_BYTES);
            if(assignedRegions == null) {
              assignedRegions = new LinkedList<HRegionInfo>();
            }
            //if no regions in meta then we have to create the table
            if (foundRegions.size() < 1 &&
                !MetaReader.tableExists(masterServices.getCatalogTracker(), GROUP_TABLE_NAME)) {
              groupInfoManager.createGroupTable(masterServices);
            }
            LOG.info("isOnline: "+found.get()+", regionCount: "+foundRegions.size()+
                ", assignCount: "+assignedRegions.size());
            found.set(found.get() && assignedRegions.size() == foundRegions.size() && foundRegions.size() > 0);
          } else {
            LOG.info("Waiting for catalog tables to come online");
            found.set(false);
          }
          if (found.get()) {
            groupInfoManager.refresh(true);
            //flush any inconsistencies between ZK and HTable
            groupInfoManager.flushConfig();
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
      return found.get();
    }

    public boolean isOnline() {
      return isOnline;
    }

    private boolean isMasterRunning() {
      return !masterServices.isAborted() && !masterServices.isStopped();
    }
  }

  private void createGroupTable(MasterServices masterServices) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(GROUP_TABLE_NAME_BYTES);
    desc.addFamily(new HColumnDescriptor(SERVER_FAMILY_BYTES));
    desc.addFamily(new HColumnDescriptor(TABLE_FAMILY_BYTES));
    desc.setMaxFileSize(1l << 32);
    HRegionInfo newRegions[] = new HRegionInfo[]{
          new HRegionInfo(desc.getName(), null, null)};
    //we need to create the table this way to bypass
    //checkInitialized
    masterServices.getExecutorService()
        .submit(new CreateTableHandler(masterServices,
            masterServices.getMasterFileSystem(),
            masterServices.getServerManager(),
            desc,
            masterServices.getConfiguration(),
            newRegions,
            masterServices.getCatalogTracker(),
            masterServices.getAssignmentManager()));
    //need this or else region won't be assigned
    masterServices.getAssignmentManager().assign(newRegions[0], false);
  }

}
