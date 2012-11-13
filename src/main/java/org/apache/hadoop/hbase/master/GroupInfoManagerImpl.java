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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class GroupInfoManagerImpl implements GroupInfoManager {
	private static final Log LOG = LogFactory.getLog(GroupInfoManagerImpl.class);

	//Access to this map should always be synchronized.
	private Map<String, GroupInfo> groupMap;
  private MasterServices master;
  private HTable table;
  private ZooKeeperWatcher watcher;

  public GroupInfoManagerImpl(MasterServices master) throws IOException {
		this.groupMap = new ConcurrentHashMap<String, GroupInfo>();
    this.master = master;
    this.watcher = master.getZooKeeper();
    reloadConfig();
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
			GroupInfo defaultInfo = new GroupInfo(GroupInfo.DEFAULT_GROUP, new TreeSet<String>());
      List<ServerName> unassignedServers =
          difference(getOnlineRS(),getAssignedServers());
      for(ServerName serverName: unassignedServers) {
        defaultInfo.addServer(serverName.getHostAndPort());
      }
			return defaultInfo;
		} else {
			return this.groupMap.get(groupName);
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
    synchronized (groupMap) {
      try {
        group = groupMap.remove(groupName);
        table.delete(new Delete(Bytes.toBytes(groupName)));
      } catch (IOException e) {
        groupMap.put(groupName, group);
        throw e;
      }
    }
	}

  @Override
  public synchronized List<GroupInfo> listGroups() throws IOException {
    List<GroupInfo> list = Lists.newLinkedList(groupMap.values());
    list.add(getGroup(GroupInfo.DEFAULT_GROUP));
    return list;
  }

	/**
	 * Read group configuration from HDFS.
	 *
	 * @throws IOException
	 */
	synchronized void reloadConfig() throws IOException {
    if(table == null) {
      table = new HTable(master.getConfiguration(), GROUP_TABLE_NAME_BYTES);
    }
		List<GroupInfo> groupList = new LinkedList<GroupInfo>();
    for(Result result: table.getScanner(new Scan())) {
      GroupInfo group =
          new GroupInfo(Bytes.toString(result.getRow()), new HashSet<String>());
      for(byte[] server: result.getFamilyMap(SERVER_FAMILY_BYTES).keySet()) {
        group.addServer(Bytes.toString(server));
      }
      groupList.add(group);
    }
    synchronized (groupMap) {
      this.groupMap.clear();
      for (GroupInfo group : groupList) {
        groupMap.put(group.getName(), group);
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

	/**
	 * Read a list of GroupInfo.
	 *
	 * @param in
	 *            DataInput
	 * @return
	 * @throws IOException
	 */
	private static List<GroupInfo> readGroups(final InputStream in)
			throws IOException {
		List<GroupInfo> groupList = new ArrayList<GroupInfo>();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		try {
			while ((line = br.readLine()) != null && (line = line.trim()).length() > 0) {
				GroupInfo group = new GroupInfo();
				if (group.readFields(line)) {
					if (group.getName().equalsIgnoreCase(GroupInfo.DEFAULT_GROUP))
            throw new IOException("Config file contains default group!");
          groupList.add(group);
				}
			}
		} finally {
			br.close();
		}
		return groupList;
	}

	/**
	 * Write a list of group information out.
	 *
	 * @param groups
	 * @param out
	 * @throws IOException
	 */
	private static void writeGroups(Collection<GroupInfo> groups, OutputStream out)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		try {
			for (GroupInfo group : groups) {
        if (group.getName().equalsIgnoreCase(GroupInfo.DEFAULT_GROUP))
          throw new IOException("Config file contains default group!");
				group.write(bw);
			}
		} finally {
			bw.close();
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

}
