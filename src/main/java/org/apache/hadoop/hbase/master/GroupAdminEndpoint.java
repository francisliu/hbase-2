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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.executor.EventHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service to support Region Server Grouping (HBase-6721)
 * This should be installed as a Master CoprocessorEndpoint
 */
public class GroupAdminEndpoint extends BaseEndpointCoprocessor
    implements GroupAdminProtocol, EventHandler.EventHandlerListener {
	private static final Log LOG = LogFactory.getLog(GroupAdminEndpoint.class);

  private MasterCoprocessorEnvironment menv;
  private MasterServices master;
  //List of servers that are being moved from one group to another
  //Key=host:port,Value=targetGroup
  private ConcurrentMap<String,String> serversInTransition =
      new ConcurrentHashMap<String,String>();

  @Override
  public void start(CoprocessorEnvironment env) {
    menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
  }

  @Override
  public List<HRegionInfo> listOnlineRegionsOfGroup(String groupName) throws IOException {
		if (groupName == null) {
      throw new NullPointerException("groupName can't be null");
    }

		List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    GroupInfo groupInfo = getGroupInfoManager().getGroup(groupName);
    if (groupInfo == null) {
			return null;
		} else {
			Set<String> servers = groupInfo.getServers();
      Map<String,List<HRegionInfo>> assignments = getOnlineRegions();
      for(ServerName serverName: master.getServerManager().getOnlineServersList()) {
        String hostPort = serverName.getHostAndPort();
        if(servers.contains(hostPort) && assignments.containsKey(hostPort)) {
          regions.addAll(assignments.get(hostPort));
        }
			}
		}
		return regions;
	}

  @Override
  public Collection<String> listTablesOfGroup(String groupName) throws IOException {
		Set<String> set = new HashSet<String>();
		if (groupName == null) {
      throw new NullPointerException("groupName can't be null");
    }

    GroupInfo groupInfo = getGroupInfoManager().getGroup(groupName);
    if (groupInfo == null) {
			return null;
		} else {
      HTableDescriptor[] tables =
          master.getTableDescriptors().getAll().values().toArray(new HTableDescriptor[0]);
      for (HTableDescriptor table : tables) {
        if(GroupInfo.getGroupProperty(table).equals(groupName))
          set.add(table.getNameAsString());
      }
    }
		return set;
	}


  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
			return getGroupInfoManager().getGroup(groupName);
	}


  @Override
  public GroupInfo getGroupInfoOfTable(byte[] tableName) throws IOException {
		HTableDescriptor des;
		GroupInfo tableRSGroup;
    des =  master.getTableDescriptors().get(tableName);
		String group = GroupInfo.getGroupProperty(des);
		tableRSGroup = getGroupInfoManager().getGroup(group);
		return tableRSGroup;
	}

  @Override
  public void moveServers(Set<String> servers, String targetGroup)
			throws IOException {
		if (servers == null) {
			throw new IOException(
					"The list of servers cannot be null.");
		}
    if (StringUtils.isEmpty(targetGroup)) {
			throw new IOException(
					"The target group cannot be null.");
    }

    GroupMoveServerHandler.MoveServerPlan plan =
        new GroupMoveServerHandler.MoveServerPlan(servers, targetGroup);
    GroupMoveServerHandler handler = null;
    try {
      handler = new GroupMoveServerHandler(master, serversInTransition, getGroupInfoManager(), plan);
      handler.setListener(this);
      master.getExecutorService().submit(handler);
      LOG.info("GroupMoveServerHanndlerSubmitted: "+plan.getTargetGroup());
    } catch(Exception e) {
      LOG.error("Failed to submit GroupMoveServerHandler", e);
      if(handler != null) {
        handler.complete();
      }
      throw new DoNotRetryIOException("Failed to submit GroupMoveServerHandler",e);
    }
	}

  @Override
  public void addGroup(String name) throws IOException {
    getGroupInfoManager().addGroup(new GroupInfo(name, new HashSet<String>()));
  }

  @Override
  public void removeGroup(String name) throws IOException {
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      if(listTablesOfGroup(name).size() > 0) {
        throw new DoNotRetryIOException("Group "+name+" must have no associated tables.");
      }
      manager.removeGroup(name);
    }

  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return getGroupInfoManager().listGroups();
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return getGroupInfoManager().getGroupOfServer(hostPort);
  }

  @Override
  public Map<String, String> listServersInTransition() throws IOException {
    return Collections.unmodifiableMap(serversInTransition);
  }

  private GroupInfoManager getGroupInfoManager() {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getLoadBalancer()).getGroupInfoManager();
  }

  private Map<String,List<HRegionInfo>> getOnlineRegions() throws IOException {
    Map<String,List<HRegionInfo>> result = new HashMap<String, List<HRegionInfo>>();
    for(Map.Entry<ServerName, java.util.List<HRegionInfo>> el:
        master.getAssignmentManager().getAssignments().entrySet()) {
      if(!result.containsKey(el.getKey().getHostAndPort())) {
        result.put(el.getKey().getHostAndPort(),new LinkedList<HRegionInfo>());
      }
      result.get(el.getKey().getHostAndPort()).addAll(el.getValue());
    }
    return result;
  }

  @Override
  public void beforeProcess(EventHandler event) {
    //do nothing
  }

  @Override
  public void afterProcess(EventHandler event) {
    GroupMoveServerHandler h =
        ((GroupMoveServerHandler)event);
    try {
      h.complete();
    } catch (IOException e) {
      LOG.error("Failed to complete GroupMoveServer with "+h.getPlan().getServers().size()+
          " servers to group "+h.getPlan().getTargetGroup());
    }
  }

}
