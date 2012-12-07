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
package org.apache.hadoop.hbase.group;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Service to support Region Server Grouping (HBase-6721)
 * This should be installed as a Master CoprocessorEndpoint
 */
public class GroupAdminEndpoint extends BaseEndpointCoprocessor
    implements GroupAdminProtocol {
	private static final Log LOG = LogFactory.getLog(GroupAdminEndpoint.class);

  private final long threadKeepAliveTimeInMillis = 1000;
  private int threadMax = 1;
  private BlockingQueue<Runnable> threadQ;
  private MasterCoprocessorEnvironment menv;
  private MasterServices master;
  private ExecutorService executorService;
  //List of servers that are being moved from one group to another
  //Key=host:port,Value=targetGroup
  private ConcurrentMap<String,String> serversInTransition =
      new ConcurrentHashMap<String,String>();

  @Override
  public void start(CoprocessorEnvironment env) {
    menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
    threadQ = new LinkedBlockingDeque<Runnable>();
    threadMax = menv.getConfiguration().getInt("hbase.group.executor.threads", 1);
    executorService = new ThreadPoolExecutor(threadMax, threadMax,
        threadKeepAliveTimeInMillis, TimeUnit.MILLISECONDS, threadQ);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    executorService.shutdown();
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
        if (servers.contains(hostPort) && assignments.containsKey(hostPort)) {
          regions.addAll(assignments.get(hostPort));
        }
			}
		}
		return regions;
	}

  @Override
  public Collection<String> listTablesOfGroup(String groupName) throws IOException {
    return getGroupInfoManager().getGroup(groupName).getTables();
	}


  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
			return getGroupInfoManager().getGroup(groupName);
	}


  @Override
  public GroupInfo getGroupInfoOfTable(String tableName) throws IOException {
    return getGroupInfoManager().getGroup(getGroupInfoManager().getGroupOfTable(tableName));
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

    GroupMoveServerWorker.MoveServerPlan plan =
        new GroupMoveServerWorker.MoveServerPlan(servers, targetGroup);
    GroupMoveServerWorker worker = null;
    try {
      worker = new GroupMoveServerWorker(master, serversInTransition, getGroupInfoManager(), plan);
      executorService.submit(worker);
      LOG.info("GroupMoveServerHanndlerSubmitted: "+plan.getTargetGroup());
    } catch(Exception e) {
      LOG.error("Failed to submit GroupMoveServerWorker", e);
      if (worker != null) {
        worker.complete();
      }
      throw new DoNotRetryIOException("Failed to submit GroupMoveServerWorker",e);
    }
	}

  @Override
  public void moveTables(Set<String> tables, String targetGroup) throws IOException {
    getGroupInfoManager().moveTables(tables, targetGroup);
    for(String table: tables) {
      master.getAssignmentManager().unassign(
          master.getAssignmentManager().getRegionsOfTable(Bytes.toBytes(table)));
    }
  }

  @Override
  public void addGroup(String name) throws IOException {
    getGroupInfoManager().addGroup(new GroupInfo(name));
  }

  @Override
  public void removeGroup(String name) throws IOException {
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      if (listTablesOfGroup(name).size() > 0) {
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

  private GroupInfoManager getGroupInfoManager() throws IOException {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getLoadBalancer()).getGroupInfoManager();
  }

  private Map<String,List<HRegionInfo>> getOnlineRegions() throws IOException {
    Map<String,List<HRegionInfo>> result = new HashMap<String, List<HRegionInfo>>();
    for(Map.Entry<ServerName, java.util.List<HRegionInfo>> el:
        master.getAssignmentManager().getAssignments().entrySet()) {
      if (!result.containsKey(el.getKey().getHostAndPort())) {
        result.put(el.getKey().getHostAndPort(),new LinkedList<HRegionInfo>());
      }
      result.get(el.getKey().getHostAndPort()).addAll(el.getValue());
    }
    return result;
  }

}
