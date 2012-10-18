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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventHandler;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class GroupMoveServerHandler extends EventHandler {
	private static final Log LOG = LogFactory.getLog(GroupMoveServerHandler.class);

  private MasterServices master;
  private MoveServerPlan plan;
  private String transGroup;
  private String sourceGroup;
  private GroupInfoManager groupManager;
  private Map<String,String> serversInTransition;
  private volatile boolean success;

  public GroupMoveServerHandler(Server master, Map<String,String> serversInTransition,
                                GroupInfoManager groupManager, MoveServerPlan plan) throws IOException {
    super(master, EventType.C_M_GROUP_MOVE_SERVER);
    this.serversInTransition = serversInTransition;
    this.groupManager = groupManager;
    this.master = (MasterServices)server;
    this.plan = plan;

    synchronized (serversInTransition) {
      //check server list
      sourceGroup = groupManager.getGroupOfServer(plan.getServers().iterator().next()).getName();
      for(String server: plan.getServers()) {
        if(serversInTransition.containsKey(server)) {
          throw new DoNotRetryIOException("Server list contains a server that is already being moved: "+server);
        }
        String tmpGroup = groupManager.getGroupOfServer(server).getName();
        if(sourceGroup != null && !tmpGroup.equals(sourceGroup)) {
          throw new DoNotRetryIOException("Move server request should only come from one source group");
        }
      }
      //update the servers as in transition
      for(String server: plan.getServers()) {
        serversInTransition.put(server, plan.getTargetGroup());
      }
    }

    if(!sourceGroup.startsWith(GroupInfo.TRANSITION_GROUP_PREFIX)) {
      transGroup = GroupInfo.TRANSITION_GROUP_PREFIX+sourceGroup+"_TO_"+plan.getTargetGroup();
      groupManager.addGroup(new GroupInfo(transGroup, new TreeSet<String>()));
    }
    groupManager.moveServers(plan.getServers(), sourceGroup, transGroup);
  }

  @Override
  public void process() throws IOException {
    try {
      boolean found;
      do {
        found = false;
        for(String rs: plan.getServers()) {
          List<HRegionInfo> regions = getOnlineRegions(rs);
          LOG.info("Unassigining "+regions.size()+" from server "+rs);
          master.getAssignmentManager().unassign(regions);
          found = found || regions.size() > 0;
        }
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
        }
      } while(found);
      success = true;
      LOG.info("Move server done: "+sourceGroup+"->"+plan.getTargetGroup());
    } catch(Exception e) {
      success = false;
      LOG.error("Caught exception while running", e);
    }
  }

  private java.util.List<HRegionInfo> getOnlineRegions(String hostPort) throws IOException {
    java.util.List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for(Map.Entry<ServerName, java.util.List<HRegionInfo>> el:
        master.getAssignmentManager().getAssignments().entrySet()) {
      if(el.getKey().getHostAndPort().equals(hostPort)) {
        regions.addAll(el.getValue());
      }
    }
    return regions;
  }

  public static class MoveServerPlan {
    private Set<String> servers;
    private String targetGroup;

    public MoveServerPlan(Set<String> servers, String targetGroup) {
      this.servers = servers;
      this.targetGroup = targetGroup;
    }

    public Set<String> getServers() {
      return servers;
    }

    public String getTargetGroup() {
      return targetGroup;
    }
  }


  public MoveServerPlan getPlan() {
    return plan;
  }

  public void complete() throws IOException {
    String tmpSourceGroup = sourceGroup;
    if(transGroup != null) {
      tmpSourceGroup = transGroup;
      LOG.debug("Moving "+plan.getServers().size()+
          " servers from transition group: "+transGroup+" to final group: "+plan.getTargetGroup());
    }
    try {
      if(success) {
        groupManager.moveServers(plan.getServers(), tmpSourceGroup, plan.getTargetGroup());
        if(transGroup != null) {
          groupManager.removeGroup(transGroup);
        }
      }
    } finally {
      //remove servers in transition
      synchronized(serversInTransition) {
        for(String server: plan.getServers()) {
          serversInTransition.remove(server);
        }
      }
    }
  }
}
