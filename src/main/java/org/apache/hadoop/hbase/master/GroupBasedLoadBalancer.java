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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.ArrayListMultimap;

public class GroupBasedLoadBalancer implements LoadBalancer {

  private static final Log LOG = LogFactory
      .getLog(GroupBasedLoadBalancer.class);
  private Configuration config;
  private ClusterStatus status;
  private MasterServices services;
  private GroupInfoManager groupManager;
  private DefaultLoadBalancer internalBalancer = new DefaultLoadBalancer();

  GroupBasedLoadBalancer() {
  }

  GroupBasedLoadBalancer(GroupInfoManager groupManager) {
    this.groupManager = groupManager;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
    internalBalancer.setConf(conf);
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.status = st;
    internalBalancer.setClusterStatus(st);
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.services = masterServices;
    internalBalancer.setMasterServices(masterServices);
    if (this.groupManager == null) {
      try {
        this.groupManager = new GroupInfoManagerImpl(
            services.getConfiguration(), services);
      } catch (IOException e) {
        LOG.warn("IOException while creating GroupInfoManagerImpl.", e);
      }
    }
  }

  @Override
  public List<RegionPlan> balanceCluster(
      Map<ServerName, List<HRegionInfo>> clusterState) {
    List<RegionPlan> regionPlans = new ArrayList<RegionPlan>();
    try {
      for (GroupInfo info : groupManager.listGroups()) {
        Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<ServerName, List<HRegionInfo>>();
        for (String sName : info.getServers()) {
          ServerName actual = ServerName.findServerWithSameHostnamePort(
              clusterState.keySet(), ServerName.parseServerName(sName));
          if (actual != null) {
            groupClusterState.put(actual, clusterState.get(actual));
          }
        }
        List<RegionPlan> groupPlans = this.internalBalancer
            .balanceCluster(groupClusterState);
        if (groupPlans != null) {
          regionPlans.addAll(groupPlans);
        }
      }
    } catch (IOException exp) {
      LOG.warn("Exception while balancing cluster.", exp);
    }
    return regionPlans;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
      ArrayListMultimap<String, HRegionInfo> regionGroup = groupRegions(regions);
      for (String groupKey : regionGroup.keys()) {
        GroupInfo info = groupManager.getGroup(groupKey);
        assignments.putAll(this.internalBalancer.roundRobinAssignment(
            regionGroup.get(groupKey), getServerToAssign(info, servers)));
      }
      return assignments;
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
      throw new IllegalStateException("Failed to access group store", e);
    }
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
      ArrayListMultimap<String, HRegionInfo> rGroup = ArrayListMultimap
          .create();
      List<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (HRegionInfo region : regions.keySet()) {
        if (misplacedRegions.contains(region) == false) {
          String groupName = groupManager.getGroupPropertyOfTable(services
              .getTableDescriptors().get(region.getTableNameAsString()));
          rGroup.put(groupName, region);
        }
      }
      // Now the "rGroup" map has only the regions which have correct
      // assignments.
      for (String key : rGroup.keys()) {
        Map<HRegionInfo, ServerName> currentAssignmentMap = new TreeMap<HRegionInfo, ServerName>();
        List<HRegionInfo> regionList = rGroup.get(key);
        GroupInfo info = groupManager.getGroup(key);
        List<ServerName> candidateList = getServerToAssign(info, servers);
        for (HRegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        assignments.putAll(this.internalBalancer.retainAssignment(
            currentAssignmentMap, candidateList));
      }

      for (HRegionInfo region : misplacedRegions) {
        String groupName = groupManager.getGroupPropertyOfTable(services
            .getTableDescriptors().get(region.getTableNameAsString()));
        GroupInfo info = groupManager.getGroup(groupName);
        List<ServerName> candidateList = getServerToAssign(info, servers);
        ServerName server = this.internalBalancer.randomAssignment(region,
            candidateList, null);
        if (assignments.containsKey(server) == false) {
          assignments.put(server, new ArrayList<HRegionInfo>());
        }
        assignments.get(server).add(region);
      }
      return assignments;
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
      throw new IllegalStateException("Failed to access group store", e);
    }
  }

  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) {
    try {
      Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();
      // Need to group regions by the group and servers and then call the
      // internal load balancer.
      ArrayListMultimap<String, HRegionInfo> regionGroups = groupRegions(regions);
      for (String key : regionGroups.keys()) {
        List<HRegionInfo> regionsOfSameGroup = regionGroups.get(key);
        GroupInfo info = groupManager.getGroup(key);
        List<ServerName> candidateList = getServerToAssign(info, servers);
        assignments.putAll(this.internalBalancer.immediateAssignment(
            regionsOfSameGroup, candidateList));
      }
      return assignments;
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
      throw new IllegalStateException("Failed to access group store", e);
    }
  }

  @Override
  public ServerName randomAssignment(HRegionInfo region,
      List<ServerName> servers, ServerName prefferedServer) {
    try {
      String tableName = region.getTableNameAsString();
      List<ServerName> candidateList;
      GroupInfo groupInfo = groupManager.getGroup(groupManager
          .getGroupPropertyOfTable(services.getTableDescriptors()
              .get(tableName)));
      candidateList = getServerToAssign(groupInfo, servers);

      if ((prefferedServer == null)
          || (groupInfo.containsServer(prefferedServer.getHostAndPort()) == false)) {
        return this.internalBalancer.randomAssignment(region, candidateList,
            null);
      } else {
        return prefferedServer;
      }
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
      throw new IllegalStateException("Failed to access group store", e);
    }
  }

  private List<ServerName> getServerToAssign(GroupInfo groupInfo,
      List<ServerName> onlineServers) {
    return filterServers(groupInfo.getServers(), onlineServers);
  }

  /**
   * Filter servers based on the online servers.
   *
   * @param servers
   *          the servers
   * @param onlineServers
   *          List of servers which are online.
   * @return the list
   */
  private List<ServerName> filterServers(Collection<String> servers,
      Collection<ServerName> onlineServers) {
    ArrayList<ServerName> finalList = new ArrayList<ServerName>();
    for (String server : servers) {
      ServerName actual = ServerName.findServerWithSameHostnamePort(
          onlineServers, ServerName.parseServerName(server));
      if (actual != null) {
        finalList.add(actual);
      }
    }
    return finalList;
  }

  private ArrayListMultimap<String, HRegionInfo> groupRegions(
      List<HRegionInfo> regionList) throws IOException {
    ArrayListMultimap<String, HRegionInfo> regionGroup = ArrayListMultimap
        .create();
    for (HRegionInfo region : regionList) {
      String groupName = groupManager.getGroupPropertyOfTable(services
          .getTableDescriptors().get(region.getTableNameAsString()));
      regionGroup.put(groupName, region);
    }
    return regionGroup;
  }

  private List<HRegionInfo> getMisplacedRegions(
      Map<HRegionInfo, ServerName> regions) throws IOException {
    List<HRegionInfo> misplacedRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo region : regions.keySet()) {
      ServerName assignedServer = regions.get(region);
      GroupInfo info = groupManager.getGroup(groupManager
          .getGroupPropertyOfTable(services.getTableDescriptors().get(
              region.getTableNameAsString())));
      if (!info.containsServer(assignedServer.getHostAndPort())) {
        misplacedRegions.add(region);
      }
    }
    return misplacedRegions;
  }

  public GroupInfoManager getGroupInfoManager() {
    return groupManager;
  }
}
