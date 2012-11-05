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

import com.google.common.collect.ListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * GroupBasedLoadBalancer, used when Region Server Grouping is configured (HBase-6721)
 * It does region balance based on a table's group membership.
 */
public class GroupBasedLoadBalancer implements LoadBalancer {
  /** Config for pluggable load balancers */
  public static final String HBASE_GROUP_LOADBALANCER_CLASS = "hbase.groups.grouploadbalancer.class";

  private static final Log LOG = LogFactory
      .getLog(GroupBasedLoadBalancer.class);
  private Configuration config;
  private ClusterStatus clusterStatus;
  private MasterServices masterServices;
  private GroupInfoManager groupManager;
  private LoadBalancer internalBalancer;

  GroupBasedLoadBalancer() {
    this(null);
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
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.clusterStatus = st;
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override
  public List<RegionPlan> balanceCluster(
      Map<ServerName, List<HRegionInfo>> clusterState) {
    Map<ServerName,List<HRegionInfo>> correctedState = correctAssignments(clusterState);
    List<RegionPlan> regionPlans = new ArrayList<RegionPlan>();
    try {
      for (GroupInfo info : groupManager.listGroups()) {
        Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<ServerName, List<HRegionInfo>>();
        for (String sName : info.getServers()) {
          ServerName actual = ServerName.findServerWithSameHostnamePort(
              clusterState.keySet(), ServerName.parseServerName(sName));
          if (actual != null) {
            groupClusterState.put(actual, correctedState.get(actual));
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
      ListMultimap<String, HRegionInfo> regionGroup = groupRegions(regions);
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
      ListMultimap<String, HRegionInfo> rGroup = ArrayListMultimap.create();
      List<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (HRegionInfo region : regions.keySet()) {
        if (misplacedRegions.contains(region) == false) {
          String groupName = masterServices
              .getTableDescriptors().get(region.getTableNameAsString()).getValue(GroupInfo.GROUP_KEY);
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
          String groupName = masterServices
              .getTableDescriptors().get(region.getTableNameAsString()).getValue(GroupInfo.GROUP_KEY);
        GroupInfo info = groupManager.getGroup(groupName);
        List<ServerName> candidateList = getServerToAssign(info, servers);
        ServerName server = this.internalBalancer.randomAssignment(region,
            candidateList);
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
      ListMultimap<String, HRegionInfo> regionGroups = groupRegions(regions);
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
      List<ServerName> servers) {
    try {
      String tableName = region.getTableNameAsString();
      List<ServerName> candidateList;
      GroupInfo groupInfo = groupManager.getGroup(GroupInfo
          .getGroupProperty(masterServices.getTableDescriptors()
              .get(tableName)));
      candidateList = getServerToAssign(groupInfo, servers);
      return this.internalBalancer.randomAssignment(region, candidateList);
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
      throw new IllegalStateException("Failed to access group store", e);
    }
  }

  private List<ServerName> getServerToAssign(GroupInfo groupInfo,
      List<ServerName> onlineServers) {
    if (groupInfo != null) {
      return filterServers(groupInfo.getServers(), onlineServers);
    } else {
      LOG.debug("Group Information found to be null. Some regions might be unassigned.");
      return new ArrayList<ServerName>();
    }
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

  private ListMultimap<String, HRegionInfo> groupRegions(
      List<HRegionInfo> regionList) throws IOException {
    ListMultimap<String, HRegionInfo> regionGroup = ArrayListMultimap
        .create();
    for (HRegionInfo region : regionList) {
      String groupName = GroupInfo.getGroupProperty(masterServices
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
      GroupInfo info = groupManager.getGroup(GroupInfo
          .getGroupProperty(masterServices.getTableDescriptors().get(
              region.getTableNameAsString())));
      if ((info == null)|| (!info.containsServer(assignedServer.getHostAndPort()))) {
        misplacedRegions.add(region);
      }
    }
    return misplacedRegions;
  }

  private Map<ServerName, List<HRegionInfo>> correctAssignments(
       Map<ServerName, List<HRegionInfo>> existingAssignments){
    Map<ServerName, List<HRegionInfo>> correctAssignments = new TreeMap<ServerName, List<HRegionInfo>>();
    List<HRegionInfo> misplacedRegions = new ArrayList<HRegionInfo>();
    for (ServerName sName : existingAssignments.keySet()) {
      correctAssignments.put(sName, new ArrayList<HRegionInfo>());
      List<HRegionInfo> regions = existingAssignments.get(sName);
      for (HRegionInfo region : regions) {
        GroupInfo info = null;
        try {
          info = groupManager.getGroup(GroupInfo.getGroupProperty(masterServices
              .getTableDescriptors().get(region.getTableNameAsString())));
        }catch(IOException exp){
          LOG.debug("Group information null for region of table " + region.getTableNameAsString(),
              exp);
        }
        if ((info == null) || (!info.containsServer(sName.getHostAndPort()))) {
          // Misplaced region.
          misplacedRegions.add(region);
        } else {
          correctAssignments.get(sName).add(region);
        }
      }
    }

    //unassign misplaced regions, so that they are assigned to correct groups.
    this.masterServices.getAssignmentManager().unassign(misplacedRegions);
    return correctAssignments;
  }

  GroupInfoManager getGroupInfoManager() {
    return groupManager;
  }

  @Override
  public void configure() throws IOException {
    if (this.groupManager == null) {
        this.groupManager = new GroupInfoManagerImpl(masterServices.getConfiguration(), masterServices);
    }
    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass = config.getClass(
        HBASE_GROUP_LOADBALANCER_CLASS,
        DefaultLoadBalancer.class, LoadBalancer.class);
    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    internalBalancer.setClusterStatus(clusterStatus);
    internalBalancer.setMasterServices(masterServices);
    internalBalancer.setConf(config);
    internalBalancer.configure();
  }
}
