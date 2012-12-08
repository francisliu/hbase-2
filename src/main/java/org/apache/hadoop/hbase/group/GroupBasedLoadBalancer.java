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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.hbase.master.DefaultLoadBalancer;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * GroupBasedLoadBalancer, used when Region Server Grouping is configured (HBase-6721)
 * It does region balance based on a table's group membership.
 *
 * Most assignment methods contain two exclusive code paths: Online - when the group
 * table is online and Offline - when it is unavailable.
 *
 * During Offline, assignments are made randomly irrespective of group memebership.
 * Though during this mode, only the tables contained in SPECIAL_TABLES
 * are given assignments to actual online servers.
 * Once the GROUP table has been assigned, the balancer switches to Online and will then
 * start providing appropriate assignments for user tables.
 *
 * An optmization has been added to cache the group information for SPECIAL_TABLES in zookeeper,
 * thus random assignments will only occur during first time a cluster is started.
 *
 */
@InterfaceAudience.Public
public class GroupBasedLoadBalancer implements LoadBalancer {
  /** Config for pluggable load balancers */
  public static final String HBASE_GROUP_LOADBALANCER_CLASS = "hbase.group.grouploadbalancer.class";

  private static final Log LOG = LogFactory.getLog(GroupBasedLoadBalancer.class);
  private static final ServerName BOGUS_SERVER_NAME = ServerName.parseServerName("127.0.0.1:1");

  public static final Set<String> SPECIAL_TABLES = new HashSet<String>();
  static {
    SPECIAL_TABLES.add(Bytes.toString(HConstants.ROOT_TABLE_NAME));
    SPECIAL_TABLES.add(Bytes.toString(HConstants.META_TABLE_NAME));
    SPECIAL_TABLES.add(GroupInfoManager.GROUP_TABLE_NAME);
  }

  private Configuration config;
  private ClusterStatus clusterStatus;
  private MasterServices masterServices;
  private GroupInfoManager groupManager;
  private LoadBalancer internalBalancer;

  //used during reflection by LoadBalancerFactory
  @InterfaceAudience.Private
  public GroupBasedLoadBalancer() {
  }

  //This constructor should only be used for unit testing
  @InterfaceAudience.Private
  public GroupBasedLoadBalancer(GroupInfoManager groupManager) {
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
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {

    if (!isOnline()) {
      throw new IllegalStateException(GroupInfoManager.GROUP_TABLE_NAME+
          " is not online, unable to perform balance");
    }

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
    Map<ServerName, List<HRegionInfo>> assignments = Maps.newHashMap();
    try {
      ListMultimap<String,HRegionInfo> regionMap = LinkedListMultimap.create();
      ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
      generateGroupMaps(regions, servers, regionMap, serverMap);
      for(String groupKey : regionMap.keySet()) {
        if (regionMap.get(groupKey).size() > 0) {
          assignments.putAll(
              this.internalBalancer.roundRobinAssignment(
                  regionMap.get(groupKey),
                  serverMap.get(groupKey)));
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
    }
    return assignments;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    if (!isOnline()) {
      //We will just keep assignments even if they are incorrect.
      //Chances are most will be assigned correctly.
      //Then we just use balance to correct the misplaced few.
      //we need to correct catalog and group table assignment anyway.
      return internalBalancer.retainAssignment(regions, servers);
    }
    return onlineRetainAssignment(regions, servers);
  }

  public Map<ServerName, List<HRegionInfo>> onlineRetainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
      ListMultimap<String, HRegionInfo> groupToRegion = ArrayListMultimap.create();
      List<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (HRegionInfo region : regions.keySet()) {
        if (!misplacedRegions.contains(region)) {
          String groupName = groupManager.getGroupOfTable(region.getTableNameAsString());
          groupToRegion.put(groupName, region);
        }
      }
      // Now the "groupToRegion" map has only the regions which have correct
      // assignments.
      for (String key : groupToRegion.keys()) {
        Map<HRegionInfo, ServerName> currentAssignmentMap = new TreeMap<HRegionInfo, ServerName>();
        List<HRegionInfo> regionList = groupToRegion.get(key);
        GroupInfo info = groupManager.getGroup(key);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        for (HRegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        assignments.putAll(this.internalBalancer.retainAssignment(
            currentAssignmentMap, candidateList));
      }

      for (HRegionInfo region : misplacedRegions) {
        String groupName = groupManager.getGroupOfTable(
            region.getTableNameAsString());
        GroupInfo info = groupManager.getGroup(groupName);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
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
    Map<HRegionInfo,ServerName> assignments = Maps.newHashMap();
    try {
      ListMultimap<String,HRegionInfo> regionMap = LinkedListMultimap.create();
      ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
      generateGroupMaps(regions, servers, regionMap, serverMap);
      for(String groupKey : regionMap.keySet()) {
        if (regionMap.get(groupKey).size() > 0) {
          assignments.putAll(
              this.internalBalancer.immediateAssignment(
                  regionMap.get(groupKey),
                  serverMap.get(groupKey)));
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
    }
    return assignments;
  }

  @Override
  public ServerName randomAssignment(HRegionInfo region,
      List<ServerName> servers) {
    try {
      ListMultimap<String,HRegionInfo> regionMap = LinkedListMultimap.create();
      ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
      generateGroupMaps(Lists.newArrayList(region), servers, regionMap, serverMap);
      List<ServerName> filteredServers = serverMap.get(regionMap.keySet().iterator().next());
      return this.internalBalancer.randomAssignment(region, filteredServers);
    } catch (IOException e) {
      LOG.error("Failed to access group store", e);
    }
    return null;
  }

  private void generateGroupMaps(
    List<HRegionInfo> regions,
    List<ServerName> servers,
    ListMultimap<String, HRegionInfo> regionMap,
    ListMultimap<String, ServerName> serverMap) throws IOException {
    if (isOnline()) {
      for (HRegionInfo region : regions) {
        String groupName = groupManager.getGroupOfTable(region.getTableNameAsString());
        regionMap.put(groupName, region);
      }
      for (String groupKey : regionMap.keys()) {
        GroupInfo info = groupManager.getGroup(groupKey);
        serverMap.putAll(groupKey, filterOfflineServers(info, servers));
      }
    } else {
      String nullGroup = "_null";
      //populate serverMap
      for(GroupInfo groupInfo: groupManager.listGroups()) {
        serverMap.putAll(groupInfo.getName(), filterOfflineServers(groupInfo, servers));
      }
      //Add bogus server
      serverMap.put(nullGroup, BOGUS_SERVER_NAME);
      //group regions
      for (HRegionInfo region : regions) {
        //Even though some of the non-special tables may be part of the cached groups.
        //We don't assign them here.
        if(SPECIAL_TABLES.contains(region.getTableNameAsString())) {
          regionMap.put(groupManager.getGroupOfTable(region.getTableNameAsString()),region);
        } else {
          regionMap.put(nullGroup,region);
        }
      }
    }
  }

  private List<ServerName> filterOfflineServers(GroupInfo groupInfo,
                                                List<ServerName> onlineServers) {
    if (groupInfo != null) {
      return filterServers(groupInfo.getServers(), onlineServers);
    } else {
      LOG.debug("Group Information found to be null. Some regions might be unassigned.");
      return Collections.EMPTY_LIST;
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
      String groupName = groupManager.getGroupOfTable(region.getTableNameAsString());
      regionGroup.put(groupName, region);
    }
    return regionGroup;
  }

  private List<HRegionInfo> getMisplacedRegions(
      Map<HRegionInfo, ServerName> regions) throws IOException {
    List<HRegionInfo> misplacedRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo region : regions.keySet()) {
      ServerName assignedServer = regions.get(region);
      GroupInfo info = groupManager.getGroup(groupManager.getGroupOfTable(region.getTableNameAsString()));
      if ((info == null)|| (!info.containsServer(assignedServer.getHostAndPort()))) {
        misplacedRegions.add(region);
      }
    }
    return misplacedRegions;
  }

  private Map<ServerName, List<HRegionInfo>> correctAssignments(
       Map<ServerName, List<HRegionInfo>> existingAssignments){
    Map<ServerName, List<HRegionInfo>> correctAssignments = new TreeMap<ServerName, List<HRegionInfo>>();
    List<HRegionInfo> misplacedRegions = new LinkedList<HRegionInfo>();
    for (ServerName sName : existingAssignments.keySet()) {
      correctAssignments.put(sName, new LinkedList<HRegionInfo>());
      List<HRegionInfo> regions = existingAssignments.get(sName);
      for (HRegionInfo region : regions) {
        GroupInfo info = null;
        try {
          info = groupManager.getGroup(groupManager.getGroupOfTable(region.getTableNameAsString()));
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

  @Override
  public void configure() throws IOException {
    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass = config.getClass(
        HBASE_GROUP_LOADBALANCER_CLASS,
        DefaultLoadBalancer.class, LoadBalancer.class);
    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    internalBalancer.setClusterStatus(clusterStatus);
    internalBalancer.setMasterServices(masterServices);
    internalBalancer.setConf(config);
    internalBalancer.configure();
    //this will only happen if the unit tests constructor is used
    if (groupManager == null) {
      groupManager = new GroupInfoManagerImpl(masterServices);
    }
  }

  public boolean isOnline() {
    return groupManager != null && groupManager.isOnline();
  }

  @InterfaceAudience.Private
  public GroupInfoManager getGroupInfoManager() throws IOException {
    return groupManager;
  }
}
