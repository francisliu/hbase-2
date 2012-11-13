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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * GroupBasedLoadBalancer, used when Region Server Grouping is configured (HBase-6721)
 * It does region balance based on a table's group membership.
 *
 * Most assignment methods contain two exclusive code paths: Online - when the group
 * table is online and Offline - when it is unavailable.
 *
 * During Offline assignments are done randomly irrespective of group memebership.
 * Though only the catalog tables and the group talbes are given non-empty/null assignments.
 *
 */
public class GroupBasedLoadBalancer implements LoadBalancer {
  /** Config for pluggable load balancers */
  public static final String HBASE_GROUP_LOADBALANCER_CLASS = "hbase.group.grouploadbalancer.class";

  private static final Log LOG = LogFactory.getLog(GroupBasedLoadBalancer.class);

  private static final TreeSet<byte[]> SPECIAL_TABLES = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  static {
    SPECIAL_TABLES.add(HConstants.ROOT_TABLE_NAME);
    SPECIAL_TABLES.add(HConstants.META_TABLE_NAME);
    SPECIAL_TABLES.add(GroupInfoManager.GROUP_TABLE_NAME_BYTES);
  }

  private Configuration config;
  private ClusterStatus clusterStatus;
  private MasterServices masterServices;
  private GroupInfoManager groupManager;
  private LoadBalancer internalBalancer;
  private GroupStartupWorker groupStartupWorker;

  //used during reflection by LoadBalancerFactory
  public GroupBasedLoadBalancer() {
  }

  //This constructor should only be used for unit testing
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
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {

    if(!isOnline()) {
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
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
      if(isOnline()) {
        ListMultimap<String, HRegionInfo> regionGroup = groupRegions(regions);
        for (String groupKey : regionGroup.keys()) {
          GroupInfo info = groupManager.getGroup(groupKey);
          assignments.putAll(this.internalBalancer.roundRobinAssignment(
              regionGroup.get(groupKey), getServerToAssign(info, servers)));
        }
      } else {
        List<HRegionInfo> filtered  = new LinkedList<HRegionInfo>();
        List<HRegionInfo> nulled = new LinkedList<HRegionInfo>();
        for (HRegionInfo region : regions) {
          if(SPECIAL_TABLES.contains(region.getTableName())) {
            filtered.add(region);
          } else {
            nulled.add(region);
          }

          assignments.putAll(this.internalBalancer.roundRobinAssignment(filtered, servers));
        }
        if(nulled.size() > 0) {
          //we need to use a bogus address since key can't be null
          assignments.put(ServerName.parseServerName("127.0.0.1:1"),regions);
        }
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
    if(!isOnline()) {
      return offlineRetainAssignment(regions, servers);
    }
    return onlineRetainAssignment(regions, servers);
  }

  public Map<ServerName, List<HRegionInfo>> offlineRetainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    //we will just keep assignments even if they are incorrect
    //chances are most are not, then we just use balance to correct
    //we need to correct catalog and group assignment anyway
    return internalBalancer.retainAssignment(regions, servers);
  }

  public Map<ServerName, List<HRegionInfo>> onlineRetainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
      ListMultimap<String, HRegionInfo> rGroup = ArrayListMultimap.create();
      List<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (HRegionInfo region : regions.keySet()) {
        if (misplacedRegions.contains(region) == false) {
          String groupName = GroupInfo.getGroupProperty(masterServices.getTableDescriptors().get(
            region.getTableNameAsString()));
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
        String groupName = GroupInfo.getGroupProperty(masterServices.getTableDescriptors().get(
            region.getTableNameAsString()));
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
      if(isOnline()) {
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
      } else {
        List<HRegionInfo> filtered = new LinkedList<HRegionInfo>();
        for(HRegionInfo region: regions) {
          if(SPECIAL_TABLES.contains(region.getTableName())) {
            filtered.add(region);
          }
          assignments.put(region, null);
        }
        assignments.putAll(this.internalBalancer.immediateAssignment(filtered, servers));
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
      List<ServerName> candidateList = Collections.EMPTY_LIST;
      if(isOnline()) {
        GroupInfo groupInfo = groupManager.getGroup(GroupInfo
            .getGroupProperty(masterServices.getTableDescriptors()
                .get(tableName)));
        candidateList = getServerToAssign(groupInfo, servers);
      } else if(SPECIAL_TABLES.contains(region.getTableName())){
        candidateList = servers;
      }
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
    if(groupManager == null) {
      groupStartupWorker = new GroupStartupWorker(masterServices, GroupInfoManager.GROUP_TABLE_NAME_BYTES);
      groupStartupWorker.start();
    }
  }

  public boolean isOnline() {
    try {
      return (getGroupInfoManager() != null);
    } catch (IOException e) {
      LOG.error("Failed to verify isOnline",e );
    }
    return false;
  }

  GroupInfoManager getGroupInfoManager() throws IOException {
    if (this.groupManager == null) {
      if(groupStartupWorker.isOnline()) {
        this.groupManager = new GroupInfoManagerImpl(masterServices);
      }
    }
    return groupManager;
  }

  private static class GroupStartupWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(GroupStartupWorker.class);

    private Configuration conf;
    private volatile boolean isOnline = false;
    private byte[] tableName;
    private MasterServices masterServices;

    public GroupStartupWorker(MasterServices masterServices, byte[] tableName) {
      this.conf = masterServices.getConfiguration();
      this.tableName = tableName;
      this.masterServices = masterServices;
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
            ((GroupBasedLoadBalancer)masterServices.getLoadBalancer()).getGroupInfoManager();
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
