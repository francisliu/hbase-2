/**
 * Copyright 2010 The Apache Software Foundation
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

import com.google.common.collect.Sets;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.group.GroupAdminClient;
import org.apache.hadoop.hbase.group.GroupAdminEndpoint;
import org.apache.hadoop.hbase.group.GroupBasedLoadBalancer;
import org.apache.hadoop.hbase.group.GroupInfo;
import org.apache.hadoop.hbase.group.GroupInfoManager;
import org.apache.hadoop.hbase.group.GroupMasterObserver;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestGroupsOfflineMode {
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(TestGroupsOfflineMode.class);
	private static HBaseTestingUtility TEST_UTIL;
	private static HMaster master;
  private static HBaseAdmin admin;
  private static MiniHBaseCluster cluster;

	@BeforeClass
	public static void setUp() throws Exception {
		TEST_UTIL = new HBaseTestingUtility();
		TEST_UTIL.getConfiguration().set(
				HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
				GroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.master.classes",
        GroupMasterObserver.class.getName()+","+
        GroupAdminEndpoint.class.getName());
		TEST_UTIL.getConfiguration().set(
				ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
				"1");
		TEST_UTIL.startMiniCluster(2, 3);
		cluster = TEST_UTIL.getHBaseCluster();
		master = cluster.getMaster();
    master.balanceSwitch(false);
    admin = TEST_UTIL.getHBaseAdmin();
    //wait till the balancer is in online mode
    while(!((GroupBasedLoadBalancer)master.getLoadBalancer()).isOnline() ||
          cluster.getMaster().getServerManager().getOnlineServers().size() < 2) {
      Thread.sleep(100);
    }
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

  @Test
  public void testOffline() throws IOException, InterruptedException {
    //table should be after group table name
    //so it gets assigned later
    String testTable = GroupInfoManager.GROUP_TABLE_NAME+"1";
    TEST_UTIL.createTable(Bytes.toBytes(testTable), Bytes.toBytes("f"));

    //adding testTable to special group so it gets assigned during offline mode
    GroupBasedLoadBalancer.SPECIAL_TABLES.add(testTable);

    GroupAdminClient groupAdmin = new GroupAdminClient(TEST_UTIL.getConfiguration());

    String newGroup =  "my_group";
    groupAdmin.addGroup(newGroup);
    for(HRegionInfo  regionInfo:
        cluster.getMaster().getAssignmentManager().getAssignments().get(cluster.getRegionServer(2).getServerName())) {
      cluster.getMaster().move(regionInfo.getEncodedNameAsBytes(),
          Bytes.toBytes(cluster.getRegionServer(0).getServerName().getServerName()));
    }
    LOG.info("Waiting for region unassignments on failover RS...");
    while(cluster.getMaster().getAssignmentManager().getAssignments().get(cluster.getRegionServer(2).getServerName()).size() > 0) {
      Thread.sleep(100);
    }

    String newGroupHost = cluster.getRegionServer(1).getServerName().getHostname();
    int newGroupPort = TEST_UTIL.getHBaseCluster().getRegionServer(1).getServerName().getPort();
    groupAdmin.moveServers(Sets.newHashSet(newGroupHost+":"+newGroupPort), newGroup);
    HRegionInterface rs = admin.getConnection().getHRegionConnection(newGroupHost, newGroupPort);
    //move server to group and make sure all tables are assigned
    while (rs.getOnlineRegions().size() > 0 ||
        groupAdmin.listOnlineRegionsOfGroup(GroupInfo.DEFAULT_GROUP).size() != TEST_UTIL.getMetaTableRows().size()+2) {
      Thread.sleep(100);
    }
    //move table to group and wait
    groupAdmin.moveTables(Sets.newHashSet(GroupInfoManager.GROUP_TABLE_NAME), newGroup);
    LOG.info("Waiting for move table...");
    while (rs.getOnlineRegions().size() < 1) {
      Thread.sleep(100);
    }
    TEST_UTIL.getHBaseCluster().getRegionServer(1).stop("die");
    //race condition here
    TEST_UTIL.getHBaseCluster().getMaster().stopMaster();
    LOG.info("Waiting for offline mode...");
    while(TEST_UTIL.getHBaseCluster().getMaster() == null ||
        !TEST_UTIL.getHBaseCluster().getMaster().isActiveMaster() ||
        TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServers().size() > 2 ||
        !TEST_UTIL.getHBaseCluster().getMaster().isInitialized()) {
      Thread.sleep(100);
    }

    //make sure balancer is in offline mode, since this is what we're testing
    assertFalse(((GroupBasedLoadBalancer)TEST_UTIL.getHBaseCluster().getMaster().getLoadBalancer()).isOnline());
    //verify the group affiliation that's loaded from ZK instead of tables
    assertEquals(newGroup, groupAdmin.getGroupInfoOfTable(GroupInfoManager.GROUP_TABLE_NAME).getName());
    assertEquals(GroupInfo.OFFLINE_DEFAULT_GROUP, groupAdmin.getGroupInfoOfTable(testTable).getName());

    //kill final regionserver to see the failover happens for all tables
    //except GROUP table since it's group does not have any online RS
    TEST_UTIL.getHBaseCluster().getRegionServer(0).stop("die");
    master = TEST_UTIL.getHBaseCluster().getMaster();
    LOG.info("Waiting for new table assignment...");
    while(TEST_UTIL.getHBaseCluster().getRegionServer(2).getOnlineRegions(Bytes.toBytes(testTable)).size() < 1) {
      Thread.sleep(100);
    }
    assertEquals(0, TEST_UTIL.getHBaseCluster().getRegionServer(2).getOnlineRegions(GroupInfoManager.GROUP_TABLE_NAME_BYTES).size());
  }

}
