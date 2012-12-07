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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.group.GroupAdminClient;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
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

@Category(MediumTests.class)
public class TestGroups {
	private static HBaseTestingUtility TEST_UTIL;
	private static HMaster master;
	private static Random rand;
  private static HBaseAdmin admin;
	private static String groupPrefix = "Group-";
	private static String tablePrefix = "TABLE-";
	private static String familyPrefix = "FAMILY-";

	@BeforeClass
	public static void setUp() throws Exception {
		TEST_UTIL = new HBaseTestingUtility();
		TEST_UTIL.getConfiguration().set(
				HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
				GroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.master.classes",
        GroupMasterObserver.class.getName()+","+
        GroupAdminEndpoint.class.getName());
		TEST_UTIL.startMiniCluster(4);
		MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
		master = cluster.getMaster();
		rand = new Random();
    admin = TEST_UTIL.getHBaseAdmin();
    //wait till the balancer is in online mode
    while(!((GroupBasedLoadBalancer)master.getLoadBalancer()).isOnline()) {
      Thread.sleep(100);
    }
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

  @After
  public void afterMethod() throws Exception {
		GroupAdminClient groupAdmin = new GroupAdminClient(master.getConfiguration());
    for(GroupInfo group: groupAdmin.listGroups()) {
      if(!group.getName().equals(GroupInfo.DEFAULT_GROUP)) {
        removeGroup(groupAdmin, group.getName());
      }
    }
    for(HTableDescriptor table: TEST_UTIL.getHBaseAdmin().listTables()) {
      if(!table.isMetaRegion() && !table.isRootRegion() &&
          !Bytes.equals(table.getName(), GroupInfoManager.GROUP_TABLE_NAME_BYTES)) {
        TEST_UTIL.deleteTable(table.getName());
      }
    }
  }

	@Test
	public void testBasicStartUp() throws IOException {
		GroupAdminClient groupAdmin = new GroupAdminClient(master.getConfiguration());
		GroupInfo defaultInfo = groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP);
		assertEquals(4, defaultInfo.getServers().size());
		// Assignment of root and meta regions.
		assertEquals(3, groupAdmin.listOnlineRegionsOfGroup(GroupInfo.DEFAULT_GROUP).size());
	}

	@Test
	public void testSimpleRegionServerMove() throws IOException,
			InterruptedException {
		GroupAdminClient groupAdmin = new GroupAdminClient(master.getConfiguration());
		GroupInfo appInfo = addGroup(groupAdmin, groupPrefix + rand.nextInt(), 1);
		GroupInfo adminInfo = addGroup(groupAdmin, groupPrefix + rand.nextInt(), 1);
    GroupInfo dInfo = groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP);
		assertEquals(3, groupAdmin.listGroups().size());
		assertTrue(adminInfo.getServers().size() == 1);
		assertTrue(appInfo.getServers().size() == 1);
		assertTrue(dInfo.getServers().size() == 2);
		groupAdmin.moveServers(appInfo.getServers(),
        GroupInfo.DEFAULT_GROUP);
		groupAdmin.removeGroup(appInfo.getName());
		groupAdmin.moveServers(adminInfo.getServers(),
        GroupInfo.DEFAULT_GROUP);
		groupAdmin.removeGroup(adminInfo.getName());
		assertTrue(groupAdmin.listGroups().size() == 1);
	}

	@Test
	public void testTableMove() throws IOException, InterruptedException {
		String tableName = tablePrefix + rand.nextInt();
		byte[] TABLENAME = Bytes.toBytes(tableName);
		byte[] FAMILYNAME = Bytes.toBytes(familyPrefix + rand.nextInt());
		GroupAdminClient groupAdmin = new GroupAdminClient(master.getConfiguration());
		GroupInfo newGroup = addGroup(groupAdmin, groupPrefix + rand.nextInt(), 2);
    int currMetaCount = TEST_UTIL.getMetaTableRows().size();
		HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht,
				FAMILYNAME, 4) == 4);
		TEST_UTIL.waitUntilAllRegionsAssigned(currMetaCount+4);
		assertTrue(master.getAssignmentManager().getZKTable()
				.isEnabledTable(Bytes.toString(TABLENAME)));
		List<HRegionInfo> regionList = TEST_UTIL.getHBaseAdmin()
				.getTableRegions(TABLENAME);
		assertTrue(regionList.size() > 0);
		GroupInfo tableGrp = groupAdmin.getGroupInfoOfTable(tableName);
		assertTrue(tableGrp.getName().equals(GroupInfo.DEFAULT_GROUP));

    //change table's group
    admin.disableTable(TABLENAME);
    groupAdmin.moveTables(Sets.newHashSet(Bytes.toString(TABLENAME)), newGroup.getName());
    admin.enableTable(TABLENAME);

    //verify group change
		assertEquals(newGroup.getName(),
        groupAdmin.getGroupInfoOfTable(Bytes.toString(TABLENAME)).getName());

		Map<String, Map<ServerName, List<HRegionInfo>>> tableRegionAssignMap = master
				.getAssignmentManager().getAssignmentsByTable();
		assertEquals(2, tableRegionAssignMap.keySet().size());
		Map<ServerName, List<HRegionInfo>> serverMap = tableRegionAssignMap
				.get(tableName);
		for (ServerName rs : serverMap.keySet()) {
			if (serverMap.get(rs).size() > 0) {
				assertTrue(newGroup.containsServer(rs.getHostAndPort()));
			}
		}
    removeGroup(groupAdmin, newGroup.getName());
		TEST_UTIL.deleteTable(TABLENAME);
		tableRegionAssignMap = master.getAssignmentManager()
				.getAssignmentsByTable();
		assertEquals(1, tableRegionAssignMap.size());
	}

	@Test
	public void testRegionMove() throws IOException, InterruptedException {
		GroupAdminClient groupAdmin = new GroupAdminClient(master.getConfiguration());
		GroupInfo newGroup = addGroup(groupAdmin, groupPrefix + rand.nextInt(), 1);
		String tableNameOne = tablePrefix + rand.nextInt();
		byte[] tableOneBytes = Bytes.toBytes(tableNameOne);
		byte[] familyOneBytes = Bytes.toBytes(familyPrefix + rand.nextInt());
		HTable ht = TEST_UTIL.createTable(tableOneBytes, familyOneBytes);

		// All the regions created below will be assigned to the default group.
    assertEquals(5, TEST_UTIL.createMultiRegions(master.getConfiguration(), ht, familyOneBytes, 5));
    while (master.getAssignmentManager().getRegionsOfTable(tableOneBytes).size() != 6) {
      Thread.sleep(100);
    }

    //get target region to move
    Map<ServerName,List<HRegionInfo>> assignMap =
        master.getAssignmentManager().getAssignmentsByTable().get(tableNameOne);
    HRegionInfo targetRegion = null;
    for(ServerName server : assignMap.keySet()) {
      targetRegion = assignMap.get(server).size() > 0 ? assignMap.get(server).get(0) : null;
      if(targetRegion != null) {
        break;
      }
    }
    //get server which is not a member of new group
    ServerName targetServer = null;
    for(ServerName server : master.getServerManager().getOnlineServersList()) {
      if(!newGroup.containsServer(server.getHostAndPort())) {
        targetServer = server;
        break;
      }
    }
    HRegionInterface targetRS =
        admin.getConnection().getHRegionConnection(targetServer.getHostname(), targetServer.getPort());

    //move target server to group
    groupAdmin.moveServers(Sets.newHashSet(targetServer.getHostAndPort()), newGroup.getName());
    while(targetRS.getOnlineRegions().size() > 0) {
      Thread.sleep(100);
    }

		// Lets move this region to the new group.
		master.move(targetRegion.getEncodedNameAsBytes(), Bytes.toBytes(targetServer.getServerName()));
    while (master.getAssignmentManager().getRegionsOfTable(tableOneBytes).size() != 6 ||
           master.getAssignmentManager().getRegionsInTransition().size() > 0) {
      Thread.sleep(100);
    }

    //verify that targetServer didn't open it
		assertFalse(targetRS.getOnlineRegions().contains(targetRegion));
	}

	static GroupInfo addGroup(GroupAdminClient gAdmin, String groupName,
			int serverCount) throws IOException, InterruptedException {
		GroupInfo defaultInfo = gAdmin
				.getGroupInfo(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo != null);
		assertTrue(defaultInfo.getServers().size() >= serverCount);
		gAdmin.addGroup(groupName);

    Set<String> set = new HashSet<String>();
    for(String server: defaultInfo.getServers()) {
      if(set.size() == serverCount) {
        break;
      }
      set.add(server);
    }
    gAdmin.moveServers(set, groupName);
    GroupInfo result = gAdmin.getGroupInfo(groupName);
		assertTrue(result.getServers().size() >= serverCount);
    return result;
	}

  static void removeGroup(GroupAdminClient groupAdmin, String groupName) throws IOException {
    for(String table: groupAdmin.listTablesOfGroup(groupName)) {
      byte[] bTable = Bytes.toBytes(table);
      admin.disableTable(bTable);
      groupAdmin.moveTables(Sets.newHashSet(table), GroupInfo.DEFAULT_GROUP);
      admin.enableTable(bTable);
    }
    groupAdmin.removeGroup(groupName);
  }

}
