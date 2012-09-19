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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestGroupInfoManager {
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
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Test
	public void testBasicStartUp() throws IOException {
		GroupAdminClient groupManager = new GroupAdminClient(master.getConfiguration());
		GroupInfo defaultInfo = groupManager.getGroup(GroupInfo.DEFAULT_GROUP);
		defaultInfo = groupManager.getGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo.getServers().size() == 4);
		// Assignment of root and meta regions.
		assertTrue(groupManager.listRegionsOfGroup(GroupInfo.DEFAULT_GROUP)
        .size() == 2);
	}

	@Test
	public void testSimpleRegionServerMove() throws IOException,
			InterruptedException {
		GroupAdminClient groupManager = new GroupAdminClient(master.getConfiguration());
		String groupOne = groupPrefix + rand.nextInt();
		addGroup(groupManager, groupOne, 1);
		GroupInfo dInfo = groupManager
				.getGroup(GroupInfo.DEFAULT_GROUP);
		String groupTwo = groupPrefix + rand.nextInt();
		addGroup(groupManager, groupTwo, 1);
		// Force the group info manager to read group information from disk.
		assertTrue(groupManager.listGroups().size() == 3);
		dInfo = groupManager.getGroup(GroupInfo.DEFAULT_GROUP);
		GroupInfo appInfo = groupManager.getGroup(groupTwo);
		GroupInfo adminInfo = groupManager.getGroup(groupOne);
		assertTrue(adminInfo.getServers().size() == 1);
		assertTrue(appInfo.getServers().size() == 1);
		assertTrue(dInfo.getServers().size() == 2);
		groupManager.moveServers(appInfo.getServers(),
        GroupInfo.DEFAULT_GROUP);
    waitForTransitions(groupManager);
		groupManager.removeGroup(groupTwo);
		groupManager.moveServers(adminInfo.getServers(),
        GroupInfo.DEFAULT_GROUP);
    waitForTransitions(groupManager);
		groupManager.removeGroup(groupOne);
		assertTrue(groupManager.listGroups().size() == 1);
	}

	@Test
	public void testTableMove() throws IOException, InterruptedException {
		String tableName = tablePrefix + rand.nextInt();
		byte[] TABLENAME = Bytes.toBytes(tableName);
		byte[] FAMILYNAME = Bytes.toBytes(familyPrefix + rand.nextInt());
		GroupAdminClient groupManager = new GroupAdminClient(master.getConfiguration());
		String newGroupName = groupPrefix + rand.nextInt();
		addGroup(groupManager, newGroupName, 2);

		HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht,
				FAMILYNAME, 4) == 4);
		TEST_UTIL.waitUntilAllRegionsAssigned(4);
		assertTrue(master.getAssignmentManager().getZKTable()
				.isEnabledTable(Bytes.toString(TABLENAME)));
		List<HRegionInfo> regionList = TEST_UTIL.getHBaseAdmin()
				.getTableRegions(TABLENAME);
		assertTrue(regionList.size() > 0);
		GroupInfo tableGrp = groupManager.getGroupInfoOfTable(Bytes.toBytes(tableName));
		assertTrue(tableGrp.getName().equals(GroupInfo.DEFAULT_GROUP));


    admin.disableTable(TABLENAME);
    HTableDescriptor desc = admin.getTableDescriptor(TABLENAME);
    GroupInfo.setGroupString(newGroupName, desc);
    admin.modifyTable(TABLENAME,desc);
    admin.enableTable(TABLENAME);
    desc = admin.getTableDescriptor(TABLENAME);
		GroupInfo newTableGrp = groupManager.getGroup(GroupInfo.getGroupString(desc));
		assertTrue(newTableGrp.getName().equals(newGroupName));
		Map<String, Map<ServerName, List<HRegionInfo>>> tableRegionAssignMap = master
				.getAssignmentManager().getAssignmentsByTable();
		assertTrue(tableRegionAssignMap.keySet().size() == 1);
		Map<ServerName, List<HRegionInfo>> serverMap = tableRegionAssignMap
				.get(tableName);
		for (ServerName rs : serverMap.keySet()) {
			if (serverMap.get(rs).size() > 0) {
				assertTrue(newTableGrp.containsServer(rs.getHostAndPort()));
			}
		}
    groupManager.removeGroup(newGroupName);
		TEST_UTIL.deleteTable(TABLENAME);
		tableRegionAssignMap = master.getAssignmentManager()
				.getAssignmentsByTable();
		assertTrue(tableRegionAssignMap.size() == 0);
	}

	@Test
	public void testRegionMove() throws IOException, InterruptedException {
		GroupAdminClient groupManager = new GroupAdminClient(master.getConfiguration());
		String newGroupName = groupPrefix + rand.nextInt();
		addGroup(groupManager, newGroupName, 1);
		String tableNameOne = tablePrefix + rand.nextInt();
		byte[] tableOneBytes = Bytes.toBytes(tableNameOne);
		byte[] familyOneBytes = Bytes.toBytes(familyPrefix + rand.nextInt());
		HTable ht = TEST_UTIL.createTable(tableOneBytes, familyOneBytes);
		// All the regions created below will be assigned to the default group.
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht,
				familyOneBytes, 5) == 5);
		TEST_UTIL.waitUntilAllRegionsAssigned(5);
		List<HRegionInfo> regions = groupManager
				.listRegionsOfGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(regions.size()+">="+5,regions.size() >= 5);
		HRegionInfo region = regions.get(regions.size()-1);
		// Lets move this region to newGroupName group.
		ServerName tobeAssigned = ServerName.parseServerName(groupManager
				.getGroup(newGroupName).getServers().iterator().next());
		master.move(region.getEncodedNameAsBytes(),
        Bytes.toBytes(tobeAssigned.toString()));

		while (master.getAssignmentManager().getRegionsInTransition().size() > 0) {
			Thread.sleep(10);
		}

		List<HRegionInfo> updatedRegions = groupManager
				.listRegionsOfGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(regions.size() == updatedRegions.size());
    HRegionInterface rs = admin.getConnection().getHRegionConnection(tobeAssigned.getHostname(),tobeAssigned.getPort());
		assertFalse(rs.getOnlineRegions().contains(region));
		TEST_UTIL.deleteTable(tableOneBytes);
    groupManager.removeGroup(newGroupName);
	}

	static void addGroup(GroupAdminClient gManager, String groupName,
			int servers) throws IOException, InterruptedException {
		GroupInfo defaultInfo = gManager
				.getGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo != null);
		assertTrue(defaultInfo.getServers().size() >= servers);
		gManager.addGroup(new GroupInfo(groupName, new TreeSet<String>()));
		Iterator<String> itr = defaultInfo.getServers().iterator();
    Set<String> set = new TreeSet<String>();
		for (int i = 0; i < servers; i++) {
      set.add(itr.next());
		}
    gManager.moveServers(set, groupName);
    waitForTransitions(gManager);
		assertTrue(gManager.getGroup(groupName).getServers().size() >= servers);
	}

  private static void waitForTransitions(GroupAdmin gAdmin) throws IOException, InterruptedException {
    while(gAdmin.listServersInTransition().size()>0) {
      Thread.sleep(1000);
    }
  }

}
