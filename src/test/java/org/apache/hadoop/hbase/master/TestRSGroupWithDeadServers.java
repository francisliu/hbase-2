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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRSGroupWithDeadServers {
	private static HBaseTestingUtility TEST_UTIL;
	private static HMaster master;
	private static Random rand;
	@BeforeClass
	public static void setUp() throws Exception {
		TEST_UTIL = new HBaseTestingUtility();
		TEST_UTIL.getConfiguration().set(
				HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
				GroupBasedLoadBalancer.class.getName());
		TEST_UTIL.getConfiguration().setInt(
				"hbase.master.assignment.timeoutmonitor.period", 2000);
		TEST_UTIL.getConfiguration().setInt(
				"hbase.master.assignment.timeoutmonitor.timeout", 5000);
		TEST_UTIL.startMiniCluster(4);
		MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
		master = cluster.getMaster();
		rand = new Random();

	}

	@AfterClass
	public static void tearDown() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Test
	public void testGroupWithOnlineServers() throws IOException, InterruptedException{
		String newRSGroup = "group-" + rand.nextInt();
		GroupInfoManager gManager = new GroupInfoManager(master);
		String tableNameTwo = "TABLE-" + rand.nextInt();
		byte[] tableTwoBytes = Bytes.toBytes(tableNameTwo);
		String familyName = "family" + rand.nextInt();
		byte[] familyTwoBytes = Bytes.toBytes(familyName);
		int NUM_REGIONS = 4;

		GroupInfo defaultInfo = gManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo.getServers().size() == 4);
		TestGroupInfoManager.addGroup(gManager, newRSGroup, 2);
		gManager.refresh();
		defaultInfo = gManager.getGroupInformation(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo.getServers().size() == 2);
		assertTrue(gManager.getGroupInformation(newRSGroup).getServers().size() == 2);
		master.getAssignmentManager().refreshBalancer();
		HTable ht = TEST_UTIL.createTable(tableTwoBytes, familyTwoBytes);
		// All the regions created below will be assigned to the default group.
		assertTrue(TEST_UTIL.createMultiRegions(master.getConfiguration(), ht,
				familyTwoBytes, NUM_REGIONS) == NUM_REGIONS);
		TEST_UTIL.waitUntilAllRegionsAssigned(NUM_REGIONS);
		List<HRegionInfo> regions = gManager.getRegionsOfGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(regions.size() >= NUM_REGIONS);
		master.getAssignmentManager().refreshBalancer();
		gManager.moveTableToGroup(newRSGroup, tableNameTwo);
		TEST_UTIL.waitUntilAllRegionsAssigned(NUM_REGIONS);
		//Move the ROOT and META regions to default group.
		ServerName serverForRoot = ServerName.findServerWithSameHostnamePort(master.getServerManager()
				.getOnlineServersList(), defaultInfo.getServers()
				.get(0));
		master.move(HRegionInfo.ROOT_REGIONINFO.getEncodedNameAsBytes(), Bytes.toBytes(serverForRoot.toString()));
		master.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(), Bytes.toBytes(serverForRoot.toString()));
		while (master.getAssignmentManager().isRegionsInTransition()){
			Thread.sleep(10);
		}
		master.getAssignmentManager().refreshBalancer();
		List<HRegionInfo> newGrpRegions = gManager.getRegionsOfGroup(newRSGroup);
		assertTrue(newGrpRegions.size() == NUM_REGIONS);
		MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
		// Now we kill all the region servers in the new group.
		List<ServerName> serverNames = gManager.getGroupInformation(newRSGroup).getServers();
		for (ServerName sName : serverNames) {
			int serverNumber = getServerNumber(
					hbaseCluster.getRegionServerThreads(), sName);
			assert (serverNumber != -1);
			hbaseCluster.stopRegionServer(serverNumber, false);
		}
		//wait till all the regions come transition state.
		while (master.getAssignmentManager().getRegionsInTransition().size() < NUM_REGIONS){
			Thread.sleep(5);
		}
		gManager.refresh();
		newGrpRegions = gManager.getRegionsOfGroup(newRSGroup);
		assertTrue(newGrpRegions.size() == 0);
		regions = gManager.getRegionsOfGroup(GroupInfo.DEFAULT_GROUP);
		assertTrue(regions.size() == 2);
		scanTableForNegativeResults(ht);
		startServersAndMove(gManager,1,newRSGroup);
		master.getAssignmentManager().refreshBalancer();
		while(master.getAssignmentManager().isRegionsInTransition()){
			Thread.sleep(5);
		}
		scanTableForPositiveResults(ht);
		newGrpRegions = gManager.getRegionsOfGroup(newRSGroup);
		assertTrue(newGrpRegions.size() == NUM_REGIONS);
		TEST_UTIL.deleteTable(tableTwoBytes);
		TEST_UTIL.getDFSCluster().getFileSystem().delete(new Path(FSUtils.getRootDir(master
				.getConfiguration()), GroupInfoManager.GROUP_INFO_FILE_NAME), true);

	}

	private int getServerNumber(List<JVMClusterUtil.RegionServerThread> servers, ServerName sName){
		int i = 0;
		for(JVMClusterUtil.RegionServerThread rs : servers){
			if(ServerName.isSameHostnameAndPort(rs.getRegionServer().getServerName(), sName)){
				return i;
			}
			i++;
		}
		return -1;
	}

	private void scanTableForNegativeResults(HTable ht){
		ResultScanner s = null;
		boolean isExceptionCaught = false;
		try {
			Scan scan = new Scan();
			s = ht.getScanner(scan);
		} catch (Exception exp) {
			assertTrue(exp instanceof RetriesExhaustedException);
			isExceptionCaught = true;
		} finally {
			if (s != null) {
				s.close();
			}
			assertTrue(isExceptionCaught);
		}
	}

	private void scanTableForPositiveResults(HTable ht) throws IOException{
		ResultScanner s = null;
		try {
			Scan scan = new Scan();
			s = ht.getScanner(scan);
		} finally {
			if (s != null) {
				s.close();
			}
		}
	}

	private void startServersAndMove(GroupInfoManager gManager, int numServers,
			String groupName) throws IOException, InterruptedException {
		MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
		ServerName newServer;
		for (int i = 0; i < numServers; i++) {
			newServer = hbaseCluster.startRegionServer().getRegionServer()
					.getServerName();
			// Make sure that the server manager reports the new online servers.
			while (ServerName.findServerWithSameHostnamePort(master
					.getServerManager().getOnlineServersList(), newServer) == null) {
				Thread.sleep(5);
			}
			assertTrue(gManager.getGroupInformation(GroupInfo.DEFAULT_GROUP)
					.contains(newServer));
			gManager.moveServer(newServer, GroupInfo.DEFAULT_GROUP, groupName);
			assertTrue(gManager.getGroupInformation(groupName).contains(
					newServer));
		}
	}

}
