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
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return !cluster.getMaster().isInitialized() ||
          !((GroupBasedLoadBalancer)master.getLoadBalancer()).isOnline() ||
          cluster.getMaster().getServerManager().getOnlineServersList().size() < 3;
      }
    });
	}

	@AfterClass
	public static void tearDown() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

  @Test
  public void testOffline() throws Exception, InterruptedException {
    //table should be after group table name
    //so it gets assigned later
    final String failoverTable = "z"+GroupInfoManager.GROUP_TABLE_NAME;
    TEST_UTIL.createTable(Bytes.toBytes(failoverTable), Bytes.toBytes("f"));

    //adding testTable to special group so it gets assigned during offline mode
    GroupBasedLoadBalancer.SPECIAL_TABLES.add(failoverTable);

    GroupAdminClient groupAdmin = new GroupAdminClient(TEST_UTIL.getConfiguration());

    final HRegionServer killRS = cluster.getRegionServer(0);
    final HRegionServer groupRS = cluster.getRegionServer(1);
    final HRegionServer failoverRS = cluster.getRegionServer(2);

    String newGroup =  "my_group";
    groupAdmin.addGroup(newGroup);
    if(cluster.getMaster().getAssignmentManager().getAssignments().containsKey(failoverRS.getServerName())) {
      for(HRegionInfo  regionInfo:
        cluster.getMaster().getAssignmentManager().getAssignments().get(failoverRS.getServerName())) {
        cluster.getMaster().move(regionInfo.getEncodedNameAsBytes(),
            Bytes.toBytes(killRS.getServerName().getServerName()));
      }
      LOG.info("Waiting for region unassignments on failover RS...");
      waitForCondition(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          return cluster.getMaster().getAssignmentManager().getAssignments().get(failoverRS.getServerName()).size() > 0;
        }
      });
    }

    //move server to group and make sure all tables are assigned
    groupAdmin.moveServers(Sets.newHashSet(groupRS.getServerName().getHostAndPort()), newGroup);
    waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return groupRS.getOnlineRegions().size() > 0 ||
           master.getAssignmentManager().getRegionsInTransition().size() > 0;
      }
    });
    //move table to group and wait
    groupAdmin.moveTables(Sets.newHashSet(GroupInfoManager.GROUP_TABLE_NAME), newGroup);
    LOG.info("Waiting for move table...");
    waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return groupRS.getOnlineRegions().size() < 1;
      }
    });
    groupRS.stop("die");
    //race condition here
    TEST_UTIL.getHBaseCluster().getMaster().stopMaster();
    LOG.info("Waiting for offline mode...");
    waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return TEST_UTIL.getHBaseCluster().getMaster() == null ||
            !TEST_UTIL.getHBaseCluster().getMaster().isActiveMaster() ||
            !TEST_UTIL.getHBaseCluster().getMaster().isInitialized() ||
            TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServers().size() > 2;
      }
    });

    //make sure balancer is in offline mode, since this is what we're testing
    assertFalse(((GroupBasedLoadBalancer)TEST_UTIL.getHBaseCluster().getMaster().getLoadBalancer()).isOnline());
    //verify the group affiliation that's loaded from ZK instead of tables
    assertEquals(newGroup, groupAdmin.getGroupInfoOfTable(GroupInfoManager.GROUP_TABLE_NAME).getName());
    assertEquals(GroupInfo.OFFLINE_DEFAULT_GROUP, groupAdmin.getGroupInfoOfTable(failoverTable).getName());

    //kill final regionserver to see the failover happens for all tables
    //except GROUP table since it's group does not have any online RS
    killRS.stop("die");
    master = TEST_UTIL.getHBaseCluster().getMaster();
    LOG.info("Waiting for new table assignment...");
    waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return failoverRS.getOnlineRegions(Bytes.toBytes(failoverTable)).size() < 1;
      }
    });
    assertEquals(0, failoverRS.getOnlineRegions(GroupInfoManager.GROUP_TABLE_NAME_BYTES).size());
    //need this for minicluster to shutdown cleanly
    master.stopMaster();
  }

  private static void waitForCondition(PrivilegedExceptionAction<Boolean> action) throws Exception {
    long sleepInterval = 100;
    long timeout = 2*60000;
    long tries = timeout/sleepInterval;
    while(action.run()) {
      Thread.sleep(sleepInterval);
      if(tries-- < 0) {
        fail("Timeout");
      }
    }
  }
}
