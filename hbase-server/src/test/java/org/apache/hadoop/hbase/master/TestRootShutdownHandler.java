/**
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RootTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests handling of root-carrying region server failover.
 */
@Category(MediumTests.class)
public class TestRootShutdownHandler {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1, 3, null, null, MyRegionServer.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test will test the expire handling of a root-carrying
   * region server.
   * After HBaseMiniCluster is up, we will delete the ephemeral
   * node of the root-carrying region server, which will trigger
   * the expire of this region server on the master.
   * On the other hand, we will slow down the abort process on
   * the region server so that it is still up during the master SSH.
   * We will check that the master SSH is still successfully done.
   */
  @Test (timeout=180000)
  public void testExpireRootRegionServer() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    ServerName rootServerName = regionStates.getRegionServerOfRegion(
      HRegionInfo.ROOT_REGIONINFO);
    if (master.getServerName().equals(rootServerName) || rootServerName == null
        || !rootServerName.equals(cluster.getServerHoldingRoot())) {
      // Move root off master
      rootServerName = cluster.getLiveRegionServerThreads()
          .get(0).getRegionServer().getServerName();
      master.move(HRegionInfo.ROOT_REGIONINFO.getEncodedNameAsBytes(),
        Bytes.toBytes(rootServerName.getServerName()));
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    }
    RegionState rootState =
        RootTableLocator.getRootRegionState(master.getZooKeeper());
    assertEquals("Root should be not in transition",
      rootState.getState(), RegionState.State.OPEN);
    assertNotEquals("Root should be moved off master",
        rootServerName, master.getServerName());

    // Delete the ephemeral node of the root-carrying region server.
    // This is trigger the expire of this region server on the master.
    String rsEphemeralNodePath =
        ZKUtil.joinZNode(master.getZooKeeper().rsZNode, rootServerName.toString());
    ZKUtil.deleteNode(master.getZooKeeper(), rsEphemeralNodePath);
    // Wait for SSH to finish
    final ServerManager serverManager = master.getServerManager();
    final ServerName priorRootServerName = rootServerName;
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !serverManager.isServerOnline(priorRootServerName)
            && !serverManager.areDeadServersInProgress();
      }
    });

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    // Now, make sure root is assigned
    assertTrue("Root should be assigned",
      regionStates.isRegionOnline(HRegionInfo.ROOT_REGIONINFO));
    // Now, make sure root is registered in zk
    rootState = RootTableLocator.getRootRegionState(master.getZooKeeper());
    assertEquals("Root should be not in transition",
      rootState.getState(), RegionState.State.OPEN);
    assertEquals("Root should be assigned", rootState.getServerName(),
      regionStates.getRegionServerOfRegion(HRegionInfo.ROOT_REGIONINFO));
    assertNotEquals("Root should be assigned on a different server",
      rootState.getServerName(), rootServerName);
  }

  public static class MyRegionServer extends MiniHBaseClusterRegionServer {

    public MyRegionServer(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    @Override
    public void abort(String reason, Throwable cause) {
      // sleep to slow down the region server abort
      try {
        Thread.sleep(30*1000);
      } catch (InterruptedException e) {
        return;
      }
      super.abort(reason, cause);
    }
  }
}
