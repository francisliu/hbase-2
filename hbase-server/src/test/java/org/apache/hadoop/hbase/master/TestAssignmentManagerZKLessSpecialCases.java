/**
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestAssignmentManagerZKLessSpecialCases {
  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();
  private final Log LOG = LogFactory.getLog(getClass());
  
  @Before
  public void before() throws Exception {
    HTU.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    HTU.startMiniCluster(1, 4, null, HMaster.class, MyTestRegionServer.class);
  }

  @After
  public void after() throws Exception {
    HTU.shutdownMiniCluster();
  }
  
  @Test
  public void testPendingOpenMetaCountsAsCarryingMeta() throws Exception {
    testPendingOpenSpecialRegionCountsAsCarryingRegion(HRegionInfo.FIRST_META_REGIONINFO);
  }
  
  @Test
  public void testPendingOpenRootCountsAsCarryingRoot() throws Exception {
    testPendingOpenSpecialRegionCountsAsCarryingRegion(HRegionInfo.ROOT_REGIONINFO);
  }
  
  public void testPendingOpenSpecialRegionCountsAsCarryingRegion(final HRegionInfo hri) throws Exception {
    final AssignmentManager am = HTU.getMiniHBaseCluster().getMaster().getAssignmentManager();
    assertEquals(State.OPEN, am.getRegionStates().getRegionState(
      hri).getState());
    HRegionLocation source = HTU.getHBaseAdmin().getConnection().getRegionLocation(
      hri.getTableName(), new byte[] { }, true);
    RegionServerThread destination = null;
    for (RegionServerThread hrst : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      if (!hrst.getRegionServer().getServerName().equals(source.getServerName())) {
        destination = hrst;
        break;
      }
    }
    ((MyTestRegionServer) destination.getRegionServer()).throwExceptionPostDeploy = true;
    
    // Stop crash processing so that we can do some consistent unit test assertions
    // without chance of race conditions.  Enable at the end and ensure regions get
    // assigned afterwards.
    HTU.getMiniHBaseCluster().getMaster().setServerCrashProcessingEnabled(false);
    
    HTU.getHBaseAdmin().move(hri.getEncodedNameAsBytes(), 
      Bytes.toBytes(destination.getRegionServer().getServerName().getServerName()));
    Waiter.waitFor(HTU.getConfiguration(), 15000, 1, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return am.getRegionStates().getRegionState(hri).isPendingOpen();
      }
    });
    LOG.info("Source Server: " + source.getServerName());
    LOG.info("Dest Server: " + destination.getRegionServer().getServerName());
    LOG.info(String.format("Region assignments: %s\n\n, Regions in transition: %s\n\n, "
        + "Source Server Holdings: %s\n\n, Destination Server Holdings: %s\n\n, "
        + "Root Region State: %s\n\n, Meta Region State: %s\n\n, "
        + "Root Region Transition State: %s\n\n, Meta Region Transition State: %s\n\n",
      am.getRegionStates().getRegionAssignments(), am.getRegionStates().getRegionsInTransition(), 
      am.getRegionStates().getServerRegions(source.getServerName()),
      am.getRegionStates().getServerRegions(destination.getRegionServer().getServerName()),
      am.getRegionStates().getRegionState(HRegionInfo.ROOT_REGIONINFO),
      am.getRegionStates().getRegionState(HRegionInfo.FIRST_META_REGIONINFO),
      am.getRegionStates().getRegionTransitionState(HRegionInfo.ROOT_REGIONINFO),
      am.getRegionStates().getRegionTransitionState(HRegionInfo.FIRST_META_REGIONINFO)));
    boolean isSourceCarrying = hri.isRootRegion() ? 
        am.isCarryingRoot(source.getServerName()).isHostingOrTransitionHosting():
        am.isCarryingMeta(source.getServerName()).isHostingOrTransitionHosting();
    boolean isDestinationCarrying = hri.isRootRegion() ? 
        am.isCarryingRoot(destination.getRegionServer().getServerName()).isHostingOrTransitionHosting():
        am.isCarryingMeta(destination.getRegionServer().getServerName()).isHostingOrTransitionHosting();
    assertEquals(State.PENDING_OPEN, am.getRegionStates().getRegionState(
      hri).getState());
    assertTrue(!isSourceCarrying);
    assertTrue(isDestinationCarrying);
    assertEquals(am.getRegionStates().getRegionState(hri).getServerName(), 
      destination.getRegionServer().getServerName());
    
    HTU.getMiniHBaseCluster().getMaster().setServerCrashProcessingEnabled(true);
    
    HTU.waitUntilAllRegionsAssigned(TableName.ROOT_TABLE_NAME);
    HTU.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);
    assertTrue(am.getRegionStates().getRegionState(HRegionInfo.ROOT_REGIONINFO).isOpened());
    assertTrue(am.getRegionStates().getRegionState(HRegionInfo.FIRST_META_REGIONINFO).isOpened());
  }
  
  public static class MyTestRegionServer extends MiniHBaseClusterRegionServer {
    private volatile boolean throwExceptionPostDeploy; 
    
    public MyTestRegionServer(Configuration conf, CoordinatedStateManager cp) throws IOException, InterruptedException {
      super(conf, cp);
    }

    @Override
    public void postOpenDeployTasks(final PostOpenDeployContext context) throws KeeperException,
        IOException {
      if (throwExceptionPostDeploy) {
        HTU.getMiniHBaseCluster().killRegionServer(getServerName());
      } else {
        super.postOpenDeployTasks(context);
      }
    }
  }
}
