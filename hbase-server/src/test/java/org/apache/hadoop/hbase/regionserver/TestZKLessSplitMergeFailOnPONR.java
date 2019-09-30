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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Category(LargeTests.class)
public class TestZKLessSplitMergeFailOnPONR {

  private static final Log LOG = LogFactory.getLog(TestZKLessSplitMergeFailOnPONR.class);

  private static final int NB_SERVERS = 3;
  private static final HBaseTestingUtility TESTING_UTIL = new HBaseTestingUtility();

  private HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;

  private void setupOnce() throws Exception {
    TESTING_UTIL.getConfiguration().setInt("hbase.balancer.period", 60000);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
  }

  @Before
  public void before() throws Exception {
    // Don't use ZK for region assignment
    TESTING_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", false);
    TESTING_UTIL.getConfiguration().set(
        RegionServerCoprocessorHost.REGION_COPROCESSOR_CONF_KEY, KillRS.class.getName());
    setupOnce();
    cluster = TESTING_UTIL.getMiniHBaseCluster();
    admin = TESTING_UTIL.getHBaseAdmin();
  }

  @After
  public void after() throws Exception {
    TESTING_UTIL.cleanupTestDir();
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testSplitFailOnPONR() throws Exception {
    TableName tableName = TableName.valueOf("testSplitFailOnPONR");
    try {
      KillRS.enabled.set(true);
      HTable table = createTableAndWait(tableName, new byte[]{'f'});
      HRegionInfo parent = table.getRegionLocations().firstEntry().getKey();
      LOG.debug("Parent region: " + parent);
      admin.split(parent.getRegionName(), new byte[]{5});
      final HMaster master = TESTING_UTIL.getHBaseCluster().getMaster();
      TESTING_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return master.getServerManager().areDeadServersInProgress();
        }
      });
      checkAndGetDaughters(tableName);
      TESTING_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return master.getAssignmentManager().getRegionStates()
              .getRegionsInTransition().size() == 0;
        }
      });
      assertTrue(master.getAssignmentManager().getRegionStates()
          .isRegionInState(parent, RegionState.State.SPLIT));

    } finally {
      KillRS.enabled.set(false);
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  private List<HRegion> checkAndGetDaughters(TableName tableName)
      throws InterruptedException {
    List<HRegion> daughters = null;
    // try up to 10s
    for (int i=0; i<100; i++) {
      daughters = cluster.getRegions(tableName);
      if (daughters.size() >= 2) break;
      Thread.sleep(100);
    }
    LOG.debug("Daughter regions: " + daughters);
    assertTrue(daughters.size() >= 2);
    return daughters;
  }

  private HTable createTableAndWait(TableName tableName, byte[] cf) throws IOException,
      InterruptedException {
    HTable t = TESTING_UTIL.createTable(tableName, cf);
    awaitTableRegions(tableName);
    assertTrue("Table not online: " + tableName,
        cluster.getRegions(tableName).size() != 0);
    return t;
  }

  private List<HRegion> awaitTableRegions(final TableName tableName) throws InterruptedException {
    List<HRegion> regions = null;
    for (int i = 0; i < 100; i++) {
      regions = cluster.getRegions(tableName);
      if (regions.size() > 0) break;
      Thread.sleep(100);
    }
    return regions;
  }

  public static class KillRS extends BaseRegionObserver {
    static AtomicInteger count = new AtomicInteger(0);
    static AtomicBoolean enabled = new AtomicBoolean(false);

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
      if(enabled.get()) {
        ctx.getEnvironment().getRegionServerServices().abort("Die - preSplitAfterPONR", null);
        throw new IOException("Die - preSplitAfterPONR");
      }
    }
  }
}