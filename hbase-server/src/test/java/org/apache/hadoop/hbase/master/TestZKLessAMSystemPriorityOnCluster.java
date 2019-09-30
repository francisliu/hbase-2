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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * Test to ensure that system tables gets assigned first. Currently only NS table
 * needs to be assigned prior to the namespace manager running. But let's address it
 * for the general case as the requirement/effort is the same.
 *
 * Currently it only has test for a specific case of failover mode.
 * More tests will be added as other aspects of system table priority assignment is implemented:
 * 1. Clean cluster startup mode - to be implemented,
 * workaround set hbase.bulk.assignment.waittillallassigned= true
 * 2. Failover mode
 * 2.a system regions are on expired servers (need replay) - to be implemented, startup will
 * take longer as assignment will block till namespace region is assigned. other system regions
 * don't really need to be assigned ahead of time.
 * 2.b system regions are offline and on expired servers - implemented, test seen below
 */
@Category(MediumTests.class)
public class TestZKLessAMSystemPriorityOnCluster {
  private static final Log LOG = LogFactory.getLog(TestZKLessAMSystemPriorityOnCluster.class);
  final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();
  static HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Don't use ZK for region assignment
    conf.setBoolean("hbase.assignment.usezk", false);
    conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);

    TEST_UTIL.startMiniCluster(1, 4);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // This tests the special case that the namespace table has been marked offline
  // And it's last assignment was on a server that does not fall into
  // the dead server category (no regions open, no wal, etc).
  // This verifies that the namespace table (any system table) still gets assigned
  // for this particular case.
  //TODO remove or fix this test
  @Ignore("Test no longer relevant") @Test(timeout=60000)
  public void testFailoverModeNamespaceOfflineRegion() throws Exception {
    HTable nsTable =
        new HTable(TEST_UTIL.getConfiguration(), TableName.NAMESPACE_TABLE_NAME);
    ServerName nsServer = nsTable.getRegionLocations().firstEntry().getValue();
    admin.offline(nsTable.getRegionLocations().firstEntry().getKey().getRegionName());
    LOG.info("Stopping RS server holding NS table");
    TEST_UTIL.getHBaseCluster().stopRegionServer(nsServer);

    LOG.info("Killing Master server");
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    TEST_UTIL.getHBaseCluster().killMaster(master.getServerName());
    TEST_UTIL.getHBaseCluster().waitForMasterToStop(master.getServerName(), 30000);

    LOG.info("killing an RS to force failover mode");
    for (RegionServerThread rs :
        TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {
      if (!nsServer.equals(rs.getRegionServer().getServerName()) &&
          rs.getRegionServer().getOnlineRegionsLocalContext().size() > 0) {
        TEST_UTIL.getHBaseCluster().killRegionServer(rs.getRegionServer().getServerName());
        break;
      }
    }

    LOG.info("Restarting master");
    TEST_UTIL.getHBaseCluster().startMaster();
    TEST_UTIL.getHBaseCluster().startRegionServer();
    TEST_UTIL.getHBaseCluster().startRegionServer();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(30000);

    ResultScanner scanner = nsTable.getScanner(new Scan());
    int count = 0;
    for (Result res : scanner) {
      count++;
    }
    assertEquals(2, count);
  }
}
