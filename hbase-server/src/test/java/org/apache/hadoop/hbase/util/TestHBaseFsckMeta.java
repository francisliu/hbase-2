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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertNoErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionStates;

import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;

import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;

import org.apache.hadoop.hbase.zookeeper.RootTableLocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * This tests HBaseFsck's ability to detect and fix issues with meta tables.
 */
@Category(LargeTests.class)
public class TestHBaseFsckMeta {
  final static Log LOG = LogFactory.getLog(TestHBaseFsckMeta.class);
  static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static Configuration conf = TEST_UTIL.getConfiguration();
  private static RegionStates regionStates;
  private final static int REGION_ONLINE_TIMEOUT = 800;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.handler.count", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.metahandler.count", 2);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 8 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(3);

    AssignmentManager assignmentManager =
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    regionStates = assignmentManager.getRegionStates();
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test(timeout=60000)
  public void testMetaOffline() throws Exception {
    // check no errors
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck);
    deleteMetaRegion(conf, true, false, false);
    hbck = doFsck(conf, false);
    // ERROR_CODE.UNKNOWN is coming because we reportError with a message for the hbase:meta
    // inconsistency and whether we will be fixing it or not.
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_META_REGION, ERROR_CODE.UNKNOWN });
    hbck = doFsck(conf, true);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_META_REGION, ERROR_CODE.UNKNOWN });
    hbck = doFsck(conf, false);
    assertNoErrors(hbck);
  }
  
  private void deleteMetaRegion(Configuration conf, boolean unassign, boolean hdfs,
      boolean regionInfoOnly) throws IOException, InterruptedException {
    HConnection connection = HConnectionManager.getConnection(conf);
    HRegionLocation metaLocation = connection.locateRegion(TableName.META_TABLE_NAME,
        HConstants.EMPTY_START_ROW);
    ServerName hsa = metaLocation.getServerName();
    HRegionInfo hri = metaLocation.getRegionInfo();
    if (unassign) {
      LOG.info("Undeploying meta region " + hri + " from server " + hsa);
      undeployRegion(new HBaseAdmin(conf), hsa, hri);
    }

    if (regionInfoOnly) {
      LOG.info("deleting hdfs .regioninfo data: " + hri.toString() + hsa.toString());
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir + "/" + HTableDescriptor.META_TABLEDESC.getNameAsString(),
          hri.getEncodedName());
      Path hriPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
      fs.delete(hriPath, true);
    }

    if (hdfs) {
      LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir + "/" + HTableDescriptor.META_TABLEDESC.getNameAsString(),
          hri.getEncodedName());
      HBaseFsck.debugLsr(conf, p);
      boolean success = fs.delete(p, true);
      LOG.info("Deleted " + p + " sucessfully? " + success);
      HBaseFsck.debugLsr(conf, p);
    }
  }
  
  /**
   * This method is used to undeploy a region -- close it and attempt to
   * remove its state from the Master.
   */
  private void undeployRegion(HBaseAdmin admin, ServerName sn,
      HRegionInfo hri) throws IOException, InterruptedException {
    try {
      HBaseFsckRepair.closeRegionSilentlyAndWait(admin.getConnection(), sn, hri);
      if (!(hri.isRootRegion() || hri.isMetaRegion())) {
        admin.offline(hri.getRegionName());
      }
    } catch (IOException ioe) {
      LOG.warn("Got exception when attempting to offline region "
          + Bytes.toString(hri.getRegionName()), ioe);
    }
  }
  
  @Test(timeout=180000)
  public void testFixAssignmentsWhenMETAinTransition() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
      admin.closeRegion(cluster.getServerHoldingMeta(),
          HRegionInfo.FIRST_META_REGIONINFO);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
    regionStates.regionOffline(HRegionInfo.FIRST_META_REGIONINFO);
    assertFalse(regionStates.isRegionOnline(HRegionInfo.FIRST_META_REGIONINFO));
    HBaseFsck hbck = doFsck(conf, true);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.UNKNOWN,
        ERROR_CODE.NO_META_REGION });
    assertNoErrors(doFsck(conf, false));
  }

  @Test(timeout=180000)
  public void testFixAssignmentsWhenRootinTransition() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    admin.closeRegion(cluster.getServerHoldingRoot(), HRegionInfo.ROOT_REGIONINFO);
    regionStates.regionOffline(HRegionInfo.ROOT_REGIONINFO);
    new RootTableLocator().deleteRootLocation(cluster.getMaster().getZooKeeper());
    assertFalse(regionStates.isRegionOnline(HRegionInfo.ROOT_REGIONINFO));
    HBaseFsck hbck = doFsck(conf, true);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.UNKNOWN,
        ERROR_CODE.NULL_ROOT_REGION, ERROR_CODE.NO_ROOT_REGION});
    assertNoErrors(doFsck(conf, false));
  }
}