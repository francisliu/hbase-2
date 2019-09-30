/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.CatalogAccessor;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.hadoop.hbase.util.Threads;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSplitMetaAssignmentOperations {
  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  final static Configuration conf = HBaseConfiguration.create();
  private static final Log LOG = LogFactory.getLog(TestSplitMetaAssignmentOperations.class);
  private final int NUM_MASTERS = 1;
  private final int NUM_RS = 3;

  private HRegionServer createTableAndSplit(String tableNameStr, MiniHBaseCluster hbaseCluster,
      byte[] splitPoint) throws Exception {
    final TableName tableName = TableName.valueOf(tableNameStr);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("cf").setBlocksize(30));
    SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
    byte[][] splits = algo.split(10);
    new HBaseAdmin(conf).createTable(desc, splits);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);

    LOG.info("Splitting meta");

    int metaServerIndex =
        hbaseCluster.getServerWith(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    HRegionServer server = hbaseCluster.getRegionServer(metaServerIndex);
    split(HRegionInfo.FIRST_META_REGIONINFO, splitPoint, TEST_UTIL);

    LOG.info("Splitting done");

    return server;
  }

  @Test(timeout = 120000)
  public void testRestartMasterSplitMeta() throws Exception {
    String table = "testRestartMasterSplitMeta";
    conf.setBoolean("hbase.assignment.usezk", false);
    conf.setBoolean("hbase.split.meta", true);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    try {
      TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();
      createTableAndSplit(table, cluster, null);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      assertEquals(State.SPLIT, regionStates.getRegionState(HRegionInfo.FIRST_META_REGIONINFO)
          .getState());

      List<Pair<HRegionInfo, ServerName>> regionInfoList =
          CatalogAccessor.getTableRegionsAndLocations(
              TEST_UTIL.getZooKeeperWatcher(),
              TEST_UTIL.getConnection(),
              TableName.META_TABLE_NAME);
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
      for (Pair<HRegionInfo, ServerName> p : regionInfoList) {
        regions.add(p.getFirst());
      }
      // Meta is now split into two
      assertEquals(2, regions.size());
      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));
      LOG.info("Stopping master");
      TEST_UTIL.getMiniHBaseCluster().stopMaster(0);
      LOG.info("Starting master back up");
      TEST_UTIL.getMiniHBaseCluster().startMaster();
      // Wait till master is active and is initialized
      while (TEST_UTIL.getMiniHBaseCluster().getMaster() == null
          || !TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
        Threads.sleep(1);
      }
      master = TEST_UTIL.getHBaseCluster().getMaster();
      regionStates = master.getAssignmentManager().getRegionStates();
      // Both meta's should remain online
      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 180000)
  public void testRestartMasterSplitMetaDifferentStates() throws Exception {
    // Start the cluster
    conf.setBoolean("hbase.assignment.usezk", false);
    conf.setBoolean("hbase.split.meta", true);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    String table = "testRestartMasterSplitMetaDifferentStates";
    RegionServerThread rst = null;
    try {
      TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();
      createTableAndSplit(table, cluster, null);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      assertEquals(State.SPLIT, regionStates.getRegionState(HRegionInfo.FIRST_META_REGIONINFO)
          .getState());

      List<Pair<HRegionInfo, ServerName>> regionInfoList =
          CatalogAccessor.getTableRegionsAndLocations(
              TEST_UTIL.getZooKeeperWatcher(),
              TEST_UTIL.getConnection(),
              TableName.META_TABLE_NAME);
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
      for (Pair<HRegionInfo, ServerName> p : regionInfoList) {
        regions.add(p.getFirst());
      }

      assertEquals(2, regions.size());
      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));

      LOG.info("Stopping master");
      TEST_UTIL.getMiniHBaseCluster().stopMaster(0);

      // Simulate a case where the split meta is on a bad server
      LOG.info("Killing " + regionInfoList.get(0).getSecond());
      TEST_UTIL.getHBaseCluster().killRegionServer(regionInfoList.get(0).getSecond());

      LOG.info("Starting master back up");
      TEST_UTIL.getMiniHBaseCluster().startMaster();
      rst = TEST_UTIL.getMiniHBaseCluster().startRegionServer();
      // Wait till master is active and is initialized
      while (TEST_UTIL.getMiniHBaseCluster().getMaster() == null
          || !TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
        Threads.sleep(100);
      }
      master = TEST_UTIL.getHBaseCluster().getMaster();
      regionStates = master.getAssignmentManager().getRegionStates();
      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));

      ServerName serverName = regionStates.getRegionState(regions.get(0)).getServerName();
      // Simulate a case where split meta is PENDING_OPEN on a wrong server
      HTable rootTable = new HTable(TableName.ROOT_TABLE_NAME, master.getConnection());
      rootTable.put(getPutForMeta(regions.get(0), serverName, State.PENDING_OPEN));
      rootTable.close();

      LOG.info("Stopping master");
      TEST_UTIL.getMiniHBaseCluster().stopMaster(0);
      LOG.info("Starting master back up");
      TEST_UTIL.getMiniHBaseCluster().startMaster();
      // Wait till master is active and is initialized
      while (TEST_UTIL.getMiniHBaseCluster().getMaster() == null
          || !TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
        Threads.sleep(1);
      }
      master = TEST_UTIL.getHBaseCluster().getMaster();
      regionStates = master.getAssignmentManager().getRegionStates();
      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));
    } finally {
      if (rst != null) {
        rst.getRegionServer().stop("shutdown");
      }
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 180000)
  public void testMetaRootShutdown() throws Exception {
    String table = "testRestartMasterSplitMetaDifferentStates";
    conf.setBoolean("hbase.assignment.usezk", false);
    conf.setBoolean("hbase.split.meta", true);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    try {
      TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();

      // Later a new CatalogJanitor is created, so disable this
      cluster.getMaster().setCatalogJanitorEnabled(false);

      HRegionServer metaServer = createTableAndSplit(table, cluster, null);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      assertEquals(State.SPLIT, regionStates.getRegionState(HRegionInfo.FIRST_META_REGIONINFO)
          .getState());

      List<Pair<HRegionInfo, ServerName>> regionInfoList =
          CatalogAccessor.getTableRegionsAndLocations(
              TEST_UTIL.getZooKeeperWatcher(),
              TEST_UTIL.getConnection(),
              TableName.META_TABLE_NAME,
              true);
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
      for (Pair<HRegionInfo, ServerName> p : regionInfoList) {
        regions.add(p.getFirst());
      }
      TEST_UTIL.compact(TableName.META_TABLE_NAME, true);
      //cleanup references so janitor can cleanup meta
      compactAndArchive(TEST_UTIL, TableName.META_TABLE_NAME);

      CatalogJanitor catalogJanitor = new CatalogJanitor(master, master);
      // Split parent should be cleaned
      assertEquals(1, catalogJanitor.scan());

      int regionCount = ProtobufUtil.getOnlineRegions(metaServer.getRSRpcServices()).size();
      split(regions.get(0), "50000000,,1".getBytes(), TEST_UTIL);

      LOG.info("Splitting done of " + regions.get(0));

      assertEquals(State.SPLIT, regionStates.getRegionState(regions.get(0)).getState());

      regionInfoList =
          CatalogAccessor.getTableRegionsAndLocations(
              TEST_UTIL.getZooKeeperWatcher(),
              TEST_UTIL.getConnection(),
              TableName.META_TABLE_NAME,
              true);
      regions = new ArrayList<HRegionInfo>();
      for (Pair<HRegionInfo, ServerName> p : regionInfoList) {
        regions.add(p.getFirst());
      }
      assertEquals(3, regions.size());

      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));
      assertTrue(regionStates.isRegionOnline(regions.get(2)));

      // Put two meta's on one server and then 1 meta and root on another
      moveRegionToServer(regions.get(0), 0, TEST_UTIL);
      moveRegionToServer(regions.get(1), 0, TEST_UTIL);
      moveRegionToServer(HRegionInfo.ROOT_REGIONINFO, 1, TEST_UTIL);
      moveRegionToServer(regions.get(2), 1, TEST_UTIL);

      CatalogAccessor.fullScanOfMeta(TEST_UTIL.getConnection());

      LOG.info("Stop the server hosting meta's and root");

      cluster.getRegionServer(0).stop(
        "Stopping meta server" + cluster.getRegionServer(0).getServerName());
      cluster.getRegionServer(1).stop(
        "Stopping meta and root server" + cluster.getRegionServer(1).getServerName());
      // TEST_UTIL.getMiniHBaseCluster().waitOnRegionServer(0);
      // TEST_UTIL.getMiniHBaseCluster().waitOnRegionServer(1);
      waitForRSShutdownToStartAndFinish(cluster.getRegionServer(0).getServerName(), cluster);
      waitForRSShutdownToStartAndFinish(cluster.getRegionServer(1).getServerName(), cluster);
      TEST_UTIL.getMiniHBaseCluster().getMaster().assignmentManager
          .waitUntilNoRegionsInTransition(60000);

      LOG.info("Verifying meta is accessible.");
      CatalogAccessor.fullScanOfMeta(TEST_UTIL.getConnection());

      assertTrue(regionStates.isRegionOnline(regions.get(0)));
      assertTrue(regionStates.isRegionOnline(regions.get(1)));
      assertTrue(regionStates.isRegionOnline(regions.get(2)));
      assertTrue(regionStates.isRegionOnline(HRegionInfo.ROOT_REGIONINFO));
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private void waitForRSShutdownToStartAndFinish(ServerName serverName, MiniHBaseCluster cluster)
      throws InterruptedException {
    ServerManager sm = cluster.getMaster().getServerManager();
    while (sm.isServerOnline(serverName)) {
      LOG.debug("Server [" + serverName + "] is still online, waiting");
      Thread.sleep(1000);
    }
    while (sm.areDeadServersInProgress()) {
      LOG.debug("Server [" + serverName + "] still being processed, waiting");
      Thread.sleep(1000);
    }
    LOG.debug("Server [" + serverName + "] done with server shutdown processing");
  }

  @Test(timeout = 120000)
  public void cannotSplitMeta() throws Exception {
    conf.setBoolean("hbase.split.meta", false);
    conf.setBoolean("hbase.assignment.usezk", false);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    try {
      TEST_UTIL.startMiniCluster(NUM_MASTERS, 1);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();

      String tableName = "checkSplitMeta";
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor("cf").setBlocksize(30));
      SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
      byte[][] splits = algo.split(10);
      HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
      hbaseAdmin.createTable(desc, splits);

      int metaServerIndex =
          cluster.getServerWith(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      HRegionServer server = cluster.getRegionServer(metaServerIndex);
      int regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      hbaseAdmin.split(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      for (int i = 0; i < 300; i++) {
        LOG.debug("Waiting on region to split");
        Thread.sleep(100);
        assertEquals(1,
            CatalogAccessor.getTableRegions(TEST_UTIL.getZooKeeperWatcher(),
                TEST_UTIL.getConnection(),
                TableName.META_TABLE_NAME,
                true).size());
      }
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 120000)
  public void canSplitMeta() throws Exception {
    conf.setBoolean("hbase.split.meta", true);
    conf.setBoolean("hbase.assignment.usezk", false);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    try {
      TEST_UTIL.startMiniCluster(NUM_MASTERS, 1);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();

      String tableName = "canSplitMeta";
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor("cf").setBlocksize(30));
      SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
      byte[][] splits = algo.split(10);
      HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
      hbaseAdmin.createTable(desc, splits);

      int metaServerIndex =
          cluster.getServerWith(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      HRegionServer server = cluster.getRegionServer(metaServerIndex);
      int regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      hbaseAdmin.split(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      for (int i = 0;
           ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size()
               <= regionCount && i < 300;
           i++) {
        LOG.debug("Waiting on region to split");
        Thread.sleep(100);
      }
      // meta is split in two, so total regionCount is increased by 1
      assertEquals(ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size(),
          regionCount + 1);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private Put getPutForMeta(HRegionInfo hregion, ServerName serverName, State state) {
    Put put = new Put(hregion.getRegionName());
    put.addImmutable(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      Bytes.toBytes(serverName.getHostAndPort()));
    put.addImmutable(HConstants.CATALOG_FAMILY, HConstants.SERVERNAME_QUALIFIER,
      Bytes.toBytes(serverName.toString()));
    put.addImmutable(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
      Bytes.toBytes(serverName.getStartcode()));
    put.addImmutable(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER,
      Bytes.toBytes(state.name()));
    return put;
  }

  private RegionServerThread moveRegionToServer(HRegionInfo regionInfo, int index,
      HBaseTestingUtility testUtil) throws Exception {
    HMaster master = testUtil.getHBaseCluster().getMaster();
    RegionServerThread hrs = testUtil.getHBaseCluster().getLiveRegionServerThreads().get(index);
    ServerName serverName = hrs.getRegionServer().getServerName();
    master.move(regionInfo.getEncodedNameAsBytes(), Bytes.toBytes(serverName.getServerName()));
    testUtil.assertRegionOnServer(regionInfo, serverName, 6000);
    return hrs;
  }

  private void split(final HRegionInfo hri,  byte[] splitPoint, HBaseTestingUtility testUtil)
      throws IOException, InterruptedException {
    new HBaseAdmin(conf).split(hri.getRegionName(), splitPoint);
    waitForSplit(hri, testUtil);
  }

  private void waitForSplit(HRegionInfo hri, HBaseTestingUtility testUtil) throws
      InterruptedException, IOException {
    KVComparator comparator = hri.getComparator();
    int count = 0;
    for (int i = 0; count < 2 && i < 300; i++) {
      count = 0;
      for (HRegionLocation loc :
          testUtil.getConnection().getRegionLocator(hri.getTable()).getAllRegionLocations()) {
        HRegionInfo regionInfo = loc.getRegionInfo();
        if (regionInfo.getRegionNameAsString().equals(hri.getRegionNameAsString())) {
          continue;
        }
        if (comparator.compareRows(hri.getStartKey(), 0, hri.getStartKey().length,
            regionInfo.getStartKey(), 0, regionInfo.getStartKey().length) == 0
            && loc.getServerName() != null) {
          count++;
          LOG.debug("Found top split: "+loc);
        }
        if (comparator.compareRows(hri.getEndKey(), 0, hri.getEndKey().length,
            regionInfo.getEndKey(), 0, regionInfo.getEndKey().length) == 0
            && loc.getServerName() != null) {
          count++;
          LOG.debug("Found bottom split: "+loc);
        }

      }
      LOG.debug("Waiting on region to split");
      Thread.sleep(100);
    }
    for (Result r :
        testUtil.getConnection().getTable(hri.getTable()).getScanner(new Scan())) {
      //do nothing we just want to make sure meta regions are accessible
    }
    assertFalse("Waited too long for region to split", count < 2);
  }

  void compactAndArchive(HBaseTestingUtility testUtil, TableName tableName) throws Exception {
    for (final RegionServerThread rst:
        testUtil.getMiniHBaseCluster().getLiveRegionServerThreads()) {
      testUtil.waitFor(60000, new Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return rst.getRegionServer().getCompactSplitThread().getCompactionQueueSize() == 0;
        }
      });
      for(final Region r: rst.getRegionServer().getOnlineRegionsLocalContext()) {
        if(r.getTableDesc().getTableName().equals(tableName)) {
          testUtil.waitFor(60000, new Predicate<Exception>() {
            @Override
            public boolean evaluate() throws Exception {
              return r.getCompactionState() == CompactionState.NONE;
            }
          });
          r.compact(true);
          for(Store store: r.getStores()) {
            store.closeAndArchiveCompactedFiles();
          }
        }
      }
    }
  }

}
