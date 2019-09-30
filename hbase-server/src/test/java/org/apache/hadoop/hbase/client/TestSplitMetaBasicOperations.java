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

package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import com.google.common.collect.Iterators;
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
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.master.TestSplitMetaAssignmentOperations;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSplitMetaBasicOperations {

  static Configuration conf = HBaseConfiguration.create();
  private static final Log LOG = LogFactory.getLog(TestSplitMetaAssignmentOperations.class);

  /**
   * Tests basic split and merge operations on meta and user tables
   * as well as tests that the tables are still accessible using basic operations.
   * Then performs some tests which verify that the proper comparator is used
   * when meta row keys are involved as the meta row key is broken up into
   * 3 parts and lexicographical ordering is performed on each one individually.
   * Given that the ',' is the delimiter for these 3 parts we have chosen split keys
   * which have table splits keys that are lexicographically smaller than ','
   * in certain scenarios to verify that the proper comparator is used.
   */
  @Test(timeout = 120000)
  public void testBasicSplitMerge() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitAtSplitPoint");
    conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", false);
    conf.setBoolean("hbase.split.meta", true);
    conf.set("hbase.client.retries.by.server", "10");
    conf.set("hbase.client.retries.number", "10");
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    try {
      TEST_UTIL.startMiniCluster(1, 3);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();

      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor("cf").setBlocksize(30));
      byte[][] splits = {{0x02},{0x03},{0x04},{0x05}};
      final HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
      hbaseAdmin.createTable(desc, splits);

      List<Result> tableRegionList = CatalogAccessor.fullScan(TEST_UTIL.getConnection());
      LOG.info("Splitting meta");
      NavigableMap<HRegionInfo, ServerName> tableRegionLocations =
          new HTable(conf, tableName).getRegionLocations();

      //Test malformed split key
      try {
        splitTable(TEST_UTIL, TableName.META_TABLE_NAME, Bytes.toBytesBinary("5555"));
        fail("Expected malformed split key related exception");
      } catch (Exception ex) {
      }

      //Test malformed split key, empty table name
      try {
        splitTable(TEST_UTIL, TableName.META_TABLE_NAME, Bytes.toBytesBinary(",,123"));
        fail("Expected malformed split key related exception");
      } catch (Exception ex) {
      }

      //Test malformed split key, empty id component
      try {
        splitTable(TEST_UTIL, TableName.META_TABLE_NAME, Bytes.toBytesBinary("123,,"));
        fail("Expected malformed split key related exception");
      } catch (Exception ex) {
      }

      splitTable(TEST_UTIL, TableName.META_TABLE_NAME,
          Iterators.get(tableRegionLocations.keySet().iterator(), 0).getRegionName());
      // Root should have two entries as meta got split in 2
      List<HRegionInfo> regions =
          CatalogAccessor.getTableRegions(TEST_UTIL.getZooKeeperWatcher(),
                    TEST_UTIL.getConnection(),
                    TableName.META_TABLE_NAME,
                    true);
      assertEquals(regions.toString(), 2, regions.size());

      splitTable(TEST_UTIL, TableName.META_TABLE_NAME,
          Iterators.get(tableRegionLocations.keySet().iterator(), 1).getRegionName());

      splitTable(TEST_UTIL, TableName.META_TABLE_NAME,
          Iterators.get(tableRegionLocations.keySet().iterator(), 2).getRegionName());

      assertEquals(4,
          CatalogAccessor.getTableRegions(TEST_UTIL.getZooKeeperWatcher(),
              TEST_UTIL.getConnection(),
              TableName.META_TABLE_NAME,
              true).size());

      checkBasicOps(tableName, tableRegionList);

      ResultScanner resultScanner =
          TEST_UTIL.getConnection().getTable(TableName.ROOT_TABLE_NAME).getScanner(new Scan());

      splitTable(TEST_UTIL, tableName, new byte[]{0x02, 0x00});

      //Always compact the tables before running a catalog scan
      //to make sure everything can be cleaned up
      TEST_UTIL.compact(tableName, true);
      TEST_UTIL.compact(TableName.META_TABLE_NAME, true);
      hbaseAdmin.runCatalogScan();

      tableRegionList = CatalogAccessor.fullScan(TEST_UTIL.getConnection());
      assertEquals(splits.length + 3, tableRegionList.size());
      checkBasicOps(tableName, tableRegionList);

      mergeRegions(TEST_UTIL, tableName, new byte[]{0x02}, new byte[]{0x02, 0x00});
      TEST_UTIL.compact(tableName, true);
      TEST_UTIL.compact(TableName.META_TABLE_NAME, true);
      hbaseAdmin.runCatalogScan();

      tableRegionList = CatalogAccessor.fullScan(TEST_UTIL.getConnection());
      assertEquals(splits.length + 2, tableRegionList.size());

      mergeRegions(TEST_UTIL,
          TableName.META_TABLE_NAME,
          Iterators.get(tableRegionLocations.keySet().iterator(), 0).getRegionName(),
          Iterators.get(tableRegionLocations.keySet().iterator(), 1).getRegionName());

      checkBasicOps(tableName, tableRegionList);

    } finally {
      TEST_UTIL.deleteTable(tableName);
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 120000)
  public void testSplitNoSplitPoint() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitNoSplitPoint");
    conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", false);
    conf.setBoolean("hbase.split.meta", true);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    HBaseAdmin hbaseAdmin = null;
    try {
      TEST_UTIL.startMiniCluster(1, 3);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();

      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor("cf").setBlocksize(30));
      SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
      // Have more splits so entries in meta span more than one block
      byte[][] splits = algo.split(20);
      hbaseAdmin = new HBaseAdmin(conf);
      hbaseAdmin.createTable(desc, splits);

      List<Result> list =
          CatalogAccessor.fullScan(TEST_UTIL.getConnection());

      LOG.info("Splitting meta");
      int metaServerIndex =
          TEST_UTIL.getHBaseCluster().getServerWith(
            HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(metaServerIndex);
      int regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      hbaseAdmin.split(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());

      int metaSize =0;
      for (int i = 0; i < 300; i++) {
        LOG.debug("Waiting on region to split");
        metaSize =
            CatalogAccessor.getTableRegions(
                TEST_UTIL.getZooKeeperWatcher(),
                TEST_UTIL.getConnection(),
                TableName.META_TABLE_NAME,
                true).size();
        if (metaSize > 1) {
          break;
        }
        Thread.sleep(100);
      }

      assertEquals(2, metaSize);
      LOG.info("Splitting done");
      checkBasicOps(tableName, list);
    } finally {
      if (hbaseAdmin != null) {
        hbaseAdmin.disableTable(tableName);
        hbaseAdmin.deleteTable(tableName);
      }
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  
  private void checkBasicOps(TableName tableName, List<Result> expectedList) throws Exception {
    LOG.info("Splitting done");
   
    // Scan meta after split
    HTable ht = new HTable(conf, TableName.META_TABLE_NAME);
    Scan s = new Scan(HConstants.EMPTY_START_ROW).addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner scanner = ht.getScanner(s);
    Result r = scanner.next();
    int i = 0;
    while (r != null) {
      assertEquals(HRegionInfo.getHRegionInfo(r), HRegionInfo.getHRegionInfo(expectedList.get(i)));
      r = scanner.next();
      i++;
    }

    ht = new HTable(conf, tableName);
    // try adding/retrieving a row to a user region referenced by the first meta
    byte[] rowKey = Bytes.toBytes("f0000000");
    byte[] family = Bytes.toBytes("cf");
    Put put = new Put(rowKey);
    put.add(family, Bytes.toBytes("A"), Bytes.toBytes("1"));
    ht.put(put);
    Get get = new Get(rowKey);
    Result result = ht.get(get);
    assertTrue("Column A value should be 1",
      Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("1"));

    // try adding/retrieving a row to a user region referenced by the second meta
    rowKey = Bytes.toBytes("10000000");
    family = Bytes.toBytes("cf");
    put = new Put(rowKey);
    put.add(family, Bytes.toBytes("A"), Bytes.toBytes("2"));
    ht.put(put);
    get = new Get(rowKey);
    result = new HTable(conf, tableName).get(get);
    assertTrue("Column A value should be 2",
      Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("2"));

    rowKey = Bytes.toBytes("f0000000");
    Delete d = new Delete(rowKey);
    ht.delete(d);
    assertTrue(ht.get(new Get(rowKey)).isEmpty());

    rowKey = Bytes.toBytes("10000000");
    d = new Delete(rowKey);
    ht.delete(d);
    assertTrue(ht.get(new Get(rowKey)).isEmpty());
  }

  public void splitTable(HBaseTestingUtility util, TableName tableName, final byte[] splitKey)
      throws Exception {
    final HTable htable = new HTable(util.getConfiguration(), tableName);
    final HRegionInfo targetRegion = htable.getRegionLocation(splitKey, true).getRegionInfo();

    util.getHBaseAdmin().flush(tableName.getName());
    util.compact(tableName, true);
    for (HRegion region : util.getMiniHBaseCluster().getRegions(tableName)) {
      for (Store store : region.getStores()) {
        store.closeAndArchiveCompactedFiles();
      }
    }
    util.getHBaseAdmin().split(targetRegion.getRegionName(), splitKey);

    util.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        HRegionLocation loc1 = htable.getRegionLocation(targetRegion.getStartKey(), true);
        HRegionLocation loc2 = htable.getRegionLocation(splitKey, true);
        return !loc1.getRegionInfo().getRegionNameAsString().equals(
            loc2.getRegionInfo().getRegionNameAsString());
      }
    });

    //make sure regions are online
    byte[][] keys = {targetRegion.getStartKey(), splitKey};
    for(byte[] el : keys) {
      byte[] key = el;
      if (key.length == 0) {
        key = new byte[]{0x00};
      }
      htable.get(new Get(key));
    }
  }


  public void mergeRegions(HBaseTestingUtility util, TableName tableName,
                           final byte[] startKey1,
                           final byte[] startKey2)
      throws Exception {
    final HTable htable = new HTable(util.getConfiguration(), tableName);
    final HRegionInfo targetRegion1 = htable.getRegionLocation(startKey1, true).getRegionInfo();
    final HRegionInfo targetRegion2 = htable.getRegionLocation(startKey2, true).getRegionInfo();

    HTable metaTable = new HTable(util.getConfiguration(), TableName.ROOT_TABLE_NAME);
    if (!tableName.equals(TableName.META_TABLE_NAME)) {
      metaTable = new HTable(util.getConfiguration(), TableName.META_TABLE_NAME);
    }

    Scan s = new Scan(HConstants.EMPTY_START_ROW).addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner scanner = metaTable.getScanner(s);
    Result r = scanner.next();
    List<HRegionInfo> splitList = new ArrayList<HRegionInfo>();
    while (r != null) {
      splitList.add(HRegionInfo.getHRegionInfo(r));
      r = scanner.next();
    }

    util.getHBaseAdmin().flush(tableName.getName());
    util.compact(tableName, true);
    for (HRegion region : util.getMiniHBaseCluster().getRegions(tableName)) {
      for (Store store : region.getStores()) {
        store.closeAndArchiveCompactedFiles();
      }
    }
    util.getHBaseAdmin().mergeRegions(targetRegion1.getEncodedNameAsBytes(),
        targetRegion2.getEncodedNameAsBytes(),
        false);
    util.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        HRegionLocation loc1 = htable.getRegionLocation(startKey1, true);
        HRegionLocation loc2 = htable.getRegionLocation(startKey2, true);
        return loc1.getRegionInfo().getRegionNameAsString().equals(
            loc2.getRegionInfo().getRegionNameAsString());
      }
    });


    s = new Scan(HConstants.EMPTY_START_ROW).addFamily(HConstants.CATALOG_FAMILY);
    scanner = metaTable.getScanner(s);
    r = scanner.next();
    splitList = new ArrayList<HRegionInfo>();
    while (r != null) {
      splitList.add(HRegionInfo.getHRegionInfo(r));
      r = scanner.next();
    }

    //make sure region is online
    byte[] key = startKey1;
    if (key.length == 0) {
      key = new byte[]{0x00};
    }
    htable.get(new Get(key));
  }
}
