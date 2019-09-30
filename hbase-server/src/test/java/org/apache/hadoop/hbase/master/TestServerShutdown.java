package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that region state RPC calls for root and meta are served by their own handlers and 
 * not affected by regular handlers & readers getting saturated by the region state RPC calls for user regions.
 */
@Category(MediumTests.class)
public class TestServerShutdown {
  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();
  private static HBaseAdmin admin;
  private static final Log LOG = LogFactory.getLog(TestAssignmentManagerOnCluster.class);

  static void setupOnce() throws Exception {
    TEST_UTIL.startMiniCluster(1, 4, null);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Use RPC based region assignment
    conf.setBoolean("hbase.assignment.usezk", false);
    // Reduce the default handler and reader threads so that they get saturated 
    conf.setInt(HConstants.MASTER_HANDLER_COUNT, 1);
    conf.setInt("hbase.ipc.server.read.threadpool.size", 1);
    setupOnce();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  
  @Test(timeout = 240000)
  public void testMultipleServerShutdown() throws Exception {
    String table = "testMultipleServerShutdown";
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
    desc.addFamily(new HColumnDescriptor(FAMILY));
    SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
    byte[][] splits = algo.split(500);
    admin.createTable(desc, splits);
    RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    List<HRegionInfo> regionInfoList = regionStates.getRegionsOfTable(TableName.valueOf(table));
    // move all user regions to region server 1
    for (HRegionInfo regionInfo : regionInfoList) {
      moveRegionToServer(regionInfo, 1);
    }
    // move root/meta to region server 0
    RegionServerThread metaServer = moveRegionToServer(HRegionInfo.FIRST_META_REGIONINFO, 0);
    moveRegionToServer(HRegionInfo.ROOT_REGIONINFO, 0);

    // Stop server 1
    RegionServerThread randomServer =
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1);
    randomServer.getRegionServer().stop(
      "Stopping server with user regions" + randomServer.getRegionServer().getServerName());
    TEST_UTIL.getMiniHBaseCluster().hbaseCluster.waitOnRegionServer(randomServer);
   
    Thread.sleep(3000);
    // Stop meta server
    metaServer.getRegionServer().stop("Stopping meta server" + metaServer.getRegionServer().getServerName());
    TEST_UTIL.getMiniHBaseCluster().hbaseCluster.waitOnRegionServer(metaServer);
    waitForRSShutdownToStartAndFinish(randomServer.getRegionServer().getServerName());
    waitForRSShutdownToStartAndFinish(metaServer.getRegionServer().getServerName());
    TEST_UTIL.getMiniHBaseCluster().getMaster().assignmentManager
        .waitUntilNoRegionsInTransition(120000);
    // Need to wait as we will to try fetch online regions from region server next
    Thread.sleep(3000);
    int numFound = 0;
    boolean isMetaFound = false;
    boolean isRootFound = false;
    NavigableSet<String> online = new TreeSet<String>();
    for (RegionServerThread rst : TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads()) {
      for (HRegionInfo region :
          ProtobufUtil.getOnlineRegions(rst.getRegionServer().getRSRpcServices())) {
        if (TableName.META_TABLE_NAME.equals(region.getTable())) {
          isMetaFound = true;
        } else if (TableName.ROOT_TABLE_NAME.equals(region.getTable())) {
          isRootFound = true;
        } // Lets not consider other system tables
        else if (!region.getTable().isSystemTable()) {
          numFound += 1;
          online.add(region.getRegionNameAsString());
        }
      }
    }
    for (HRegionInfo region : regionInfoList) {
      if (!online.contains(region.getRegionNameAsString())) {
        LOG.debug("Missing region: " + region);
      }
    }
    assertEquals(regionInfoList.size(), numFound);
    assertTrue(isRootFound);
    assertTrue(isMetaFound);
  }
  
  private void waitForRSShutdownToStartAndFinish(
      ServerName serverName) throws InterruptedException {
    ServerManager sm = TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager();
    while (sm.areDeadServersInProgress()) {
      LOG.debug("Server [" + serverName + "] still being processed, waiting");
      Thread.sleep(100);
    }
    LOG.debug("Server [" + serverName + "] done with server shutdown processing");
  }
  
  private RegionServerThread moveRegionToServer(HRegionInfo regionInfo,
      int index) throws Exception {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    RegionServerThread hrs = TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(index);
    ServerName serverName = hrs.getRegionServer().getServerName();
    master.move(regionInfo.getEncodedNameAsBytes(), Bytes.toBytes(serverName.getServerName()));
    TEST_UTIL.assertRegionOnServer(regionInfo, serverName, 6000);
    return hrs;
  }

}

