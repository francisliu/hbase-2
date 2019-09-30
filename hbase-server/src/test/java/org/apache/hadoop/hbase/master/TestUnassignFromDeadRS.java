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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This verifies a bugfix in a scenario
 * where the time period between dead RS detection and SSH starts is quick
 * and unassign on a region hosted by the dead RS is called. This causes
 * the unassign call to use regionStats.regionOffline() and in zkless
 * SSH won't be able to recover the region as it's been remove from regionStates.
 *
 * Fix is to just move it to offline state so it stays in RIT.
 */
@Category(MediumTests.class)
public class TestUnassignFromDeadRS {
  final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();
  static HBaseAdmin admin;

  static void setupOnce() throws Exception {
    conf.setBoolean("hbase.assignment.usezk", false);
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);

    TEST_UTIL.startMiniCluster(1, 4, null, MyMaster.class, MiniHBaseClusterRegionServer.class);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupOnce();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=60000)
  public void testUnassignRegion() throws Exception {
    String table = "testUnassignRegion";
    try {
      MyMaster.delayEnabled.set(true);
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);
      HTable htable = new HTable(conf, desc.getTableName());
      Map.Entry<HRegionInfo, ServerName> entry = htable.getRegionLocations().firstEntry();
      ServerName moveServer = null;
      for (RegionServerThread thread :
          TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
        moveServer = thread.getRegionServer().getServerName();
        if (!moveServer.equals(entry.getValue())) {
          break;
        }
      }
      TEST_UTIL.getMiniHBaseCluster().stopRegionServer(entry.getValue());
      TEST_UTIL.getMiniHBaseCluster().waitForRegionServerToStop(entry.getValue(), 10000);
      admin.unassign(entry.getKey().getRegionName(), false);
      htable.get(new Get(new byte[]{0}));
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      MyMaster.delayEnabled.set(false);
    }
  }

  @Test (timeout=60000)
  public void testDisableTable() throws Exception {
    TableName table = TableName.valueOf("testDisableTable");
    try {
      MyMaster.delayEnabled.set(true);
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);
      HTable htable = new HTable(conf, desc.getTableName());
      Map.Entry<HRegionInfo, ServerName> entry = htable.getRegionLocations().firstEntry();
      ServerName moveServer = null;
      for (RegionServerThread thread :
          TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
        moveServer = thread.getRegionServer().getServerName();
        if (!moveServer.equals(entry.getValue())) {
          break;
        }
      }
      TEST_UTIL.getMiniHBaseCluster().stopRegionServer(entry.getValue());
      TEST_UTIL.getMiniHBaseCluster().waitForRegionServerToStop(entry.getValue(), 10000);
      //Disable table will timeout if region won't get offlined
      admin.disableTable(table);
    } finally {
      admin.deleteTable(table.getName());
      MyMaster.delayEnabled.set(false);
    }
  }

  public static class MyMaster extends HMaster {
    public static AtomicBoolean delayEnabled = new AtomicBoolean(false);
    private volatile AssignmentManager am;
    private volatile RegionStates states;

    public MyMaster(Configuration conf, CoordinatedStateManager csm)
        throws IOException, KeeperException, InterruptedException {
      super(conf, csm);
    }

    @Override
    public synchronized AssignmentManager getAssignmentManager() {
      if (am == null) {
        am = Mockito.spy(super.getAssignmentManager());
        states = Mockito.spy(assignmentManager.getRegionStates());
        Mockito.when(am.getRegionStates()).thenReturn(states);

        Answer answer = new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            if (delayEnabled.get()) {
              //Simulate an time window between a dead RS is detected
              //and the SSH is actually triggered
              //We add a delay to this method since it's the first method
              //for zkless that returns some state to SSH
              //consequently it is also the regions returned by this method
              //that changes because of the race condition bug
              Thread.sleep(20000);
            }
            return assignmentManager.getRegionStates().getServerRegions(
                (ServerName) invocation.getArguments()[0]);
          }
        };
        Mockito.doAnswer(answer).when(states).getServerRegions(Mockito.any(ServerName.class));
      }
      return am;
    }
  }
}
