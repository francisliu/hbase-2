package org.apache.hadoop.hbase.group;

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DistributedHBaseCluster;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(IntegrationTests.class)
public class IntegrationTestGroup {
  protected static final Log LOG = LogFactory.getLog(IntegrationTestGroup.class);
  private static final int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster
  private static String tablePrefix = null;
	private static Random rand = new Random();
  private static GroupTestingUtility groupUtil;
  private static GroupAdminClient groupAdmin;

  /** A soft limit on how long we should run */
  private static final String RUN_TIME_KEY = "hbase.%s.runtime";

  protected IntegrationTestingUtility util;
  protected DistributedHBaseCluster cluster;

  @Before
  public void setUp() throws Exception {
    LOG.info("Setting up IntegrationTestGroup");
    tablePrefix = IntegrationTestGroup.class.getSimpleName().replaceAll("\\.","-")+"-";
    util = new IntegrationTestingUtility();
    groupUtil = new GroupTestingUtility(util);
    groupAdmin = new VerifyingGroupAdminClient(util.getConfiguration());
    LOG.info("Initializing cluster with " + NUM_SLAVES_BASE + " servers");
    util.initializeCluster(NUM_SLAVES_BASE);
    LOG.info("Done initializing cluster");
    cluster = (DistributedHBaseCluster)util.getHBaseClusterInterface();
    deleteTableIfNecessary();
    deleteGroups();
//    util.createTable(Bytes.toBytes(tableName), Bytes.toBytes("f"));
//    util.createMultiRegions(util.getConfiguration(),
//        new HTable(util.getConfiguration(),tableName),Bytes.toBytes("f"), 100);
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("Restoring the cluster");
    util.restoreCluster();
    LOG.info("Done restoring the cluster");
  }


	@Test
	public void testMoveServers() throws Exception, InterruptedException {
    LOG.info("testMoveServers");

    addGroup(groupAdmin, "bar", 3);
    groupAdmin.addGroup("foo");

    GroupInfo barGroup = groupAdmin.getGroupInfo("bar");
    GroupInfo fooGroup = groupAdmin.getGroupInfo("foo");
    assertEquals(3, barGroup.getServers().size());
    assertEquals(0, fooGroup.getServers().size());

    LOG.info("moving servers "+barGroup.getServers()+" to group foo");
    groupAdmin.moveServers(barGroup.getServers(), fooGroup.getName());

    barGroup = groupAdmin.getGroupInfo("bar");
    fooGroup = groupAdmin.getGroupInfo("foo");
		assertEquals(0,barGroup.getServers().size());
    assertEquals(3,fooGroup.getServers().size());

    LOG.info("moving servers "+fooGroup.getServers()+" to group default");
    groupAdmin.moveServers(fooGroup.getServers(), GroupInfo.DEFAULT_GROUP);

    groupUtil.waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return cluster.getClusterStatus().getServers().size() !=
            groupAdmin.getGroupInfo(GroupInfo.DEFAULT_GROUP).getServers().size();
      }
    });
    fooGroup = groupAdmin.getGroupInfo("foo");
    assertEquals(0, fooGroup.getServers().size());


    LOG.info("Remove group "+barGroup.getName());
    groupAdmin.removeGroup(barGroup.getName());
    assertEquals(null, groupAdmin.getGroupInfo(barGroup.getName()));
    LOG.info("Remove group "+fooGroup.getName());
    groupAdmin.removeGroup(fooGroup.getName());
    assertEquals(null, groupAdmin.getGroupInfo(fooGroup.getName()));
	}

	@Test
	public void testTableMove() throws Exception, InterruptedException {
    LOG.info("testTableMove");
		final String tableName = tablePrefix + rand.nextInt();
		final byte[] tableNameBytes = Bytes.toBytes(tableName);
		final byte[] familyNameBytes = Bytes.toBytes("f");
		GroupAdminClient groupAdmin = new GroupAdminClient(util.getConfiguration());
    String newGroupName = "g_" + rand.nextInt();
		final GroupInfo newGroup = addGroup(groupAdmin, newGroupName, 2);

		HTable ht = util.createTable(tableNameBytes, familyNameBytes);
		assertTrue(util.createMultiRegions(util.getConfiguration(), ht,
				familyNameBytes, 4) == 4);
    groupUtil.waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        List<String> regions = groupUtil.getTableRegionMap().get(tableName);
        if(regions == null)
          return true;
        return groupUtil.getTableRegionMap().get(tableName).size() < 5;
      }
    });

		GroupInfo tableGrp = groupAdmin.getGroupInfoOfTable(tableName);
		assertTrue(tableGrp.getName().equals(GroupInfo.DEFAULT_GROUP));

    //change table's group
    LOG.info("Moving table "+tableName+" to "+newGroup.getName());
    groupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());

    //verify group change
		assertEquals(newGroup.getName(),
        groupAdmin.getGroupInfoOfTable(tableName).getName());

    groupUtil.waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        Map<ServerName, List<String>> serverMap =
            groupUtil.getTableServerRegionMap().get(tableName);
        boolean found = true;
        int count = 0;
        if(serverMap != null) {
          for (ServerName rs : serverMap.keySet()) {
            if (newGroup.containsServer(rs.getHostAndPort())) {
              count += serverMap.get(rs).size();
            }
          }
        }
        return count != 5;
      }
    });
	}

	@Test
	public void testRegionMove() throws Exception, InterruptedException {
		final GroupInfo newGroup = addGroup(groupAdmin, "g_" + rand.nextInt(), 1);
		final String tableName = tablePrefix + rand.nextInt();
		final byte[] tableNameBytes = Bytes.toBytes(tableName);
		final byte[] familyNameBytes = Bytes.toBytes("f");
		HTable ht = util.createTable(tableNameBytes, familyNameBytes);

		// All the regions created below will be assigned to the default group.
    assertEquals(5, util.createMultiRegions(util.getConfiguration(), ht, familyNameBytes, 5));
    groupUtil.waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        List<String> regions = groupUtil.getTableRegionMap().get(tableName);
        if(regions == null)
          return true;
        return groupUtil.getTableRegionMap().get(tableName).size() < 6;
      }
    });

    //get target region to move
    Map<ServerName,List<String>> assignMap =
        groupUtil.getTableServerRegionMap().get(tableName);
    String targetRegion = null;
    for(ServerName server : assignMap.keySet()) {
      targetRegion = assignMap.get(server).size() > 0 ? assignMap.get(server).get(0) : null;
      if(targetRegion != null) {
        break;
      }
    }
    //get server which is not a member of new group
    ServerName targetServer = null;
    for(ServerName server : cluster.getClusterStatus().getServers()) {
      if(!newGroup.containsServer(server.getHostAndPort())) {
        targetServer = server;
        break;
      }
    }
    final HRegionInterface targetRS =
        util.getHBaseAdmin().getConnection().getHRegionConnection(targetServer.getHostname(), targetServer.getPort());

    //move target server to group
    groupAdmin.moveServers(Sets.newHashSet(targetServer.getHostAndPort()), newGroup.getName());
    groupUtil.waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return targetRS.getOnlineRegions().size() > 0;
      }
    });

		// Lets move this region to the new group.
		util.getHBaseAdmin().move(Bytes.toBytes(HRegionInfo.encodeRegionName(Bytes.toBytes(targetRegion))),
        Bytes.toBytes(targetServer.getServerName()));
    groupUtil.waitForCondition(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        return groupUtil.getTableRegionMap().get(tableName).size() != 6 ||
            cluster.getClusterStatus().getRegionsInTransition().size() > 0;
      }
    });

    //verify that targetServer didn't open it
		assertFalse(targetRS.getOnlineRegions().contains(targetRegion));
	}

  @Test
  public void testStopMaster() throws IOException {
    ServerName masterName = cluster.getClusterStatus().getMaster();
    cluster.stopMaster(cluster.getClusterStatus().getMaster());
    cluster.startMaster(masterName.getHostname());
  }

  private void deleteTableIfNecessary() throws IOException {
    for (HTableDescriptor desc : util.getHBaseAdmin().listTables(tablePrefix+".*")) {
      util.deleteTable(desc.getName());
    }
  }

  private void deleteGroups() throws IOException {
		GroupAdminClient groupAdmin = new GroupAdminClient(util.getConfiguration());
    for(GroupInfo group: groupAdmin.listGroups()) {
      if(!group.getName().equals(GroupInfo.DEFAULT_GROUP)) {
        groupAdmin.moveTables(group.getTables(),GroupInfo.DEFAULT_GROUP);
        groupAdmin.moveServers(group.getServers(),GroupInfo.DEFAULT_GROUP);
        groupAdmin.removeGroup(group.getName());
      }
    }
  }

	static GroupInfo addGroup(GroupAdminClient gAdmin, String groupName,
			int serverCount) throws IOException, InterruptedException {
		GroupInfo defaultInfo = gAdmin
				.getGroupInfo(GroupInfo.DEFAULT_GROUP);
		assertTrue(defaultInfo != null);
		assertTrue(defaultInfo.getServers().size() >= serverCount);
		gAdmin.addGroup(groupName);

    Set<String> set = new HashSet<String>();
    for(String server: defaultInfo.getServers()) {
      if(set.size() == serverCount) {
        break;
      }
      set.add(server);
    }
    LOG.info("moving servers "+set+" to group "+groupName);
    gAdmin.moveServers(set, groupName);
    GroupInfo result = gAdmin.getGroupInfo(groupName);
		assertTrue(result.getServers().size() >= serverCount);
    return result;
	}
}
