package org.apache.hadoop.hbase.master;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class GroupAdminEndpoint extends BaseEndpointCoprocessor implements GroupAdminProtocol {
	private static final Log LOG = LogFactory.getLog(GroupAdminClient.class);

  private MasterCoprocessorEnvironment menv;
  private MasterServices master;

  @Override
  public void start(CoprocessorEnvironment env) {
    menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
  }

  private List<HRegionInfo> getOnlineRegions(String hostPort) throws IOException {
    List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for(Map.Entry<ServerName,List<HRegionInfo>> el:
        master.getAssignmentManager().getAssignments().entrySet()) {
      if(el.getKey().getHostAndPort().equals(hostPort)) {
        regions.addAll(el.getValue());
      }
    }
    return regions;
  }

	/**
	 * Get regions of a region server group.
	 *
	 * @param groupName
	 *            the name of the group
	 * @return list of regions this group contains
	 */
  @Override
  public List<HRegionInfo> listRegionsOfGroup(String groupName) throws IOException {
		List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
		if (groupName == null) {
      throw new NullPointerException("groupName can't be null");
    }

    GroupInfo groupInfo = getGroupInfoManager().getGroup(groupName);
    if (groupInfo == null) {
			return null;
		} else {
			NavigableSet<String> servers = groupInfo.getServers();
      for(ServerName serverName: master.getServerManager().getOnlineServersList()) {
        String hostPort = serverName.getHostAndPort();
        if(servers.contains(hostPort)) {
          List<HRegionInfo> temp = getOnlineRegions(hostPort);
          regions.addAll(temp);
        }
			}
		}
		return regions;
	}

	/**
	 * Get tables of a group.
	 *
	 * @param groupName
	 *            the name of the group
	 * @return List of HTableDescriptor
	 */
  @Override
  public Collection<String> listTablesOfGroup(String groupName) throws IOException {
		Set<String> set = new HashSet<String>();
		if (groupName == null) {
      throw new NullPointerException("groupName can't be null");
    }

    GroupInfo groupInfo = getGroupInfoManager().getGroup(groupName);
    if (groupInfo == null) {
			return null;
		} else {
      HTableDescriptor[] tables = master.getTableDescriptors().getAll().values().toArray(new HTableDescriptor[0]);
      for (HTableDescriptor table : tables) {
        if(GroupInfo.getGroupString(table).equals(groupName))
          set.add(table.getNameAsString());
      }
    }
		return set;
	}


	/**
	 * Gets the group information.
	 *
	 * @param groupName the group name
	 * @return An instance of GroupInfo
	 */
  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
			return getGroupInfoManager().getGroup(groupName);
	}


	/**
	 * Gets the group info of table.
	 *
	 * @param tableName the table name
	 * @return An instance of GroupInfo.
	 */
  @Override
  public GroupInfo getGroupInfoOfTable(byte[] tableName) throws IOException {
		HTableDescriptor des;
		GroupInfo tableRSGroup;
    des =  master.getTableDescriptors().get(tableName);
		String group = GroupInfo.getGroupString(des);
		tableRSGroup = getGroupInfoManager().getGroup(group);
		return tableRSGroup;
	}

	/**
	 * Carry out the server movement from one group to another.
	 *
	 * @param server the server
	 * @param targetGroup the target group
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
  @Override
  public void moveServer(String server, String targetGroup)
			throws IOException {
		if ((server == null) || (StringUtils.isEmpty(targetGroup))) {
			throw new IOException(
					"The region server or the target to move found to be null.");
		}

    String sourceGroup = getGroupOfServer(server).getName();
    long period = 10000;
    long tries = 30*60*1000/period;
    boolean isTrans = false;

    String transName = sourceGroup;
    if(!sourceGroup.startsWith(GroupInfo.TRANSITION_GROUP_PREFIX)) {
      transName = GroupInfo.TRANSITION_GROUP_PREFIX+sourceGroup+"_TO_"+targetGroup;
      getGroupInfoManager().addGroup(new GroupInfo(transName, new TreeSet<String>()));
      isTrans = false;
    }


    getGroupInfoManager().moveServer(server, sourceGroup, transName);
    int size = 0;
    do {
      master.getAssignmentManager().unassign(getOnlineRegions(server));
      try {
        Thread.sleep(period);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted", e);
      }
    } while(getOnlineRegions(server).size() > 0 && --tries > 0);

    if(tries == 0) {
      throw new DoNotRetryIOException("Waiting too long for regions to be unassigned.");
    }
    getGroupInfoManager().moveServer(server, transName, targetGroup);
    if(!isTrans) {
      getGroupInfoManager().removeGroup(transName);
    }
	}

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    getGroupInfoManager().addGroup(groupInfo);
  }

  @Override
  public void removeGroup(String name) throws IOException {
    getGroupInfoManager().removeGroup(name);
  }

	/**
	 * Gets the existing groups.
	 *
	 * @return Collection of GroupInfo.
	 */
  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return getGroupInfoManager().listGroups();
  }

  @Override
  public GroupInfo getGroupOfServer(String hostPort) throws IOException {
    return getGroupInfoManager().getGroupOfServer(hostPort);
  }

  private GroupInfoManager getGroupInfoManager() {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getAssignmentManager().getBalancer()).getGroupInfoManager();
  }
}
