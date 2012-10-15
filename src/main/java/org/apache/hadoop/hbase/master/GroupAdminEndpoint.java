package org.apache.hadoop.hbase.master;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.executor.EventHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GroupAdminEndpoint extends BaseEndpointCoprocessor implements GroupAdminProtocol, EventHandler.EventHandlerListener {
	private static final Log LOG = LogFactory.getLog(GroupAdminClient.class);

  private MasterCoprocessorEnvironment menv;
  private MasterServices master;
  private ConcurrentMap<String,String> serversInTransition =
      new ConcurrentHashMap<String,String>();

  @Override
  public void start(CoprocessorEnvironment env) {
    menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
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
			Set<String> servers = groupInfo.getServers();
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
	 * @param servers the server
	 * @param targetGroup the target group
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
  @Override
  public void moveServers(Set<String> servers, String targetGroup)
			throws IOException {
		if ((servers == null) || (StringUtils.isEmpty(targetGroup))) {
			throw new IOException(
					"The region server or the target to move found to be null.");
		}

    GroupMoveServerHandler.MoveServerPlan plan =
        new GroupMoveServerHandler.MoveServerPlan(servers, targetGroup);
    GroupMoveServerHandler handler = null;
    try {
      handler = new GroupMoveServerHandler(master, serversInTransition, getGroupInfoManager(), plan);
      handler.setListener(this);
      master.getExecutorService().submit(handler);
      LOG.info("GroupMoveServerHanndlerSubmitted: "+plan.getTargetGroup());
    } catch(Exception e) {
      LOG.error("Failed to submit GroupMoveServerHandler", e);
      handler.complete();
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

  @Override
  public Map<String, String> listServersInTransition() throws IOException {
    return Collections.unmodifiableMap(serversInTransition);
  }

  private GroupInfoManager getGroupInfoManager() {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getAssignmentManager().getBalancer()).getGroupInfoManager();
  }

  private List<HRegionInfo> getOnlineRegions(String hostPort) throws IOException {
    java.util.List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for(Map.Entry<ServerName, java.util.List<HRegionInfo>> el:
        master.getAssignmentManager().getAssignments().entrySet()) {
      if(el.getKey().getHostAndPort().equals(hostPort)) {
        regions.addAll(el.getValue());
      }
    }
    return regions;
  }

  @Override
  public void beforeProcess(EventHandler event) {
    //do nothing
  }

  @Override
  public void afterProcess(EventHandler event) {
    GroupMoveServerHandler h =
        ((GroupMoveServerHandler)event);
    try {
      h.complete();
    } catch (IOException e) {
      LOG.error("Failed to complete GroupMoveServer with of "+h.getPlan().getServers().size()+
          " servers to group "+h.getPlan().getTargetGroup());
    }
  }

}
