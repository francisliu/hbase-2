package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GroupAdmin {
  /**
   * Get regions of a region server group.
   *
   * @param groupName
   *            the name of the group
   * @return list of regions this group contains
   */
  List<HRegionInfo> listRegionsOfGroup(String groupName) throws IOException;

  /**
   * Get tables of a group.
   *
   * @param groupName
   *            the name of the group
   * @return List of HTableDescriptor
   */
  Collection<String> listTablesOfGroup(String groupName) throws IOException;

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   */
  GroupInfo getGroup(String groupName) throws IOException;

  /**
   * Gets the group info of table.
   *
   * @param tableName the table name
   * @return An instance of GroupInfo.
   */
  GroupInfo getGroupInfoOfTable(byte[] tableName) throws IOException;

  /**
   * Carry out the server movement from one group to another.
   *
   * @param server the server
   * @param targetGroup the target group
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
  void moveServers(Set<String> server, String targetGroup)
      throws IOException, InterruptedException;


  void addGroup(GroupInfo groupInfo) throws IOException;

  void removeGroup(String name) throws IOException;

  /**
   * Gets the existing groups.
   *
   * @return Collection of GroupInfo.
   */
  List<GroupInfo> listGroups() throws IOException;

  GroupInfo getGroupOfServer(String hostPort) throws IOException;

  Map<String, String> listServersInTransition() throws IOException;
}
