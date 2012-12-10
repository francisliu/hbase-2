package org.apache.hadoop.hbase.group;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

public class VerifyingGroupAdminClient extends GroupAdminClient {
  private HTable table;
  private ZooKeeperWatcher zkw;

  public VerifyingGroupAdminClient(Configuration conf)
      throws IOException {
    super(conf);
    table = new HTable(conf, GroupInfoManager.GROUP_TABLE_NAME_BYTES);
    zkw = new ZooKeeperWatcher(conf, this.getClass().getSimpleName(), null);
  }

  @Override
  public void addGroup(String groupName) throws IOException {
    super.addGroup(groupName);
    verify();
  }

  @Override
  public void moveServers(Set<String> servers, String targetGroup) throws IOException {
    super.moveServers(servers, targetGroup);
    verify();
  }

  @Override
  public void moveTables(Set<String> tables, String targetGroup) throws IOException {
    super.moveTables(tables, targetGroup);
    verify();
  }

  @Override
  public void removeGroup(String name) throws IOException {
    super.removeGroup(name);
    verify();
  }

  public void verify() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Get get = new Get(GroupInfoManager.ROW_KEY);
    get.addFamily(GroupInfoManager.SERVER_FAMILY_BYTES);
    get.addFamily(GroupInfoManager.TABLE_FAMILY_BYTES);
    Map<String, GroupInfo> groupMap = Maps.newHashMap();
    Set<GroupInfo> specialList = Sets.newHashSet();
    Set<GroupInfo> zList = Sets.newHashSet();

    Result result = table.get(get);
    if(!result.isEmpty()) {
      NavigableMap<byte[],NavigableMap<byte[],byte[]>> dataMap =
          result.getNoVersionMap();
      for(byte[] groupNameBytes:
          dataMap.get(GroupInfoManager.SERVER_FAMILY_BYTES).keySet()) {
        TreeSet<String> servers =
            mapper.readValue(
                Bytes.toString(dataMap.get(GroupInfoManager.SERVER_FAMILY_BYTES).get(groupNameBytes)),
                new TypeReference<TreeSet<String>>() {});
        TreeSet<String> tables =
            mapper.readValue(
                Bytes.toString(dataMap.get(GroupInfoManager.TABLE_FAMILY_BYTES).get(groupNameBytes)),
                new TypeReference<TreeSet<String>>() {});
        GroupInfo groupInfo =
            new GroupInfo(Bytes.toString(groupNameBytes), servers, tables);
        groupMap.put(groupInfo.getName(), groupInfo);
        for(String special: GroupBasedLoadBalancer.SPECIAL_TABLES) {
          if(tables.contains(special)) {
            specialList.add(groupInfo);
            break;
          }
        }
      }
    }
    groupMap.put(GroupInfo.DEFAULT_GROUP, super.getGroupInfo(GroupInfo.DEFAULT_GROUP));
    for(String special: GroupBasedLoadBalancer.SPECIAL_TABLES) {
      GroupInfo groupInfo = groupMap.get(GroupInfo.DEFAULT_GROUP);
      if(groupInfo.getTables().contains(special)) {
        specialList.add(groupInfo);
        break;
      }
    }
    Assert.assertEquals(Sets.newHashSet(groupMap.values()),
        Sets.newHashSet(super.listGroups()));
    try {
      String data = Bytes.toString(
          ZKUtil.getData(zkw, ZKUtil.joinZNode(zkw.baseZNode,"groupInfo")));
      zList.addAll((List<GroupInfo>)mapper.readValue(data, new TypeReference<List<GroupInfo>>(){}));
      Assert.assertEquals(zList.size(),specialList.size());
      for(GroupInfo groupInfo: zList) {
        if(groupInfo.getName().equals(GroupInfo.OFFLINE_DEFAULT_GROUP)) {
          Assert.assertEquals(groupMap.get(GroupInfo.DEFAULT_GROUP).getServers(), groupInfo.getServers());
          Assert.assertEquals(groupMap.get(GroupInfo.DEFAULT_GROUP).getTables(), groupInfo.getTables());
        } else {
          Assert.assertTrue(specialList.contains(groupInfo));
        }
      }
    } catch (KeeperException e) {
      throw new IOException("ZK verification failed", e);
    }
  }

}
