package org.apache.hadoop.hbase.group;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.fail;

public class GroupTestingUtility {
  protected static final Log LOG = LogFactory.getLog(IntegrationTestGroup.class);
  private IntegrationTestingUtility util;

  public GroupTestingUtility(IntegrationTestingUtility util) {
    this.util = util;
  }

  public Map<String,List<String>> getTableRegionMap() throws IOException {
    Map<String,List<String>> map = Maps.newTreeMap();
    Map<String,Map<ServerName,List<String>>> tableServerRegionMap
        = getTableServerRegionMap();
    for(String tableName : tableServerRegionMap.keySet()) {
      if(!map.containsKey(tableName)) {
        map.put(tableName, new LinkedList<String>());
      }
      for(List<String> subset: tableServerRegionMap.get(tableName).values()) {
        map.get(tableName).addAll(subset);
      }
    }
    return map;
  }

  public Map<String,Map<ServerName,List<String>>> getTableServerRegionMap() throws IOException {
    Map<String,Map<ServerName,List<String>>> map = Maps.newTreeMap();
    ClusterStatus status = util.getHBaseClusterInterface().getClusterStatus();
    for(ServerName serverName : status.getServers()) {
      for(HServerLoad.RegionLoad rl : status.getLoad(serverName).getRegionsLoad().values()) {
        String tableName = Bytes.toString(HRegionInfo.getTableName(rl.getName()));
        if(!map.containsKey(tableName)) {
          map.put(tableName, new TreeMap<ServerName, List<String>>());
        }
        if(!map.get(tableName).containsKey(serverName)) {
          map.get(tableName).put(serverName, new LinkedList<String>());
        }
        map.get(tableName).get(serverName).add(rl.getNameAsString());
      }
    }
    return map;
  }

  public static void waitForCondition(PrivilegedExceptionAction<Boolean> action) throws Exception {
      waitForCondition(5*60000, action);
  }

  public static void waitForCondition(long timeout, PrivilegedExceptionAction<Boolean> action) throws Exception {
    long sleepInterval = 100;
    long tries = timeout/sleepInterval;
    int i = 0;
    while(action.run()) {
      if(i==0) {
        StackTraceElement el = Thread.currentThread().getStackTrace()[2];
        if(el.getMethodName().equals("waitForCondition")) {
          el = Thread.currentThread().getStackTrace()[3];
        }
        LOG.info("Waiting for method: "+el.getClassName()+"."+
            el.getMethodName()+"("+el.getFileName()+":"+el.getLineNumber()+")");
      }
      Thread.sleep(sleepInterval);
      if(tries-- < 0) {
        fail("Timeout");
      }
      i = (i+1) % 10;
    }
  }
}
