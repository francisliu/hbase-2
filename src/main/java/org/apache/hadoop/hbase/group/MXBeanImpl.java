/**
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MXBeanImpl implements MXBean {
  private static final Log LOG = LogFactory.getLog(MXBeanImpl.class);

  private static MXBeanImpl instance = null;

  private GroupAdmin groupAdmin;
  private MasterServices master;

  public synchronized static MXBeanImpl init(
      final GroupAdmin groupAdmin,
      MasterServices master) {
    LOG.info("inited", new Exception());
    if (instance == null) {
      instance = new MXBeanImpl(groupAdmin, master);
    }
    return instance;
  }

  protected MXBeanImpl(final GroupAdmin groupAdmin,
      MasterServices master) {
    this.groupAdmin = groupAdmin;
    this.master = master;
  }

  @Override
  public Map<String, Map<String, HServerLoad>> getServerLoadByGroup() throws IOException {
    LOG.info("get serverload", new Exception());
    Map<String, Map<String,HServerLoad>> data = new HashMap<String, Map<String,HServerLoad>>();
    for (final Map.Entry<ServerName, HServerLoad> entry :
      master.getServerManager().getOnlineServers().entrySet()) {
      GroupInfo groupInfo = groupAdmin.getGroupOfServer(entry.getKey().getHostAndPort());
      if(!data.containsKey(groupInfo.getName())) {
        data.put(groupInfo.getName(), new HashMap<String,HServerLoad>());
      }
      data.get(groupInfo.getName()).put(entry.getKey().getHostAndPort(), entry.getValue());
    }
    return data;
  }

//  @Override
//  public List<GroupInfo> getGroups() throws IOException {
//    LOG.info("get groups", new Exception());
//    return groupAdmin.listGroups();
//  }

  @Override
  public Map<String, String> getServersInTransition() throws IOException {
    LOG.info("get in trans", new Exception());
    return groupAdmin.listServersInTransition();
  }
}
