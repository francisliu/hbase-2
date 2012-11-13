/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class GroupMasterObserver extends BaseMasterObserver {

    private MasterCoprocessorEnvironment menv;

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    menv = (MasterCoprocessorEnvironment)ctx;
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    //We bypass group checking if the table being created is the group table
    if(getBalancer().isOnline()) {
      String groupName = GroupInfo.getGroupProperty(desc);
      if(getGroupInfoManager().getGroup(groupName) == null) {
        throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
      }
    } else if (!Bytes.equals(desc.getName(), GroupInfoManager.GROUP_TABLE_NAME_BYTES)) {
      throw new DoNotRetryIOException("Failed to retrieve GroupInfoManager");
    }
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HTableDescriptor htd) throws IOException {
    MasterServices master = ctx.getEnvironment().getMasterServices();
    String groupName = GroupInfo.getGroupProperty(htd);
    if(getGroupInfoManager().getGroup(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }

    List<HRegionInfo> tableRegionList = master.getAssignmentManager().getRegionsOfTable(tableName);
    master.getAssignmentManager().unassign(tableRegionList);
  }

  private GroupBasedLoadBalancer getBalancer() {
    return (GroupBasedLoadBalancer)menv.getMasterServices().getLoadBalancer();
  }

  private GroupInfoManager getGroupInfoManager() throws IOException {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getLoadBalancer()).getGroupInfoManager();
  }
}
