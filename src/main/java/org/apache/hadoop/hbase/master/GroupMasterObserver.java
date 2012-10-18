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
    String groupName = GroupInfo.getGroupString(desc);
    if(getGroupInfoManager().getGroup(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName, HTableDescriptor htd) throws IOException {
    MasterServices master = ctx.getEnvironment().getMasterServices();
    String groupName = GroupInfo.getGroupString(htd);
    if(getGroupInfoManager().getGroup(groupName) == null) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist.");
    }

    List<HRegionInfo> tableRegionList = master.getAssignmentManager().getRegionsOfTable(tableName);
    master.getAssignmentManager().unassign(tableRegionList);
  }

  private GroupInfoManager getGroupInfoManager() {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getAssignmentManager().getBalancer()).getGroupInfoManager();
  }
}
