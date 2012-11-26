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
package org.apache.hadoop.hbase.group;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GroupMasterObserver extends BaseMasterObserver {

    private MasterCoprocessorEnvironment menv;

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    menv = (MasterCoprocessorEnvironment)ctx;
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
    String table = Bytes.toString(tableName);
    String groupName = getGroupInfoManager().getGroupOfTable(table);
    if(!GroupInfo.DEFAULT_GROUP.equals(groupName)) {
      getGroupInfoManager().moveTables(Sets.newHashSet(table), GroupInfo.DEFAULT_GROUP);
    }
  }

  private GroupBasedLoadBalancer getBalancer() {
    return (GroupBasedLoadBalancer)menv.getMasterServices().getLoadBalancer();
  }

  private GroupInfoManager getGroupInfoManager() throws IOException {
    return ((GroupBasedLoadBalancer)menv.getMasterServices().getLoadBalancer()).getGroupInfoManager();
  }
}
