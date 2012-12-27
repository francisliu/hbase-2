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

import com.google.common.collect.Sets;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GroupMasterObserver extends BaseMasterObserver {
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(GroupMasterObserver.class);

  private MasterCoprocessorEnvironment menv;
  private GroupAdmin groupAdmin;

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    menv = (MasterCoprocessorEnvironment)ctx;
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    String groupName = desc.getValue(GroupInfo.TABLEDESC_PROP_GROUP);
    if(groupName == null) {
      return;
    }

    GroupInfo groupInfo = getGroupAdmin().getGroupInfo(groupName);
    if(groupInfo == null) {
      throw new ConstraintException("Group "+groupName+" does not exist.");
    }
    //we remove the property since it is ephemeral
    desc.remove(GroupInfo.TABLEDESC_PROP_GROUP);
    getGroupAdmin().moveTables(Sets.newHashSet(desc.getNameAsString()), groupName);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
    if(tableName.length > 0) {
      String table = Bytes.toString(tableName);
      GroupInfo group = getGroupAdmin().getGroupInfoOfTable(table);
      LOG.debug("Removing deleted table from table group "+group.getName());
      if (!GroupInfo.DEFAULT_GROUP.equals(group.getName())) {
        getGroupAdmin().moveTables(Sets.newHashSet(table), GroupInfo.DEFAULT_GROUP);
      }
    }
  }

  private GroupAdmin getGroupAdmin() {
    if(groupAdmin == null) {
      groupAdmin = (GroupAdmin)
          menv.getMasterServices().getCoprocessorHost().findCoprocessor(GroupAdminEndpoint.class.getName());
      if(groupAdmin == null) {
        groupAdmin = (GroupAdmin)
            menv.getMasterServices().getCoprocessorHost().findCoprocessor("SecureGroupAdminEndpoint");
      }
    }
    return groupAdmin;
  }
}
