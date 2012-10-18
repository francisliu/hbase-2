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
package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.GroupAdminEndpoint;
import org.apache.hadoop.hbase.master.GroupAdminProtocol;
import org.apache.hadoop.hbase.master.GroupInfo;

import java.io.IOException;
import java.util.Set;

public class SecureGroupAdminEndpoint extends GroupAdminEndpoint implements GroupAdminProtocol{
  private MasterCoprocessorEnvironment menv;

  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);
    menv = (MasterCoprocessorEnvironment)env;
  }

  @Override
  public void moveServers(Set<String> hostPorts, String dstGroup) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.ADMIN);
    super.moveServers(hostPorts, dstGroup);
  }

  @Override
  public void removeGroup(String groupName) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.ADMIN);
    super.removeGroup(groupName);
  }

  @Override
  public void addGroup(GroupInfo groupInfo) throws IOException {
    getAccessController().requirePermission(HConstants.ROOT_TABLE_NAME, null, null, Permission.Action.ADMIN);
    super.addGroup(groupInfo);
  }

  private AccessController getAccessController() {
    return (AccessController)menv.getMasterServices()
        .getCoprocessorHost().findCoprocessor(AccessController.class.getName());
  }
}
