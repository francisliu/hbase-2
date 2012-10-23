/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.master.GroupAdmin;
import org.apache.hadoop.hbase.master.GroupBasedLoadBalancer;
import org.apache.hadoop.hbase.master.GroupInfo;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
@Category(LargeTests.class)
@SuppressWarnings("rawtypes")
public class TestSecureGroupAdminEndpoint {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user with all permissions
  private static User SUPERUSER;
  // user granted with all global permission
  private static User USER_ADMIN;
  // user with rw permissions
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with no permissions
  private static User USER_NONE;

  private static AccessController ACCESS_CONTROLLER;
  private static SecureGroupAdminEndpoint GROUP_ENDPOINT;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
		TEST_UTIL.getConfiguration().set(
				HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
				GroupBasedLoadBalancer.class.getName());
    conf.set("hbase.coprocessor.master.classes",
        conf.get("hbase.coprocessor.master.classes")+","+SecureGroupAdminEndpoint.class.getName());

    TEST_UTIL.startMiniCluster(1,2);
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster().getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    GROUP_ENDPOINT = (SecureGroupAdminEndpoint)
        TEST_UTIL.getMiniHBaseCluster().getMaster()
           .getCoprocessorHost().findCoprocessor(SecureGroupAdminEndpoint.class.getName());
    // Wait for the ACL table to become available
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME, 5000);



    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);

    // initialize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, HConstants.EMPTY_START_ROW);

    protocol.grant(new UserPermission(Bytes.toBytes(USER_ADMIN.getShortName()),
        Permission.Action.ADMIN, Permission.Action.CREATE, Permission.Action.READ,
        Permission.Action.WRITE));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void verifyAllowed(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  public void verifyAllowed(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyAllowed(user, action);
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
        fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
      } catch (RetriesExhaustedWithDetailsException e) {
        // in case of batch operations, and put, the client assembles a
        // RetriesExhaustedWithDetailsException instead of throwing an
        // AccessDeniedException
        boolean isAccessDeniedException = false;
        for (Throwable ex : e.getCauses()) {
          if (ex instanceof AccessDeniedException) {
            isAccessDeniedException = true;
            break;
          }
        }
        if (!isAccessDeniedException) {
          fail("Not receiving AccessDeniedException for user '" + user.getShortName() + "'");
        }
      } catch (AccessDeniedException ade) {
        // expected result
      }
    }
  }

  public void verifyDenied(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }

  @Test
  public void testGetAddRemove() throws Exception {
    final AtomicLong counter = new AtomicLong(0);

    PrivilegedExceptionAction getGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.getGroup("default");
        return null;
      }
    };
    verifyAllowed(getGroup, SUPERUSER, USER_ADMIN, USER_NONE);

    PrivilegedExceptionAction addGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.addGroup("testGetAddRemove"+counter.incrementAndGet());
        return null;
      }
    };
    verifyDenied(addGroup, USER_NONE);
    verifyAllowed(addGroup, SUPERUSER, USER_ADMIN);

    PrivilegedExceptionAction removeGroup = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.removeGroup("testGetAddRemove"+counter.getAndDecrement());
        return null;
      }
    };
    verifyAllowed(removeGroup, SUPERUSER, USER_ADMIN);
    verifyDenied(removeGroup, USER_NONE);
  }

  @Test
  public void testMoveServer() throws Exception {
    final AtomicLong counter = new AtomicLong(0);
    Set<String> servers = new TreeSet<String>();
    for(int i=1;i<=100;i++) {
      GROUP_ENDPOINT.addGroup("testMoveServer_"+i);
    }

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        String hostPort;
        Set<String> set = new TreeSet<String>();
        hostPort = TEST_UTIL.getMiniHBaseCluster().getRegionServer(1).getServerName().getHostAndPort();
        set.add(hostPort);
        GROUP_ENDPOINT.moveServers(set, "testMoveServer_" + counter.incrementAndGet());
        waitForTransitions(GROUP_ENDPOINT);
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_NONE);
  }

  @Test
  public void testListGroups() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        GROUP_ENDPOINT.listGroups();
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN,USER_NONE);
  }

  private static void waitForTransitions(GroupAdmin gAdmin) throws IOException, InterruptedException {
    while(gAdmin.listServersInTransition().size()>0) {
      Thread.sleep(1000);
    }
  }
}
