/**
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.zookeeper.RootTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Basic mock Server for handler tests.
 */
public class MockServer implements Server {
  private static final Log LOG = LogFactory.getLog(MockServer.class);
  final static ServerName NAME = ServerName.valueOf("MockServer", 123, -1);
  
  boolean stopped;
  boolean aborted;
  final ZooKeeperWatcher zk;
  final HBaseTestingUtility htu;

  @SuppressWarnings("unused")
  public MockServer() throws ZooKeeperConnectionException, IOException {
    // Shutdown default constructor by making it private.
    this(null);
  }

  public MockServer(final HBaseTestingUtility htu)
  throws ZooKeeperConnectionException, IOException {
    this(htu, true);
  }

  /**
   * @param htu Testing utility to use
   * @param zkw If true, create a zkw.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  public MockServer(final HBaseTestingUtility htu, final boolean zkw)
  throws ZooKeeperConnectionException, IOException {
    this.htu = htu;
    this.zk = zkw?
      new ZooKeeperWatcher(htu.getConfiguration(), NAME.toString(), this, true):
      null;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.fatal("Abort why=" + why, e);
    stop(why);
    this.aborted = true;
  }

  @Override
  public void stop(String why) {
    LOG.debug("Stop why=" + why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public Configuration getConfiguration() {
    return this.htu.getConfiguration();
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return this.zk;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
    return null;
  }

  public RootTableLocator getRootTableLocator() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return NAME;
  }

  @Override
  public boolean isAborted() {
    // TODO Auto-generated method stub
    return this.aborted;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }
}