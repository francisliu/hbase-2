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
package org.apache.hadoop.hbase.zookeeper;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.MetaRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Utility class to perform operation (get/wait for/verify/set/delete) on znode in ZooKeeper
 * which keeps hbase:root region server location.
 *
 * Stateless class with a bunch of static methods. Doesn't manage resources passed in
 * (e.g. HConnection, ZooKeeperWatcher etc).
 *
 * Root region location is set by <code>RegionServerServices</code>.
 * This class doesn't use ZK watchers, rather accesses ZK directly.
 *
 * This class it stateless. The only reason it's not made a non-instantiable util class
 * with a collection of static methods is that it'd be rather hard to mock properly in tests.
 *
 * TODO: rewrite using RPC calls to master to find out about hbase:root.
 */
@InterfaceAudience.Private
public class RootTableLocator {
  private static final Log LOG = LogFactory.getLog(RootTableLocator.class);

  // only needed to allow non-timeout infinite waits to stop when cluster shuts down
  private volatile boolean stopped = false;

  /**
   * Checks if the root region location is available.
   * @return true if root region location is available, false if not
   */
  public boolean isLocationAvailable(ZooKeeperWatcher zkw) {
    return getRootRegionLocation(zkw) != null;
  }

  /**
   * @param zkw ZooKeeper watcher to be used
   * @return root table regions and their locations.
   */
  public List<Pair<HRegionInfo, ServerName>> getRootRegionsAndLocations(ZooKeeperWatcher zkw) {
    return getRootRegionsAndLocations(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * 
   * @param zkw
   * @param replicaId
   * @return root table regions and their locations.
   */
  public List<Pair<HRegionInfo, ServerName>> getRootRegionsAndLocations(ZooKeeperWatcher zkw,
                                                                        int replicaId) {
    ServerName serverName = getRootRegionLocation(zkw, replicaId);
    List<Pair<HRegionInfo, ServerName>> list = new ArrayList<Pair<HRegionInfo, ServerName>>();
    list.add(new Pair<HRegionInfo, ServerName>(RegionReplicaUtil.getRegionInfoForReplica(
        HRegionInfo.ROOT_REGIONINFO, replicaId), serverName));
    return list;
  }

  /**
   * @param zkw ZooKeeper watcher to be used
   * @return List of root regions
   */
  public List<HRegionInfo> getRootRegions(ZooKeeperWatcher zkw) {
    return getRootRegions(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * 
   * @param zkw
   * @param replicaId
   * @return List of root regions
   */
  public List<HRegionInfo> getRootRegions(ZooKeeperWatcher zkw, int replicaId) {
    List<Pair<HRegionInfo, ServerName>> result;
    result = getRootRegionsAndLocations(zkw, replicaId);
    return getListOfHRegionInfos(result);
  }

  private List<HRegionInfo> getListOfHRegionInfos(
      final List<Pair<HRegionInfo, ServerName>> pairs) {
    if (pairs == null || pairs.isEmpty()) return null;
    List<HRegionInfo> result = new ArrayList<HRegionInfo>(pairs.size());
    for (Pair<HRegionInfo, ServerName> pair: pairs) {
      result.add(pair.getFirst());
    }
    return result;
  }

  /**
   * Gets the root region location, if available.  Does not block.
   * @param zkw zookeeper connection to use
   * @return server name or null if we failed to get the data.
   */
  public ServerName getRootRegionLocation(final ZooKeeperWatcher zkw) {
    try {
      RegionState state = getRootRegionState(zkw);
      return state.isOpened() ? state.getServerName() : null;
    } catch (KeeperException ke) {
      return null;
    }
  }

  /**
   * Gets the root region location, if available.  Does not block.
   * @param zkw
   * @param replicaId
   * @return server name
   */
  public ServerName getRootRegionLocation(final ZooKeeperWatcher zkw, int replicaId) {
    try {
      RegionState state = getRootRegionState(zkw, replicaId);
      return state.isOpened() ? state.getServerName() : null;
    } catch (KeeperException ke) {
      return null;
    }
  }

  /**
   * Gets the root region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param zkw
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting root region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException
   */
  public ServerName waitRootRegionLocation(ZooKeeperWatcher zkw, long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    return waitRootRegionLocation(zkw, HRegionInfo.DEFAULT_REPLICA_ID, timeout);
  }

  /**
   * Gets the root region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param zkw
   * @param replicaId
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting root region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException
   */
  public ServerName waitRootRegionLocation(ZooKeeperWatcher zkw, int replicaId, long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    try {
      if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
        String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. "
            + "There could be a mismatch with the one configured in the master.";
        LOG.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("KeeperException while trying to check baseZNode:", e);
    }
    Pair<HRegionInfo, ServerName> p = blockUntilAvailable(zkw, replicaId, timeout);

    if (p == null) {
      throw new NotAllMetaRegionsOnlineException("Timed out; " + timeout + "ms");
    }

    return p.getSecond();
  }

  /**
   * Waits indefinitely for availability of <code>hbase:root</code>.  Used during
   * cluster startup.  Does not verify root, just that something has been
   * set up in zk.
   * @see #waitRootRegionLocation(org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher, long)
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitRootRegionLocation(ZooKeeperWatcher zkw) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while (!stopped) {
      try {
        if (waitRootRegionLocation(zkw, 100) != null) break;
        long sleepTime = System.currentTimeMillis() - startTime;
        // +1 in case sleepTime=0
        if ((sleepTime + 1) % 10000 == 0) {
          LOG.warn("Have been waiting for root to be assigned for " + sleepTime + "ms");
        }
      } catch (NotAllMetaRegionsOnlineException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("hbase:root still not available, sleeping and retrying." +
            " Reason: " + e.getMessage());
        }
      }
    }
  }

  /**
   * Verify <code>hbase:root</code> is deployed and accessible.
   * @param hConnection
   * @param zkw
   * @param timeout How long to wait on zk for root address (passed through to
   * the internal call to {@link #getRootServerConnection}.
   * @return True if the <code>hbase:root</code> location is healthy.
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public boolean verifyRootRegionLocation(HConnection hConnection,
                                          ZooKeeperWatcher zkw, final long timeout)
  throws InterruptedException, IOException {
    return verifyRootRegionLocation(hConnection, zkw, timeout, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Verify <code>hbase:root</code> is deployed and accessible.
   * @param hConnection
   * @param zkw
   * @param timeout How long to wait on zk for root address (passed through to
   * @param replicaId
   * @return True if the <code>hbase:root</code> location is healthy.
   * @throws InterruptedException
   * @throws IOException
   */
  public boolean verifyRootRegionLocation(HConnection hConnection,
                                          ZooKeeperWatcher zkw, final long timeout, int replicaId)
  throws InterruptedException, IOException {
    AdminProtos.AdminService.BlockingInterface service = null;
    try {
      service = getRootServerConnection(hConnection, zkw, timeout, replicaId);
    } catch (NotAllMetaRegionsOnlineException e) {
      // Pass
    } catch (ServerNotRunningYetException e) {
      // Pass -- remote server is not up so can't be carrying root
    } catch (UnknownHostException e) {
      // Pass -- server name doesn't resolve so it can't be assigned anything.
    } catch (RegionServerStoppedException e) {
      // Pass -- server name sends us to a server that is dying or already dead.
    }
    return (service != null) && verifyRegionLocation(hConnection, service,
            getRootRegionLocation(zkw, replicaId), RegionReplicaUtil.getRegionInfoForReplica(
                HRegionInfo.ROOT_REGIONINFO, replicaId).getRegionName());
  }

  /**
   * Verify we can connect to <code>hostingServer</code> and that its carrying
   * <code>regionName</code>.
   * @param hostingServer Interface to the server hosting <code>regionName</code>
   * @param address The servername that goes with the <code>rootServer</code>
   * Interface.  Used logging.
   * @param regionName The regionname we are interested in.
   * @return True if we were able to verify the region located at other side of
   * the Interface.
   * @throws IOException
   */
  // TODO: We should be able to get the ServerName from the AdminProtocol
  // rather than have to pass it in.  Its made awkward by the fact that the
  // HRI is likely a proxy against remote server so the getServerName needs
  // to be fixed to go to a local method or to a cache before we can do this.
  private boolean verifyRegionLocation(final Connection connection,
      AdminService.BlockingInterface hostingServer, final ServerName address,
      final byte [] regionName)
  throws IOException {
    if (hostingServer == null) {
      LOG.info("Passed hostingServer is null");
      return false;
    }
    Throwable t;
    PayloadCarryingRpcController controller = null;
    if (connection instanceof ClusterConnection) {
      controller = ((ClusterConnection) connection).getRpcControllerFactory().newController();
    }
    try {
      // Try and get regioninfo from the hosting server.
      return ProtobufUtil.getRegionInfo(controller, hostingServer, regionName) != null;
    } catch (ConnectException e) {
      t = e;
    } catch (RetriesExhaustedException e) {
      t = e;
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      t = ioe;
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof EOFException) {
        t = cause;
      } else if (cause != null && cause.getMessage() != null
          && cause.getMessage().contains("Connection reset")) {
        t = cause;
      } else {
        t = e;
      }
    }
    LOG.info("Failed verification of " + Bytes.toStringBinary(regionName) +
      " at address=" + address + ", exception=" + t.getMessage());
    return false;
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * <p>WARNING: Does not retry.  Use an {@link org.apache.hadoop.hbase.client.HTable} instead.
   * @param hConnection
   * @param zkw
   * @param timeout How long to wait on root location
   * @param replicaId
   * @return connection to server hosting root
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  private AdminService.BlockingInterface getRootServerConnection(HConnection hConnection,
                                                                 ZooKeeperWatcher zkw, long timeout, int replicaId)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(hConnection, waitRootRegionLocation(zkw, replicaId, timeout));
  }

  /**
   * @param sn ServerName to get a connection against.
   * @return The AdminProtocol we got when we connected to <code>sn</code>
   * May have come from cache, may not be good, may have been setup by this
   * invocation, or may be null.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private static AdminService.BlockingInterface getCachedConnection(HConnection hConnection,
    ServerName sn)
  throws IOException {
    if (sn == null) {
      return null;
    }
    AdminService.BlockingInterface service = null;
    try {
      service = hConnection.getAdmin(sn);
    } catch (RetriesExhaustedException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        // Catch this; presume it means the cached connection has gone bad.
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      LOG.debug("Timed out connecting to " + sn);
    } catch (NoRouteToHostException e) {
      LOG.debug("Connecting to " + sn, e);
    } catch (SocketException e) {
      LOG.debug("Exception connecting to " + sn);
    } catch (UnknownHostException e) {
      LOG.debug("Unknown host exception connecting to  " + sn);
    } catch (FailedServerException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server " + sn + " is in failed server list.");
      }
    } catch (IOException ioe) {
      Throwable cause = ioe.getCause();
      if (ioe instanceof ConnectException) {
        // Catch. Connect refused.
      } else if (cause != null && cause instanceof EOFException) {
        // Catch. Other end disconnected us.
      } else if (cause != null && cause.getMessage() != null &&
        cause.getMessage().toLowerCase().contains("connection reset")) {
        // Catch. Connection reset.
      } else {
        throw ioe;
      }

    }
    return service;
  }

  /**
   * Sets the location of <code>hbase:root</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param serverName The server hosting <code>hbase:root</code>
   * @param state The region transition state
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setRootLocation(ZooKeeperWatcher zookeeper,
                                     ServerName serverName,
                                     RegionState.State state) throws KeeperException {
    setRootLocation(zookeeper, serverName, HRegionInfo.DEFAULT_REPLICA_ID, state);
  }

  /**
   * Sets the location of <code>hbase:root</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper
   * @param serverName
   * @param replicaId
   * @param state
   * @throws KeeperException
   */
  public static void setRootLocation(ZooKeeperWatcher zookeeper,
                                     ServerName serverName,
                                     int replicaId,
                                     RegionState.State state) throws KeeperException {
    LOG.info("Setting hbase:root region location in ZooKeeper as " + serverName);
    // Make the RootRegionServer pb and then get its bytes and save this as
    // the znode content.
    MetaRegionServer pbrsr = MetaRegionServer.newBuilder()
      .setServer(ProtobufUtil.toServerName(serverName))
      .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
      .setIsRoot(true)
      .setState(state.convert()).build();
    byte[] data = ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
    try {
      ZKUtil.setData(zookeeper, zookeeper.getZNodeForReplica(replicaId), data);
    } catch(KeeperException.NoNodeException nne) {
      if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
        LOG.debug("ROOT region location doesn't exist, create it");
      } else {
        LOG.debug("ROOT region location doesn't exist for replicaId " + replicaId +
            ", create it");
      }
      ZKUtil.createAndWatch(zookeeper, zookeeper.getZNodeForReplica(replicaId), data);
    }
  }

  /**
   * Load the root region state from the root server ZNode.
   */
  public static RegionState getRootRegionState(ZooKeeperWatcher zkw) throws KeeperException {
    return getRootRegionState(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Load the root region state from the root server ZNode.
   * @param zkw
   * @param replicaId
   * @return regionstate
   * @throws KeeperException
   */
  public static RegionState getRootRegionState(ZooKeeperWatcher zkw, int replicaId)
      throws KeeperException {
    RegionState.State state = RegionState.State.OPEN;
    ServerName serverName = null;
    boolean isRoot = false;
    try {
      byte[] data = ZKUtil.getData(zkw, zkw.getZNodeForReplica(replicaId));
      if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
        try {
          int prefixLen = ProtobufUtil.lengthOfPBMagic();
          ZooKeeperProtos.MetaRegionServer rl =
            ZooKeeperProtos.MetaRegionServer.PARSER.parseFrom
              (data, prefixLen, data.length - prefixLen);
          if (rl.hasState()) {
            state = RegionState.State.convert(rl.getState());
          }
          HBaseProtos.ServerName sn = rl.getServer();
          serverName = ServerName.valueOf(
            sn.getHostName(), sn.getPort(), sn.getStartCode());
          if (rl.hasIsRoot()) {
            isRoot = rl.getIsRoot();
          }
        } catch (InvalidProtocolBufferException e) {
          throw new DeserializationException("Unable to parse root region location");
        }
      } else {
        if (data != null) {
          throw new DeserializationException(
              "meta-region-server znode seems corrupted, not PB data and not empty either.");
        }
        //If znode does not exist or failed to read data assume it's root
        isRoot = true;
      }
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (serverName == null) {
      state = RegionState.State.OFFLINE;
    }
    HRegionInfo regionInfo = isRoot ? HRegionInfo.ROOT_REGIONINFO : HRegionInfo.FIRST_META_REGIONINFO;
    return new RegionState(
        RegionReplicaUtil.getRegionInfoForReplica(regionInfo, replicaId),
      state, serverName);
  }

  /**
   * Deletes the location of <code>hbase:root</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  public void deleteRootLocation(ZooKeeperWatcher zookeeper)
  throws KeeperException {
    deleteRootLocation(zookeeper, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  public void deleteRootLocation(ZooKeeperWatcher zookeeper, int replicaId)
  throws KeeperException {
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
      LOG.info("Deleting hbase:root region location in ZooKeeper");
    } else {
      LOG.info("Deleting hbase:root for " + replicaId + " region location in ZooKeeper");
    }
    try {
      // Just delete the node.  Don't need any watches.
      ZKUtil.deleteNode(zookeeper, zookeeper.getZNodeForReplica(replicaId));
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }
  /**
   * Wait until the primary root region is available. Get the secondary
   * locations as well but don't block for those.
   * @param zkw
   * @param timeout
   * @param conf
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public List<Pair<HRegionInfo, ServerName>> blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout, Configuration conf)
          throws InterruptedException {
    int numReplicasConfigured = 1;

    List<Pair<HRegionInfo,ServerName>> result = new ArrayList<>();
    // Make the blocking call first so that we do the wait to know
    // the znodes are all in place or timeout.
    Pair<HRegionInfo, ServerName> pair = blockUntilAvailable(zkw, timeout);
    if (pair == null) return null;
    result.add(pair);
    HRegionInfo defaultRegionInfo = pair.getFirst();

    //TODO francis deal with replicas as well
    try {
      List<String> rootReplicaNodes = zkw.getRootReplicaNodes();
      numReplicasConfigured = rootReplicaNodes.size();
    } catch (KeeperException e) {
      LOG.warn("Got ZK exception " + e);
    }
    for (int replicaId = 1; replicaId < numReplicasConfigured; replicaId++) {
      // return all replica locations for the root
      ServerName sn = getRootRegionLocation(zkw, replicaId);
      result.add(
          new Pair<HRegionInfo, ServerName>(
              RegionReplicaUtil.getRegionInfoForReplica(defaultRegionInfo, replicaId),
              sn));
    }
    return result;
  }

  /**
   * Wait until the root region is available and is not in transition.
   * @param zkw zookeeper connection to use
   * @param timeout maximum time to wait, in millis
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public Pair<HRegionInfo, ServerName> blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout)
  throws InterruptedException {
    return blockUntilAvailable(zkw, HRegionInfo.DEFAULT_REPLICA_ID, timeout);
  }

  /**
   * Wait until the root region is available and is not in transition.
   * @param zkw
   * @param replicaId
   * @param timeout
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public Pair<HRegionInfo, ServerName> blockUntilAvailable(final ZooKeeperWatcher zkw, int replicaId,
      final long timeout)
  throws InterruptedException {
    if (timeout < 0) throw new IllegalArgumentException();
    if (zkw == null) throw new IllegalArgumentException();
    long startTime = System.currentTimeMillis();
    RegionState state = null;
    while (true) {
      try {
        state = getRootRegionState(zkw, replicaId);
      } catch (KeeperException ke) {
        LOG.debug(
            "Blocking until available, got exception while trying to retrieve root state",
            ke);
      }
      if ((state != null && state.isOpened()) || (System.currentTimeMillis() - startTime)
          > timeout - HConstants.SOCKET_RETRY_WAIT_MS) {
        break;
      }
      Thread.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
    }
    if (state == null || !state.isOpened()) {
      return null;
    }
    return new Pair<>(state.getRegion(), state.getServerName());
  }

  /**
   * Stop working.
   * Interrupts any ongoing waits.
   */
  public void stop() {
    if (!stopped) {
      LOG.debug("Stopping RootTableLocator");
      stopped = true;
    }
  }
}
