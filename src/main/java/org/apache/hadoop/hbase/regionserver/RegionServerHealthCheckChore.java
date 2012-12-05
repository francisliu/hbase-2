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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.HealthChecker.HealthCheckerExitStatus;
import org.apache.hadoop.util.StringUtils;

/**
 * The Class RegionServerHealthCheckChore for checking the health of
 * the region server.
 */
 class RegionServerHealthCheckChore extends Chore {
  private static Log LOG = LogFactory.getLog(RegionServerHealthCheckChore.class);
  HealthChecker rsHealthChecker;
  Configuration config;
  int threshold;
  int numTimesUnhealthy = 0;
  long failureWindow;
  long startWindow;

  RegionServerHealthCheckChore(int sleepTime, Stoppable stopper, Configuration config) {
    super("RegionHealthChecker", sleepTime, stopper);
    LOG.info("Health Check Chore runs every " + StringUtils.formatTime(sleepTime));
    this.config = config;
    String healthCheckScript = this.config.get(HConstants.RS_HEALTH_SCRIPT_LOC);
    long scriptTimeout = this.config.getLong(HConstants.RS_HEALTH_SCRIPT_TIMEOUT,
      HConstants.DEFAULT_RS_HEALTH_SCRIPT_TIMEOUT);
    rsHealthChecker = new HealthChecker();
    rsHealthChecker.init(healthCheckScript, scriptTimeout);    
    this.threshold = config.getInt(HConstants.RS_HEALTH_FAILURE_THRESHOLD,
      HConstants.DEFAULT_RS_HEALTH_FAILURE_THRESHOLD);
    this.failureWindow = this.threshold * sleepTime;
  }

  @Override
  protected void chore() {
    if (rsHealthChecker.shouldRun()) {
      HealthReport report = rsHealthChecker.checkHealth();
      boolean isHealthy = (report.getStatus() == HealthCheckerExitStatus.SUCCESS);
      if (!isHealthy) {
        boolean needToStop = decideToStop();
        if (needToStop) {
          this.stopper.stop("The region server reported unhealthy " + threshold
              + "number of times in " + failureWindow + "time.");
        }
        // Always log health report.
        LOG.info("Health status at " + StringUtils.formatTime(System.currentTimeMillis()) + ": "
            + report.getHealthReport());
      }
    } else {
      LOG.info("Health checker did not run.");
    }
  }

  private boolean decideToStop() {
    boolean stop = false;
    if (numTimesUnhealthy == 0) {
      // First time we are seeing a failure. No need to stop, just
      // record the time.
      numTimesUnhealthy++;
      stop = false;
      startWindow = System.currentTimeMillis();
    } else {
      if ((System.currentTimeMillis() - startWindow) < failureWindow) {
        numTimesUnhealthy++;
        if (numTimesUnhealthy == threshold) {
          stop = true;
        } else {
          stop = false;
        }
      } else {
        // Outside of failure window, so we reset to 1.
        numTimesUnhealthy = 1;
        startWindow = System.currentTimeMillis();
        stop = false;
      }
    }
    return stop;
  }

}
