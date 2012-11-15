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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.HealthChecker.HealthCheckerExitStatus;

public class RegionServerHealthCheckChore extends Chore {

  RegionServerHealthChecker rsHealthChecker;
  Configuration config;
  int threshold;
  int numTimesUnhealthy = 0;
  long failureWindow;
  long startWindow;

  public RegionServerHealthCheckChore(String name, int p, Stoppable stopper, Configuration config) {
    super(name, p, stopper);
    rsHealthChecker = new RegionServerHealthChecker();
    rsHealthChecker.init(config);
    this.config = config;
    // Get script location, threshold count and failure window.
  }

  @Override
  protected void chore() {
    boolean isHealthy = (rsHealthChecker.checkHealth() == HealthCheckerExitStatus.SUCCESS);
    if(!isHealthy){
      boolean needToStop = decideToStop();
      if (needToStop) {
        this.stopper.stop("The region server reported unhealthy " + threshold
            + "number of times in " + failureWindow + "time.");
      }
    }
  }

  private boolean decideToStop() {
    boolean stop = false;
    if(numTimesUnhealthy == 0){
      //First time we are seeing a failure. No need to stop, just
      // record the time. 
      stop = false;
      startWindow = System.currentTimeMillis();     
    }else{
      if((System.currentTimeMillis() - startWindow) < failureWindow){
        numTimesUnhealthy++;
        if(numTimesUnhealthy == threshold){
          stop = true;
        }else {
          stop = false;
        }
      }else {
        //Outside of failure window, so we reset to 1.
        numTimesUnhealthy = 1;
        stop = false;
      }
    }
    return stop;
  }

}
