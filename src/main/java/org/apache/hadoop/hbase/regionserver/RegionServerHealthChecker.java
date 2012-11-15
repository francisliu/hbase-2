package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;


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

public class RegionServerHealthChecker implements HealthChecker{
  private static Log LOG = LogFactory.getLog(RegionServerHealthChecker.class);
  
  ShellCommandExecutor shexec = null;
 
  private Configuration conf;

  /** Pattern used for searching in the output of the node health script */
  static private final String ERROR_PATTERN = "ERROR";

  /** Time out error message */
  static final String NODE_HEALTH_SCRIPT_TIMED_OUT_MSG = "Node health script timed out";

  private String healthReport;
  private String healthCheckScript;
  private long scriptTimeout;
  
  @Override
  public void init(Configuration conf) {
    this.conf = conf;
   // Get all the required variables from config and initialize
    ArrayList<String> execScript = new ArrayList<String>();
    execScript.add(healthCheckScript);
    shexec = new ShellCommandExecutor(execScript
      .toArray(new String[execScript.size()]), null, null, scriptTimeout);   
    
    LOG.info("RegionServerHealthChecker initialized.");
  }
  
  @Override
  public String getHealthReport(){
    return healthReport;
  }
  
  @Override
  public HealthCheckerExitStatus checkHealth() {
    HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
    try {
      shexec.execute();
    } catch (ExitCodeException e) {
      // ignore the exit code of the script
      LOG.warn("Caught exception : " + e);
      status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;    
    } catch (IOException e) {
      LOG.warn("Caught exception : " + e);
      if (!shexec.isTimedOut()) {
        status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
      } else {
        status = HealthCheckerExitStatus.TIMED_OUT;
      }
    } finally {
      if (status == HealthCheckerExitStatus.SUCCESS) {
        if (hasErrors(shexec.getOutput())) {
          status = HealthCheckerExitStatus.FAILED;
        }
      }
    }
    return status;
  }    
  

  private boolean hasErrors(String output) {
    String[] splits = output.split("\n");
    for (String split : splits) {
      if (split.startsWith(ERROR_PATTERN)) {
        return true;
      }
    }
    return false;
  }

}
