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

/**
 * The Interface HealthChecker for providing methods for regularly checking
 * health of region servers.
 */
public interface HealthChecker {
  
  /**
   * Initialize.
   *
   * @param configuration
   */
  public void init(Configuration config);
  
  /**
   * Check health of the server.
   *
   * @return HealthCheckerExitStatus - The status of the server. 
   */
  public HealthCheckerExitStatus checkHealth();
  
  /**
   * Gets the health report of the region server.
   *
   * @return the health report
   */
  public String getHealthReport();
  
  enum HealthCheckerExitStatus {
    SUCCESS,
    TIMED_OUT,
    FAILED_WITH_EXIT_CODE,
    FAILED_WITH_EXCEPTION,
    FAILED
  }
}
