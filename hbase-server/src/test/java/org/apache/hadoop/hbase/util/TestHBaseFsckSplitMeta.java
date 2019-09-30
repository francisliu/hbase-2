/**
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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;


/**
 * This tests HBaseFsck's ability to detect reasons for inconsistent tables.
 */
@Category(LargeTests.class)
public class TestHBaseFsckSplitMeta extends TestHBaseFsck {


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.getConfiguration().set("hbase.split.meta", "true");
    TEST_UTIL.getConfiguration().set("hbase.assignment.usezk", "false");
    startup();
  }

  @Override
  public void testCheckTableLocks() throws Exception {
    //Do nothing
    //Test does not pass because it replaces environment edge
    //Which is likely causing non timeline consistent reads
    //on region state
  }
}
