/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.coprocessor;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents the return value from a
 * {@link MasterExec} invocation.
 * This simply wraps the value for easier
 * {@link org.apache.hadoop.hbase.io.HbaseObjectWritable}
 * serialization.
 *
 * <p>
 * This class is used internally by the HBaseAdmin client code to properly serialize
 * responses from {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol}
 * method invocations.  It should not be used directly by clients.
 * </p>
 *
 * @see org.apache.hadoop.hbase.client.coprocessor.MasterExec
 * @see org.apache.hadoop.hbase.client.HBaseAdmin#coprocessorProxy(Class)
 */
public class MasterExecResult implements Writable {
  private Object value;

  public MasterExecResult() {
  }

  public MasterExecResult(Object value) {
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HbaseObjectWritable.writeObject(out, value,
        value != null ? value.getClass() : Writable.class, null);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = HbaseObjectWritable.readObject(in, null);
  }
}
