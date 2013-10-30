/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.ricemap.spateDB.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**A class that stores a pair of objects where both are writable*/
public class PairWritable<T extends Writable> implements Writable {
  public T first;
  public T second;

  public PairWritable() {}
  
  public PairWritable(T first, T second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

  @Override
  public String toString() {
    return "<"+first.toString()+", "+second.toString()+">";
  }
}