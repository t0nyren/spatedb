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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.ricemap.spateDB.core.CellInfo;


/**
 * An input format used with spatial data. It filters generated splits before
 * creating record readers.
 * @author tonyren, eldawy
 *
 * @param <S>
 */
public class ShapeLineInputFormat extends SpatialInputFormat<CellInfo, Text> {
  
  @Override
  public RecordReader<CellInfo, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    this.rrClass = (Class<? extends RecordReader<CellInfo, Text>>) (Class) ShapeLineRecordReader.class;
    return super.getRecordReader(split, job, reporter);
  }
}
