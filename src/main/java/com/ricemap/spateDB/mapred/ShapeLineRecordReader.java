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
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.ricemap.spateDB.core.CellInfo;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;

/**
 * A record reader for objects of class {@link Shape}
 * @author tonyren, Ahmed Eldawy
 *
 */
public class ShapeLineRecordReader
    extends SpatialRecordReader<Prism, Text> {

  public ShapeLineRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
  }

  public ShapeLineRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
  }
  
  public ShapeLineRecordReader(InputStream in, long offset, long endOffset)
      throws IOException {
    super(in, offset, endOffset);
  }

  @Override
  public boolean next(Prism key, Text shapeLine) throws IOException {
    boolean read_line = nextLine(shapeLine);
    key.set(cellMbr);
    return read_line;
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public Text createValue() {
    return new Text();
  }
}
