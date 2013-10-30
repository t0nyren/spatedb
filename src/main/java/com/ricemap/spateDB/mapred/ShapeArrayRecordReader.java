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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.ricemap.spateDB.core.CellInfo;
import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;


/**
 * Reads a file as a list of RTrees
 * @author tonyren, Ahmed Eldawy
 *
 */
public class ShapeArrayRecordReader extends SpatialRecordReader<Prism, ArrayWritable> {
  public static final Log LOG = LogFactory.getLog(ShapeArrayRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private Class<? extends Shape> shapeClass;
  
  public ShapeArrayRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    shapeClass = SpatialSite.getShapeClass(conf);
  }
  
  public ShapeArrayRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    shapeClass = SpatialSite.getShapeClass(job);
  }

  public ShapeArrayRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(Prism key, ArrayWritable shapes) throws IOException {
    // Get cellInfo for the current position in file
    boolean element_read = nextShapes(shapes);
    key.set(cellMbr); // Set the cellInfo for the last block read
    return element_read;
  }

  @Override
  public CellInfo createKey() {
    return new CellInfo();
  }

  @Override
  public ArrayWritable createValue() {
    return new ArrayWritable(shapeClass);
  }
  
}
