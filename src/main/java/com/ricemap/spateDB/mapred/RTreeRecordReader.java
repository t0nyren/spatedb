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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.ricemap.spateDB.core.RTree;
import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;


/**
 * Reads a file as a list of RTrees
 * @author tonyren, Ahmed Eldawy
 *
 */
public class RTreeRecordReader<S extends Shape> extends SpatialRecordReader<Prism, RTree<S>> {
  public static final Log LOG = LogFactory.getLog(RTreeRecordReader.class);
  
  /**Shape used to deserialize shapes from disk*/
  private S stockShape;
  
  public RTreeRecordReader(CombineFileSplit split, Configuration conf,
      Reporter reporter, Integer index) throws IOException {
    super(split, conf, reporter, index);
    stockShape = (S) SpatialSite.createStockShape(conf);
  }
  
  public RTreeRecordReader(Configuration job, FileSplit split)
      throws IOException {
    super(job, split);
    stockShape = (S) SpatialSite.createStockShape(job);
  }

  public RTreeRecordReader(InputStream is, long offset, long endOffset)
      throws IOException {
    super(is, offset, endOffset);
  }

  @Override
  public boolean next(Prism key, RTree<S> rtree) throws IOException {
    boolean read_line = nextRTree(rtree);
    key.set(cellMbr);
    return read_line;
  }

  @Override
  public Prism createKey() {
    return new Prism();
  }

  @Override
  public RTree<S> createValue() {
    RTree<S> rtree = new RTree<S>();
    rtree.setStockObject(stockShape);
    return rtree;
  }
}
