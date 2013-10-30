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
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.net.NetworkTopology;

import com.ricemap.spateDB.core.GlobalIndex;
import com.ricemap.spateDB.core.Partition;
import com.ricemap.spateDB.core.ResultCollector2;
import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.shape.Prism;

class IndexedPrism extends Prism {
  int index;

  public IndexedPrism(int index, Prism r) {
    super(r);
    this.index = index;
  }
  
  @Override
  public boolean equals(Object obj) {
    return index == ((IndexedPrism)obj).index;
  }
}

/**
 * An input format that reads a pair of files simultaneously and returns
 * a key for one of them and the value as a pair of values.
 * It generates a CombineFileSplit for each pair of blocks returned by the
 * BlockFilter. 
 * @author tonyren, Ahmed Eldawy
 *
 */
public abstract class BinarySpatialInputFormat<K extends Writable, V extends Writable>
    extends FileInputFormat<PairWritable<K>, PairWritable<V>> {
  
  private static final Log LOG = LogFactory.getLog(BinarySpatialInputFormat.class);
  
  private static final double SPLIT_SLOP = 1.1;   // 10% slop
  
  @SuppressWarnings("unchecked")
  @Override
  public InputSplit[] getSplits(final JobConf job, int numSplits) throws IOException {
    // Get a list of all input files. There should be exactly two files.
    final Path[] inputFiles = getInputPaths(job);
    GlobalIndex<Partition> gIndexes[] = new GlobalIndex[inputFiles.length];
    
    BlockFilter blockFilter = null;
    try {
      Class<? extends BlockFilter> blockFilterClass =
        job.getClass(SpatialSite.FilterClass, null, BlockFilter.class);
      if (blockFilterClass != null) {
        // Get all blocks the user wants to process
        blockFilter = blockFilterClass.newInstance();
        blockFilter.configure(job);
      }
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }

    if (blockFilter != null) {
      // Extract global indexes from input files

      for (int i_file = 0; i_file < inputFiles.length; i_file++) {
        FileSystem fs = inputFiles[i_file].getFileSystem(job);
        gIndexes[i_file] = SpatialSite.getGlobalIndex(fs, inputFiles[i_file]);
      }
    }
    
    final Vector<CombineFileSplit> matchedSplits = new Vector<CombineFileSplit>();
    if (gIndexes[0] == null || gIndexes[1] == null) {
      // Join every possible pair (Cartesian product)
      BlockLocation[][] fileBlockLocations = new BlockLocation[inputFiles.length][];
      for (int i_file = 0; i_file < inputFiles.length; i_file++) {
        FileSystem fs = inputFiles[i_file].getFileSystem(job);
        FileStatus fileStatus = fs.getFileStatus(inputFiles[i_file]);
        fileBlockLocations[i_file] = fs.getFileBlockLocations(fileStatus, 0,
            fileStatus.getLen());
      }
      LOG.info("Doing a Cartesian product of blocks: "+
            fileBlockLocations[0].length+"x"+fileBlockLocations[1].length);
      for (BlockLocation block1 : fileBlockLocations[0]) {
        for (BlockLocation block2 : fileBlockLocations[1]) {
          FileSplit fsplit1 = new FileSplit(inputFiles[0],
              block1.getOffset(), block1.getLength(), block1.getHosts());
          FileSplit fsplit2 = new FileSplit(inputFiles[1],
              block2.getOffset(), block2.getLength(), block2.getHosts());
          CombineFileSplit combinedSplit = (CombineFileSplit) FileSplitUtil
              .combineFileSplits(job, fsplit1, fsplit2);
          matchedSplits.add(combinedSplit);
        }
      }
    } else {
      // Filter block pairs by the BlockFilter
      blockFilter.selectCellPairs(gIndexes[0], gIndexes[1],
        new ResultCollector2<Partition, Partition>() {
          @Override
          public void collect(Partition p1, Partition p2) {
              try {
                List<FileSplit> splits1 = new ArrayList<FileSplit>();
                Path path1 = new Path(inputFiles[0], p1.filename);
                splitFile(job, path1, splits1);
                
                List<FileSplit> splits2 = new ArrayList<FileSplit>();
                Path path2 = new Path(inputFiles[1], p2.filename);
                splitFile(job, path2, splits2);
                
                for (FileSplit split1 : splits1) {
                  for (FileSplit split2 : splits2) {
                    matchedSplits.add((CombineFileSplit) FileSplitUtil
                        .combineFileSplits(job, split1, split2));
                  }
                }
                
              } catch (IOException e) {
                e.printStackTrace();
              }
          }
        }
      );
    }

    LOG.info("Matched "+matchedSplits.size()+" combine splits");

    // Return all matched splits
    return matchedSplits.toArray(new InputSplit[matchedSplits.size()]);
  }

  public void splitFile(JobConf job, Path path, List<FileSplit> splits)
      throws IOException {
    NetworkTopology clusterMap = new NetworkTopology();
    FileSystem fs = path.getFileSystem(job);
    FileStatus file = fs.getFileStatus(path);
    long length = file.getLen();
    BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
    if (length != 0) { 
      long blockSize = file.getBlockSize();
      long splitSize = blockSize;

      long bytesRemaining = length;
      while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
        String[] splitHosts = getSplitHosts(blkLocations, 
            length-bytesRemaining, splitSize, clusterMap);
        splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
            splitHosts));
        bytesRemaining -= splitSize;
      }
      
      if (bytesRemaining != 0) {
        splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
                   blkLocations[blkLocations.length-1].getHosts()));
      }
    } else if (length != 0) {
      String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
      splits.add(new FileSplit(path, 0, length, splitHosts));
    } else { 
      //Create empty hosts array for zero length files
      splits.add(new FileSplit(path, 0, length, new String[0]));
    }
  }

}
