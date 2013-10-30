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
package com.ricemap.spateDB.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.ricemap.spateDB.core.CellInfo;
import com.ricemap.spateDB.mapred.TextOutputFormat;
import com.ricemap.spateDB.util.CommandLineArguments;


/**
 * Calculates number of records in a file depending on its type. If the file
 * is a text file, it counts number of lines. If it's a grid file with no local
 * index, it counts number of non-empty lines. If it's a grid file with RTree
 * index, it counts total number of records stored in all RTrees.
 * @author tonyren, eldawy
 *
 */
public class LineRandomizer {

  private static final String NumOfPartitions =
      "com.ricemap.spateDB.operations.LineRandomizer";
  
  public static class Map extends MapReduceBase implements
      Mapper<CellInfo, Text, IntWritable, Text> {
    /**Total number of partitions to generate*/
    private int totalNumberOfPartitions;
    
    /**Temporary key used to generate intermediate records*/
    private IntWritable tempKey = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      totalNumberOfPartitions = job.getInt(NumOfPartitions, 1);
    }
    
    public void map(CellInfo cell, Text line,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      tempKey.set((int) (Math.random() * totalNumberOfPartitions));
      output.collect(tempKey, line);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable, Text, NullWritable, Text> {

    private NullWritable dummy = NullWritable.get();
    
    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      // Retrieve all lines in this reduce group
      Vector<Text> all_lines = new Vector<Text>();
      while (values.hasNext()) {
        Text t = values.next();
        all_lines.add(new Text(t));
      }
      
      // Randomize lines within this group
      Collections.shuffle(all_lines);
      
      // Output lines in the randomized order
      for (Text line : all_lines) {
        output.collect(dummy, line);
      }
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param conf
   * @param infs
   * @param infile
   * @return
   * @throws IOException 
   */
  public static void randomizerMapReduce(Path infile, Path outfile,
      boolean overwrite) throws IOException {
    JobConf job = new JobConf(LineRandomizer.class);
    
    FileSystem outfs = outfile.getFileSystem(job);

    if (overwrite)
      outfs.delete(outfile, true);
    
    job.setJobName("Randomizer");
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setMapperClass(Map.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);

    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

    FileSystem infs = infile.getFileSystem(job);
    int numOfPartitions = (int) Math.ceil((double)
        infs.getFileStatus(infile).getLen() / infs.getDefaultBlockSize(outfile));
    job.setInt(NumOfPartitions, numOfPartitions);
    
    job.setInputFormat(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, infile);
    
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outfile);
    
    // Submit the job
    JobClient.runJob(job);
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path inputFile = cla.getPaths()[0];
    Path outputFile = cla.getPaths()[1];
    boolean overwrite = cla.isOverwrite();
    randomizerMapReduce(inputFile, outputFile, overwrite);
  }

}
