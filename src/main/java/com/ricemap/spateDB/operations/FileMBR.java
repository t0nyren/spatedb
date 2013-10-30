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
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.LineReader;

import com.ricemap.spateDB.core.GlobalIndex;
import com.ricemap.spateDB.core.Partition;
import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.mapred.ShapeInputFormat;
import com.ricemap.spateDB.mapred.ShapeRecordReader;
import com.ricemap.spateDB.mapred.SpatialInputFormat;
import com.ricemap.spateDB.mapred.TextOutputFormat;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;
import com.ricemap.spateDB.util.CommandLineArguments;

/**
 * Finds the minimal bounding Prism for a file.
 * @author tonyren, Ahmed Eldawy
 *
 */
public class FileMBR {
  private static final NullWritable Dummy = NullWritable.get();
  
  /**
   * Keeps track of the size of last processed file. Used to determine the
   * uncompressed size of a file.
   */
  public static long sizeOfLastProcessedFile;

  /**Last submitted MBR MapReduce job*/
  public static RunningJob lastSubmittedJob;

  public static class Map extends MapReduceBase implements
      Mapper<Prism, Shape, NullWritable, Prism> {
    
    private final Prism MBR = new Prism();
    
    public void map(Prism dummy, Shape shape,
        OutputCollector<NullWritable, Prism> output, Reporter reporter)
            throws IOException {
      Prism mbr = shape.getMBR();

      // Skip writing Prism to output if totally contained in mbr_so_far
      if (mbr != null) {
        MBR.set(mbr.t1, mbr.x1, mbr.y1, mbr.t2, mbr.x2, mbr.y2);
        output.collect(Dummy, MBR);
      }
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<NullWritable, Prism, NullWritable, Prism> {
    @Override
    public void reduce(NullWritable dummy, Iterator<Prism> values,
        OutputCollector<NullWritable, Prism> output, Reporter reporter)
            throws IOException {
      Prism mbr = new Prism(Double.MAX_VALUE, Double.MAX_VALUE,Double.MAX_VALUE,-Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
      while (values.hasNext()) {
        Prism rect = values.next();
        mbr.expand(rect);
      }
      output.collect(dummy, mbr);
    }
  }
  
  public static class MBROutputCommitter extends FileOutputCommitter {
    // If input is a directory, save the MBR to a file there
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      // Store the result back in the input file if it is a directory
      JobConf job = context.getJobConf();
      
      // Read job result
      Path outPath = TextOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);
      FileStatus[] results = outFs.listStatus(outPath);
      Prism mbr = new Prism();
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          if (lineReader.readLine(text) > 0) {
            mbr.fromText(text);
          }
          lineReader.close();
        }
      }

      // Store the result back to disk
      Path[] inPaths = SpatialInputFormat.getInputPaths(job);
      for (Path inPath : inPaths) {
        FileSystem fs = inPath.getFileSystem(job);
        if (fs.getFileStatus(inPath).isDir()) {
          // Results can be stored back only if input is a directory
          FileStatus[] datafiles = fs.listStatus(inPath,new PathFilter(){
            public boolean accept(Path p){
              String name = p.getName(); 
              return !name.startsWith("_") && !name.startsWith("."); 
            }
          });
          Path gindex_path = new Path(inPath, "_master.grid");
          PrintStream gout = new PrintStream(fs.create(gindex_path, false));
          for (FileStatus datafile : datafiles) {
            gout.print(mbr.toText(new Text()));
            gout.print(",");
            gout.print(datafile.getPath().getName());
            gout.println();
          }
          gout.close();
        }
      }
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param conf
   * @param fs
   * @param file
   * @return
   * @throws IOException 
   */
  public static <S extends Shape> Prism fileMBRMapReduce(FileSystem fs,
      Path file, S stockShape, boolean background) throws IOException {
    // Quickly get file MBR if it is globally indexed
    GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(fs, file);
    if (globalIndex != null) {
      // Return the MBR of the global index.
      // Compute file size by adding up sizes of all files assuming they are
      // not compressed
      long totalLength = 0;
      for (Partition p : globalIndex) {
        Path filePath = new Path(file, p.filename);
        if (fs.exists(filePath))
          totalLength += fs.getFileStatus(filePath).getLen();
      }
      sizeOfLastProcessedFile = totalLength;
      return globalIndex.getMBR();
    }
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path(file.toUri().getPath()+".mbr_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("FileMBR");
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Prism.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setInputFormat(ShapeInputFormat.class);
    SpatialSite.setShapeClass(job, stockShape.getClass());
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setOutputCommitter(MBROutputCommitter.class);
    
    // Submit the job
    if (background) {
      JobClient jc = new JobClient(job);
      lastSubmittedJob = jc.submitJob(job);
      return null;
    } else {
      lastSubmittedJob = JobClient.runJob(job);
      Counters counters = lastSubmittedJob.getCounters();
      Counter inputBytesCounter = counters.findCounter(Task.Counter.MAP_INPUT_BYTES);
      FileMBR.sizeOfLastProcessedFile = inputBytesCounter.getValue();
      
      // Read job result
      FileStatus[] results = outFs.listStatus(outputPath);
      Prism mbr = new Prism();
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          if (lineReader.readLine(text) > 0) {
            mbr.fromText(text);
          }
          lineReader.close();
        }
      }
      
      outFs.delete(outputPath, true);
      
      return mbr;
    }
  }
  
  /**
   * Counts the exact number of lines in a file by opening the file and
   * reading it line by line
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static <S extends Shape> Prism fileMBRLocal(FileSystem fs,
      Path file, S shape) throws IOException {
    // Try to get file MBR from the global index (if possible)
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(fs, file);
    if (gindex != null) {
      return gindex.getMBR();
    }
    long file_size = fs.getFileStatus(file).getLen();
    sizeOfLastProcessedFile = file_size;
    
    ShapeRecordReader<Shape> shapeReader = new ShapeRecordReader<Shape>(
        new Configuration(), new FileSplit(file, 0, file_size, new String[] {}));

    Prism mbr = new Prism(Double.MAX_VALUE,Double.MAX_VALUE, Double.MAX_VALUE,-Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    
    Prism key = shapeReader.createKey();

    while (shapeReader.next(key, shape)) {
      Prism rect = shape.getMBR();
      mbr.expand(rect);
    }
    return mbr;
  }
  
  public static Prism fileMBR(FileSystem fs, Path inFile, Shape stockShape) throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus.isDir() || inFStatus.getLen() / inFStatus.getBlockSize() > 1) {
      // Either a directory of file or a large file
      return fileMBRMapReduce(fs, inFile, stockShape, false);
    } else {
      // A single small file, process it without MapReduce
      return fileMBRLocal(fs, inFile, stockShape);
    }
  }

  private static void printUsage() {
    System.out.println("Finds the MBR of an input file");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file>: (*) Path to input file");
  }
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(FileMBR.class);
    Path inputFile = cla.getPath();
    if (inputFile == null) {
      printUsage();
      return;
    }
    
    FileSystem fs = inputFile.getFileSystem(conf);
    if (!fs.exists(inputFile)) {
      printUsage();
      return;
    }

    Shape stockShape = cla.getShape(true);
    long t1 = System.currentTimeMillis();
    Prism mbr = fileMBR(fs, inputFile, stockShape);
    long t2 = System.currentTimeMillis();
    System.out.println("Total processing time: "+(t2-t1)+" millis");
    System.out.println("MBR of records in file "+inputFile+" is "+mbr);
  }

}
