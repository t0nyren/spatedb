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

import java.awt.Color;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ricemap.spateDB.core.CellInfo;
import com.ricemap.spateDB.core.GlobalIndex;
import com.ricemap.spateDB.core.GridInfo;
import com.ricemap.spateDB.core.GridRecordWriter;
import com.ricemap.spateDB.core.Partition;
import com.ricemap.spateDB.core.RTree;
import com.ricemap.spateDB.core.RTreeGridRecordWriter;
import com.ricemap.spateDB.core.ResultCollector;
import com.ricemap.spateDB.core.ShapeRecordWriter;
import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.mapred.GridOutputFormat;
import com.ricemap.spateDB.mapred.RTreeGridOutputFormat;
import com.ricemap.spateDB.mapred.ShapeInputFormat;
import com.ricemap.spateDB.mapred.ShapeRecordReader;
import com.ricemap.spateDB.shape.Point3d;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;
import com.ricemap.spateDB.util.CommandLineArguments;

/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author tonyren, Ahmed Eldawy
 *
 */
public class Repartition {
  static final Log LOG = LogFactory.getLog(Repartition.class);
  
  /**
   * The map class maps each object to every cell it overlaps with.
   * @author tonyren, Ahmed Eldawy
   *
   */
  public static class RepartitionMap<T extends Shape> extends MapReduceBase
      implements Mapper<Prism, T, IntWritable, T> {
    /**List of cells used by the mapper*/
    private CellInfo[] cellInfos;
    
    /**Used to output intermediate records*/
    private IntWritable cellId = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      try {
        cellInfos = SpatialSite.getCells(job);
        super.configure(job);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    /**
     * Map function
     * @param dummy
     * @param shape
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(Prism cellMbr, T shape,
        OutputCollector<IntWritable, T> output, Reporter reporter)
        throws IOException {
      Prism shape_mbr = shape.getMBR();
      if (shape_mbr == null)
        return;
      // Only send shape to output if its lowest corner lies in the cellMBR
      // This ensures that a replicated shape in an already partitioned file
      // doesn't get send to output from all partitions
      if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.t1, shape_mbr.x1, shape_mbr.y1)) {
        for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
          if (cellInfos[cellIndex].isIntersected(shape)) {
            cellId.set((int) cellInfos[cellIndex].cellId);
            output.collect(cellId, shape);
          }
        }
      }
    }
  }
  
  /**
   * The map class maps each object to the cell with maximum overlap.
   * @author tonyren, Ahmed Eldawy
   *
   */
  public static class RepartitionMapNoReplication<T extends Shape> extends MapReduceBase
      implements Mapper<Prism, T, IntWritable, T> {
    /**List of cells used by the mapper*/
    private CellInfo[] cellInfos;
    
    /**Used to output intermediate records*/
    private IntWritable cellId = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      try {
        cellInfos = SpatialSite.getCells(job);
        super.configure(job);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    /**
     * Map function
     * @param dummy
     * @param shape
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(Prism cellMbr, T shape,
        OutputCollector<IntWritable, T> output, Reporter reporter)
        throws IOException {
      Prism shape_mbr = shape.getMBR();
      if (shape_mbr == null)
        return;
      double maxOverlap = 0.0;
      int bestCell = -1;
      // Only send shape to output if its lowest corner lies in the cellMBR
      // This ensures that a replicated shape in an already partitioned file
      // doesn't get send to output from all partitions
      if (!cellMbr.isValid() || cellMbr.contains(shape_mbr.t1, shape_mbr.x1, shape_mbr.y1)) {
        for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
          Prism overlap = cellInfos[cellIndex].getIntersection(shape_mbr);
          if (overlap != null) {
            double overlapArea = overlap.getWidth() * overlap.getHeight();
            if (overlapArea > maxOverlap) {
              maxOverlap = overlapArea;
              bestCell = cellIndex;
            }
          }
        }
      }
      if (bestCell != -1) {
        cellId.set((int) cellInfos[bestCell].cellId);
        output.collect(cellId, shape);
      } else {
        LOG.warn("Shape: "+shape+" doesn't overlap any partitions");
      }
    }
  }
  
  public static class RepartitionReduce<T extends Shape> extends MapReduceBase
  implements Reducer<IntWritable, T, IntWritable, T> {

    @Override
    public void reduce(IntWritable cellIndex, Iterator<T> shapes,
        OutputCollector<IntWritable, T> output, Reporter reporter)
        throws IOException {
      T shape = null;
      while (shapes.hasNext()) {
        shape = shapes.next();
        output.collect(cellIndex, shape);
      }
      // Close cell
      output.collect(new IntWritable(-cellIndex.get()), shape);
    }
    
  }
  
  /**
   * Calculates number of partitions required to index the given file
   * @param inFs
   * @param inFile
   * @param rtree
   * @return
   * @throws IOException 
   */
  public static int calculateNumberOfPartitions(Configuration conf, long inFileSize,
      FileSystem outFs, Path outFile, long blockSize) throws IOException {
    final float IndexingOverhead =
        conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f);
    long indexedFileSize = (long) (inFileSize * (1 + IndexingOverhead));
    if (blockSize == 0)
      blockSize = outFs.getDefaultBlockSize(outFile);
    return (int)Math.ceil((float)indexedFileSize / blockSize);
  }
	
	/**
   * Repartitions a file that is already in HDFS. It runs a MapReduce job
   * that partitions the file into cells, and writes each cell separately.
   * @param conf
   * @param inFile
   * @param outPath
   * @param gridInfo
	 * @param stockShape 
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException
   */
  public static void repartitionMapReduce(Path inFile, Path outPath,
      Shape stockShape, long blockSize, String sindex,
      boolean overwrite, boolean columnar) throws IOException {
    
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileSystem outFs = outPath.getFileSystem(new Configuration());

    // Calculate number of partitions in output file
    // Copy blocksize from source file if it's globally indexed
    if (blockSize == 0) {
      GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, inFile);
      if (globalIndex != null) {
        blockSize = inFs.getFileStatus(new Path(inFile, globalIndex.iterator().next().filename)).getBlockSize();
      } else {
        blockSize = outFs.getDefaultBlockSize(outPath);
      }
    }
    
    // Calculate the dimensions of each partition based on gindex type
    CellInfo[] cellInfos;
    if (sindex.equals("grid")) {
      Prism input_mbr = FileMBR.fileMBRMapReduce(inFs, inFile, stockShape, false);
      long inFileSize = FileMBR.sizeOfLastProcessedFile;
      int num_partitions = calculateNumberOfPartitions(new Configuration(),
          inFileSize, outFs, outPath, blockSize);

      GridInfo gridInfo = new GridInfo(input_mbr.t1, input_mbr.x1, input_mbr.y1, input_mbr.t2,
          input_mbr.x2, input_mbr.y2);
      gridInfo.calculateCellDimensions(num_partitions);
      cellInfos = gridInfo.getAllCells();
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      // Pack in Prisms using an RTree
      cellInfos = packInPrisms(inFs, inFile, outFs, outPath, blockSize, stockShape);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    repartitionMapReduce(inFile, outPath, stockShape, blockSize, cellInfos,
        sindex, overwrite, columnar);
  }
  
  public static class RepartitionOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      JobConf job = context.getJobConf();
      Path outPath = GridOutputFormat.getOutputPath(job);
      FileSystem outFs = outPath.getFileSystem(job);
      CellInfo[] cellInfos = SpatialSite.getCells(job);

      // Concatenate all master files into one file
      FileStatus[] resultFiles = outFs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.getName().contains("_master");
        }
      });
      String ext = resultFiles[0].getPath().getName()
          .substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
      Path masterPath = new Path(outPath, "_master" + ext);
      OutputStream destOut = outFs.create(masterPath);
      byte[] buffer = new byte[4096];
      for (FileStatus f : resultFiles) {
        InputStream in = outFs.open(f.getPath());
        int bytes_read;
        do {
          bytes_read = in.read(buffer);
          if (bytes_read > 0)
            destOut.write(buffer, 0, bytes_read);
        } while (bytes_read > 0);
        in.close();
        outFs.delete(f.getPath(), false);
      }
      destOut.close();
      
      // Plot an image for the partitions used in file
      //Path imagePath = new Path(outPath, "_partitions.png");
     //int imageSize = (int) (Math.sqrt(cellInfos.length) * 300);
      //Plot.plotLocal(masterPath, imagePath, new Partition(), imageSize, imageSize, Color.BLACK, false, false, false);
    }
  }
  
  /**
   * Repartitions an input file according to the given list of cells.
   * @param inFile
   * @param outPath
   * @param cellInfos
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException
   */
  public static void repartitionMapReduce(Path inFile, Path outPath,
      Shape stockShape, long blockSize, CellInfo[] cellInfos, String sindex,
      boolean overwrite, boolean columnar) throws IOException {
    JobConf job = new JobConf(Repartition.class);
    job.setJobName("Repartition");
    FileSystem outFs = outPath.getFileSystem(job);
    
    // Overwrite output file
    if (outFs.exists(outPath)) {
      if (overwrite)
        outFs.delete(outPath, true);
      else
        throw new RuntimeException("Output file '" + outPath
            + "' already exists and overwrite flag is not set");
    }
    
    // Decide which map function to use depending on the type of global index
    if (sindex.equals("rtree")) {
      // Repartition without replication
      job.setMapperClass(RepartitionMapNoReplication.class);
    } else {
      // Repartition with replication (grid and r+tree)
      job.setMapperClass(RepartitionMap.class);
    }
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(stockShape.getClass());
    ShapeInputFormat.setInputPaths(job, inFile);
    job.setInputFormat(ShapeInputFormat.class);
    boolean pack = sindex.equals("r+tree");
    boolean expand = sindex.equals("rtree");
    job.setBoolean(SpatialSite.PACK_CELLS, pack);
    job.setBoolean(SpatialSite.EXPAND_CELLS, expand);
    job.setStrings(SpatialSite.STORAGE_MODE, columnar?"columnar":"normal");

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));

    // Set default parameters for reading input file
    SpatialSite.setShapeClass(job, stockShape.getClass());
  
    FileOutputFormat.setOutputPath(job,outPath);
    if (sindex.equals("grid")) {
      job.setOutputFormat(GridOutputFormat.class);
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      // For now, the two types of local index are the same
      job.setOutputFormat(RTreeGridOutputFormat.class);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    // Copy block size from source file if it's globally indexed
    FileSystem inFs = inFile.getFileSystem(job);
    
    if (blockSize == 0) {
      GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, inFile);
      if (globalIndex != null) {
        blockSize = inFs.getFileStatus(new Path(inFile, globalIndex.iterator().next().filename)).getBlockSize();
        LOG.info("Automatically setting block size to "+blockSize);
      }
    }

    if (blockSize != 0)
      job.setLong(SpatialSite.LOCAL_INDEX_BLOCK_SIZE, blockSize);
    SpatialSite.setCells(job, cellInfos);
    job.setBoolean(SpatialSite.OVERWRITE, overwrite);

    // Set reduce function
    job.setReducerClass(RepartitionReduce.class);
    job.setNumReduceTasks(Math.max(1, Math.min(cellInfos.length,
        (clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));

    // Set output committer that combines output files together
    job.setOutputCommitter(RepartitionOutputCommitter.class);
    
    JobClient.runJob(job);
  }

  public static <S extends Shape> CellInfo[] packInPrisms(FileSystem inFS,
      Path inFile, FileSystem outFS, Path outFile, long blocksize, S stockShape)
      throws IOException {
    return packInPrisms(inFS, new Path[] { inFile },
        outFS, outFile, blocksize, stockShape);
  }
  
  public static <S extends Shape> CellInfo[] packInPrisms(FileSystem fs,
      Path[] files, FileSystem outFileSystem, Path outFile, long blocksize, S stockShape)
      throws IOException {
    final Vector<Point3d> sample = new Vector<Point3d>();
    
    double sample_ratio =
        outFileSystem.getConf().getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size =
      outFileSystem.getConf().getLong(SpatialSite.SAMPLE_SIZE, 100*1024*1024);
    
    // 24 is the estimated size in bytes needed to store each sample point 
    long sample_count = sample_size / 24;
    
    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point3d> resultCollector = new ResultCollector<Point3d>(){
      @Override
      public void collect(Point3d value) {
        sample.add(value.clone());
      }
    };
    Sampler.sampleWithRatio(fs, files, sample_ratio, sample_count,
        System.currentTimeMillis(), resultCollector, stockShape, new Point3d());
    LOG.info("Finished reading a sample of size: "+sample.size()+" records");
    
    long inFileSize = Sampler.sizeOfLastProcessedFile;

    // Compute an approximate MBR to determine the desired number of rows
    // and columns
    Prism approxMBR = new Prism(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Point3d pt : sample) {
      approxMBR.expand(pt);
    }
    GridInfo gridInfo = new GridInfo(approxMBR.t1, approxMBR.x1, approxMBR.y1, approxMBR.t2, approxMBR.x2, approxMBR.y2);
    gridInfo.calculateCellDimensions(Math.max(1, (int)((inFileSize + blocksize / 2) / blocksize)));
    gridInfo.set(-Double.MAX_VALUE,-Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
    
    Prism[] Prisms = RTree.packInPrisms(gridInfo,
            sample.toArray(new Point3d[sample.size()]));
    CellInfo[] cellsInfo = new CellInfo[Prisms.length];
    for (int i = 0; i < Prisms.length; i++)
      cellsInfo[i] = new CellInfo(i + 1, Prisms[i]);
    
    return cellsInfo;
  }
  
  public static <S extends Shape> void repartitionLocal(Path inFile,
      Path outFile, S stockShape, long blockSize, String sindex,
      boolean overwrite)
          throws IOException {
    
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileSystem outFs = outFile.getFileSystem(new Configuration());
    
    // Calculate number of partitions in output file
    // Copy blocksize from source file if it's globally indexed
    if (blockSize == 0) {
      GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, inFile);
      if (globalIndex != null) {
        blockSize = inFs.getFileStatus(new Path(inFile, globalIndex.iterator().next().filename)).getBlockSize();
      }
    }

    // Calculate the dimensions of each partition based on gindex type
    CellInfo[] cellInfos;
    if (sindex.equals("grid")) {
      Prism input_mbr = FileMBR.fileMBRLocal(inFs, inFile, stockShape);
      long inFileSize = FileMBR.sizeOfLastProcessedFile;
      int num_partitions = calculateNumberOfPartitions(new Configuration(),
          inFileSize, outFs, outFile, blockSize);

      GridInfo gridInfo = new GridInfo(input_mbr.t1, input_mbr.x1, input_mbr.y1, input_mbr.t2,
          input_mbr.x2, input_mbr.y2);
      gridInfo.calculateCellDimensions(num_partitions);
      cellInfos = gridInfo.getAllCells();
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      cellInfos = packInPrisms(inFs, inFile, outFs, outFile, blockSize, stockShape);
    } else {
      throw new RuntimeException("Unsupported spatial index: "+sindex);
    }
    
    repartitionLocal(inFile, outFile, stockShape, blockSize, cellInfos, sindex, overwrite);
  }

  /**
   * Repartitions a file on local machine without MapReduce jobs.
   * @param inFs
   * @param in
   * @param outFs
   * @param out
   * @param cells
   * @param stockShape
   * @param rtree
   * @param overwrite
   * @throws IOException 
   */
  public static <S extends Shape> void repartitionLocal(Path in, Path out,
      S stockShape, long blockSize, CellInfo[] cells, String sindex,
      boolean overwrite) throws IOException {
    FileSystem inFs = in.getFileSystem(new Configuration());
    FileSystem outFs = out.getFileSystem(new Configuration());
    // Overwrite output file
    if (outFs.exists(out)) {
      if (overwrite)
        outFs.delete(out, true);
      else
        throw new RuntimeException("Output file '" + out
            + "' already exists and overwrite flag is not set");
    }
    outFs.mkdirs(out);
    
    ShapeRecordWriter<Shape> writer;
    boolean pack = sindex.equals("r+tree");
    boolean expand = sindex.equals("rtree");
    if (sindex.equals("grid")) {
      writer = new GridRecordWriter<Shape>(out, null, null, cells, pack, expand);
    } else if (sindex.equals("rtree") || sindex.equals("r+tree")) {
      writer = new RTreeGridRecordWriter<Shape>(out, null, null, cells, pack, expand);
      writer.setStockObject(stockShape);
    } else {
      throw new RuntimeException("Unupoorted spatial idnex: "+sindex);
    }
    
    FileStatus inFileStatus = inFs.getFileStatus(in);
    // Copy blocksize from source file if it's globally indexed
    if (blockSize == 0) {
      GlobalIndex<Partition> globalIndex = SpatialSite.getGlobalIndex(inFs, in);
      if (globalIndex != null) {
        blockSize = inFs.getFileStatus(new Path(in, globalIndex.iterator().next().filename)).getBlockSize();
      }
    }
    if (blockSize != 0)
      ((GridRecordWriter<Shape>)writer).setBlockSize(blockSize);
    
    long length = inFileStatus.getLen();
    FSDataInputStream datain = inFs.open(in);
    ShapeRecordReader<S> reader = new ShapeRecordReader<S>(datain, 0, length);
    Prism c = reader.createKey();
    
    NullWritable dummy = NullWritable.get();
    
    while (reader.next(c, stockShape)) {
      writer.write(dummy, stockShape);
    }
    writer.close(null);
  }
  
  private static void printUsage() {
    System.out.println("Builds a spatial index on an input file");
    System.out.println("Parameters (* marks required parameters):");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|Prism|polygon> - (*) Type of shapes stored in input file");
    System.out.println("global:<grid|rtree> - (*) Type of global index");
    System.out.println("local:<grid|rtree> - Type of local index");
    System.out.println("mbr:<x,y,w,h> - MBR of data in input file");
    System.out.println("blocksize:<size> - Size of blocks in output file");
    System.out.println("-overwrite - Overwrite output file without noitce");
  }

  /**
	 * Entry point to the file.
	 * rect:<mbr> mbr of the data in file. Automatically obtained if not set. 
	 * shape:<s> the shape to use. Automatically inferred from input file if not set.
	 * gindex<grid:rtree> Type of global index. If not set, no global index is built.
	 * lindex<grid:rtree> Type of local index. If not set, no local index is built.
	 * cells-of:<filename> Use the cells of the given file for the global index.
	 * blocksize:<size> Size of each block in indexed file in bytes.
	 * -local: If set, the index is built on the local machine.
	 * input filename: Input file in HDFS
	 * output filename: Output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
    CommandLineArguments cla = new CommandLineArguments(args);
    if (cla.getPaths().length < 2 || cla.get("sindex") == null) {
      printUsage();
      throw new RuntimeException("Illegal arguments");
    }
    Path inputPath = cla.getPaths()[0];
    FileSystem fs = inputPath.getFileSystem(new Configuration());
    
    if (!fs.exists(inputPath)) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    
    Path outputPath = cla.getPaths()[1];

    // The spatial index to use
    String sindex = cla.get("sindex");
    boolean columnar = cla.get("columnar").equals("columnar");
    boolean overwrite = cla.isOverwrite();
    boolean local = cla.isLocal();
    long blockSize = cla.getBlockSize();
    Shape stockShape = cla.getShape(true);
    LOG.info("Shape: "+stockShape.getClass());
    CellInfo[] cells = cla.getCells();

    long t1 = System.currentTimeMillis();
    if (cells != null) {
      if (local)
        repartitionLocal(inputPath, outputPath, stockShape,
            blockSize, cells, sindex, overwrite);
      else
        repartitionMapReduce(inputPath, outputPath, stockShape,
            blockSize, cells, sindex, overwrite, columnar);
    } else {
      if (local)
        repartitionLocal(inputPath, outputPath, stockShape,
            blockSize, sindex, overwrite);
      else
        repartitionMapReduce(inputPath, outputPath, stockShape,
            blockSize, sindex, overwrite, columnar);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Total indexing time in millis "+(t2-t1));
	}
}
