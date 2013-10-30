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
package com.ricemap.spateDB.core;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;

/**
 * Writes a spatial file where objects are of type S.
 * @author tonyren, Ahmed Eldawy
 *
 * @param <S>
 */
public class GridRecordWriter<S extends Shape> implements ShapeRecordWriter<S> {
  public static final Log LOG = LogFactory.getLog(GridRecordWriter.class);
  /**The spatial boundaries for each cell*/
  protected CellInfo[] cells;
  
  /**Paths of intermediate files*/
  protected Path[] intermediateCellPath;
  
  /**An output stream for each grid cell*/
  protected OutputStream[] intermediateCellStreams;
  
  /**MBR of the records written so far to each cell*/
  protected Prism[] cellsMbr;
  
  /**Job configuration if part of a MapReduce job*/
  protected JobConf jobConf;
  
  /**Path of the output directory if not part of a MapReduce job*/
  protected Path outDir;
  
  /**File system for output path*/
  protected final FileSystem fileSystem;

  /**Temporary text to serialize one object*/
  protected Text text;
  
  /**Block size for grid file written*/
  protected long blockSize;
  
  /**A stock object used for serialization/deserialization*/
  protected S stockObject;
  
  /**An output stream to the master file*/
  protected OutputStream masterFile;
  
  /**A list of threads closing cells in background*/
  protected ArrayList<Thread> closingThreads;
  
  /**New line marker to separate records*/
  protected static byte[] NEW_LINE;
  
  static {
    try {
      NEW_LINE = System.getProperty("line.separator").getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  
  /**A unique prefix to all files written by this writer*/
  protected String prefix;
  
  /**Pack MBR of each cell around its content after it's written to disk*/
  protected boolean pack;
  
  /**Expand MBR of each cell to totally cover all of its contents*/
  private boolean expand;
  private int counter;
  
  /**
   * Creates a new GridRecordWriter that will write all data files to the
   * given directory
   * @param outDir - The directory in which all files will be stored
   * @param job - The MapReduce job associated with this output
   * @param prefix - A unique prefix to be associated with files of this writer
   * @param cells - Cells to partition the file
   * @param pack - After writing each cell, pack its MBR around contents
   * @throws IOException
   */
  public GridRecordWriter(Path outDir, JobConf job, String prefix,
      CellInfo[] cells, boolean pack, boolean expand) throws IOException {
    this.pack = pack;
    this.expand = expand;
    this.prefix = prefix;
    this.fileSystem = outDir == null ? 
      FileOutputFormat.getOutputPath(job).getFileSystem(job):
      outDir.getFileSystem(job != null? job : new Configuration());
    this.outDir = outDir;
    this.jobConf = job;
    
    if (cells != null) {
      // Make sure cellIndex maps to array index. This is necessary for calls that
      // call directly write(int, Text)
      int highest_index = 0;
      
      for (CellInfo cell : cells) {
        if (cell.cellId > highest_index)
          highest_index = (int) cell.cellId;
      }
      
      // Create a master file that contains meta information about partitions
      masterFile = fileSystem.create(getMasterFilePath());
      
      this.cells = new CellInfo[highest_index + 1];
      for (CellInfo cell : cells)
        this.cells[(int) cell.cellId] = cell;
      
      // Prepare arrays that hold cells information
      intermediateCellStreams = new OutputStream[this.cells.length];
      intermediateCellPath = new Path[this.cells.length];
      cellsMbr = new Prism[this.cells.length];
    } else {
      intermediateCellStreams = new OutputStream[1];
      intermediateCellPath = new Path[1];
      cellsMbr = new Prism[1];
    }
    for (int i = 0; i < cellsMbr.length; i++) {
      cellsMbr[i] = new Prism(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
          -Double.MAX_VALUE, -Double.MAX_VALUE);
    }

    this.blockSize = job == null ? fileSystem.getDefaultBlockSize(this.outDir) :
      job.getLong(SpatialSite.LOCAL_INDEX_BLOCK_SIZE,
            fileSystem.getDefaultBlockSize(this.outDir));
    
    closingThreads = new ArrayList<Thread>();
    text = new Text();
  }

  protected Path getMasterFilePath() throws IOException {
    String extension;
    if (pack)
      extension = ".r+tree";
    else if (expand)
      extension = ".rtree";
    else
      extension = ".grid";
    return getFilePath("_master"+extension);
  }
  
  /**
   * Returns a path to a file with the given name in the output directory
   * of the record writer.
   * @param filename
   * @return
   * @throws IOException
   */
  protected Path getFilePath(String filename) throws IOException {
    if (prefix != null)
      filename = prefix + "_" + filename;
    return outDir != null ? new Path(outDir, filename) : 
      FileOutputFormat.getTaskOutputPath(jobConf, filename);
  }

  public void setBlockSize(long _block_size) {
    this.blockSize = _block_size;
  }
  
  public void setStockObject(S stockObject) {
    this.stockObject = stockObject;
  }

  @Override
  public synchronized void write(NullWritable dummy, S shape) throws IOException {
    if (cells == null) {
      // No cells. Write to the only stream open to this file
      writeInternal(0, shape);
    } else {
      // Check which cells should contain the given shape
      Prism mbr = shape.getMBR();
      for (int cellIndex = 0; cellIndex < cells.length; cellIndex++) {
        if (cells[cellIndex] != null && mbr.isIntersected(cells[cellIndex])) {
          writeInternal(cellIndex, shape);
        }
      }
    }
  }

  /**
   * Write the given shape to a specific cell. The shape is not replicated to any other cells.
   * It's just written to the given cell. This is useful when shapes are already assigned
   * and replicated to grid cells another way, e.g. from a map phase that partitions.
   * @param cellInfo
   * @param shape
   * @throws IOException
   */
  @Override
  public synchronized void write(CellInfo cellInfo, S shape) throws IOException {
    for (int i_cell = 0; i_cell < cells.length; i_cell++) {
      if (cellInfo.equals(cells[i_cell]))
        write(i_cell, shape);
    }
  }

  @Override
  public void write(int cellId, S shape) throws IOException {
    writeInternal(cellId, shape);
  }

  /**
   * Write the given shape to the cellIndex indicated.
   * @param cellIndex
   * @param shape
   * @throws IOException
   */
  protected synchronized void writeInternal(int cellIndex, S shape) throws IOException {
    if (cellIndex < 0) {
      // A special marker to close a cell
      closeCell(-cellIndex);
      return;
    }
    cellsMbr[cellIndex].expand(shape.getMBR());
    // Convert shape to text
    text.clear();
    shape.toText(text);
    // Write text representation to the file
    OutputStream cellStream = getIntermediateCellStream(cellIndex);
    cellStream.write(text.getBytes(), 0, text.getLength());
    cellStream.write(NEW_LINE);
  }
  
  /**
   * Returns an output stream in which records are written as they come before
   * they are finally flushed to the cell file.
   * @param cellIndex
   * @return
   * @throws IOException
   */
  protected OutputStream getIntermediateCellStream(int cellIndex) throws IOException {
    if (intermediateCellStreams[cellIndex] == null) {
      // For grid file, we write directly to the final file
      intermediateCellPath[cellIndex] = getFinalCellPath(cellIndex);
      intermediateCellStreams[cellIndex] = createFinalCellStream(intermediateCellPath[cellIndex]);
    }
    return intermediateCellStreams[cellIndex];
  }
  
  /**
   * Creates an output stream that will be used to write the final cell file
   * @param cellFilePath
   * @return
   * @throws IOException 
   */
  protected OutputStream createFinalCellStream(Path cellFilePath)
      throws IOException {
    OutputStream cellStream;
    boolean isCompressed = jobConf != null && FileOutputFormat.getCompressOutput(jobConf);
    
    if (!isCompressed) {
      // Create new file
      cellStream = fileSystem.create(cellFilePath, true,
          fileSystem.getConf().getInt("io.file.buffer.size", 4096),
          fileSystem.getDefaultReplication(outDir), this.blockSize);
    } else {
      Class<? extends CompressionCodec> codecClass =
          FileOutputFormat.getOutputCompressorClass(jobConf, GzipCodec.class);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConf);

      // Open a stream to the output file
      cellStream = fileSystem.create(cellFilePath, true,
          fileSystem.getConf().getInt("io.file.buffer.size", 4096),
          fileSystem.getDefaultReplication(outDir), this.blockSize);

      // Encode the output stream using the codec
      cellStream = new DataOutputStream(codec.createOutputStream(cellStream));
    }

    return cellStream;
  }
  
  /**
   * Closes (or initiates a close command) for the cell with the given index.
   * Once this method returns, it should be safe to reuse the same cell index
   * to write more data in a new file.
   * @param cellIndex
   * @throws IOException
   */
  protected void closeCell(int cellIndex) throws IOException {
    Prism cell = cells[cellIndex];
    if (expand)
      cell = cellsMbr[cellIndex];
    else if (pack)
      cell = cell.getIntersection(cellsMbr[cellIndex]);

    closeCellBackground(intermediateCellPath[cellIndex],
        getFinalCellPath(cellIndex), intermediateCellStreams[cellIndex],
        masterFile, cell);
    cellsMbr[cellIndex] = new Prism(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    intermediateCellPath[cellIndex] = null;
    intermediateCellStreams[cellIndex] = null;
  }
  
  /**
   * Close the given cell freeing all memory reserved by it.
   * Once a cell is closed, we should not write more data to it.
   * @param cellInfo
   * @throws IOException
   */
  protected void closeCellBackground(final Path intermediateCellPath,
      final Path finalCellPath, final OutputStream intermediateCellStream,
      final OutputStream masterFile, final Prism cellMbr) throws IOException {
    
    Thread closingThread = new Thread() {
      @Override
      public void run() {
        try {
          Path finalfinalCellPath = flushAllEntries(intermediateCellPath,
              intermediateCellStream, finalCellPath);
          // Write an entry to the master file

          // Write a line to the master file including file name and cellInfo
          if (masterFile != null) {
            Partition partition = new Partition(finalfinalCellPath.getName(), cellMbr);
            Text line = partition.toText(new Text());
            masterFile.write(line.getBytes(), 0, line.getLength());
            masterFile.write(NEW_LINE);
          }
        } catch (IOException e) {
          throw new RuntimeException("Error closing thread", e);
        }
      }
    };
    
    closingThreads.add(closingThread);
    // Remove previously terminated threads
    while (!closingThreads.isEmpty() &&
        closingThreads.get(0).getState() == Thread.State.TERMINATED) {
      closingThreads.remove(0);
    }
    // Start first thread (if exists)
    if (!closingThreads.isEmpty() && closingThreads.get(0).getState() == Thread.State.NEW)
      closingThreads.get(0).start();
  }
  
  /**
   * Flushes all shapes that were written to one cell to the final file.
   * It returns a path to a (closed) file that contains all entries written.
   * @param cellIndex
   * @return
   * @throws IOException
   */
  protected Path flushAllEntries(Path intermediateCellPath,
      OutputStream intermediateCellStream, Path finalCellPath) throws IOException {
    // For global-only indexed file, the intermediate file is the final file
    intermediateCellStream.close();
    return intermediateCellPath;
  }
  
  /**
   * Close the whole writer. Finalize all cell files and concatenate them
   * into the output file.
   */
  public synchronized void close(Progressable progressable) throws IOException {
    // Close all output files
    for (int cellIndex = 0; cellIndex < intermediateCellStreams.length; cellIndex++) {
      if (intermediateCellStreams[cellIndex] != null) {
        closeCell(cellIndex);
      }
      // Indicate progress. Useful if closing a single cell takes a long time
      if (progressable != null)
        progressable.progress();
    }

    while (!closingThreads.isEmpty()) {
      try {
        Thread t = closingThreads.get(0);
        switch (t.getState()) {
        case NEW: t.start(); break;
        case TERMINATED: closingThreads.remove(0); break;
        default:
          t.join(10000);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    
    if (masterFile != null)
      masterFile.close();
  }

  /**
   * Returns path to a file in which the final cell will be written.
   * @param column
   * @param row
   * @return
   * @throws IOException 
   */
  protected Path getFinalCellPath(int cellIndex) throws IOException {
    Path path = null;
    do {
      String filename = counter == 0 ? String.format("data_%05d", cellIndex)
          : String.format("data_%05d_%d", cellIndex, counter);
      boolean isCompressed = jobConf != null && FileOutputFormat.getCompressOutput(jobConf);
      if (isCompressed) {
        Class<? extends CompressionCodec> codecClass =
            FileOutputFormat.getOutputCompressorClass(jobConf, GzipCodec.class);
        // create the named codec
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConf);
        filename += codec.getDefaultExtension();
      }
      
      path = getFilePath(filename);
      counter++;
    } while (fileSystem.exists(path));
    return path;
  }
}
