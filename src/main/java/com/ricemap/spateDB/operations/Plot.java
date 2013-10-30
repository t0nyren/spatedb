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
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import com.ricemap.spateDB.core.CellInfo;
import com.ricemap.spateDB.core.GlobalIndex;
import com.ricemap.spateDB.core.GridInfo;
import com.ricemap.spateDB.core.Partition;
import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.mapred.ShapeInputFormat;
import com.ricemap.spateDB.mapred.ShapeRecordReader;
import com.ricemap.spateDB.mapred.TextOutputFormat;
import com.ricemap.spateDB.shape.Point3d;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;
import com.ricemap.spateDB.util.CommandLineArguments;
import com.ricemap.spateDB.util.ImageOutputFormat;
import com.ricemap.spateDB.util.ImageWritable;
import com.ricemap.spateDB.util.SimpleGraphics;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;

public class Plot {
  /**Logger*/
  private static final Log LOG = LogFactory.getLog(Plot.class);
  
  /**Whether or not to show partition borders (boundaries) in generated image*/
  private static final String ShowBorders = "plot.show_borders";
  private static final String ShowBlockCount = "plot.show_block_count";
  private static final String ShowRecordCount = "plot.show_record_count";
  private static final String StrokeColor = "plot.stroke_color";

  /**
   * If the processed block is already partitioned (via global index), then
   * the output is the same as input (Identity map function). If the input
   * partition is not partitioned (a heap file), then the given shape is output
   * to all overlapping partitions.
   * @author tonyren, Ahmed Eldawy
   *
   */
  public static class PlotMap extends MapReduceBase 
    implements Mapper<Prism, Shape, Prism, Shape> {
    
    private Prism[] cellInfos;
    
    @Override
    public void configure(JobConf job) {
      try {
        super.configure(job);
        CellInfo[] fileCells = SpatialSite.getCells(job);
        if (fileCells == null) {
          cellInfos = null;
        } else {
          // Fill cellInfos with Prisms to be able to use them as map
          // output
          this.cellInfos = new Prism[fileCells.length];
          for (int i = 0; i < this.cellInfos.length; i++) {
            this.cellInfos[i] = new Prism(fileCells[i]);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    public void map(Prism cell, Shape shape,
        OutputCollector<Prism, Shape> output, Reporter reporter)
        throws IOException {
      if (!cell.isValid()) {
        // Output shape to all overlapping cells
        for (Prism matchedCell : cellInfos)
          if (matchedCell.isIntersected(shape))
            output.collect(matchedCell, shape);
      } else {
        // Input is already partitioned
        // Output shape to containing cell only
        output.collect(cell, shape);
      }
    }
  }
  
  /**
   * The reducer class draws an image for contents (shapes) in each cell info
   * @author tonyren, Ahmed Eldawy
   *
   */
  public static class PlotReduce extends MapReduceBase
      implements Reducer<Prism, Shape, Prism, ImageWritable> {
    
    private Prism fileMbr;
    private int imageWidth, imageHeight;
    private ImageWritable sharedValue = new ImageWritable();
    private double scale2;
    private Color strokeColor;

    @Override
    public void configure(JobConf job) {
      System.setProperty("java.awt.headless", "true");
      super.configure(job);
      fileMbr = ImageOutputFormat.getFileMBR(job);
      imageWidth = ImageOutputFormat.getImageWidth(job);
      imageHeight = ImageOutputFormat.getImageHeight(job);
      this.strokeColor = new Color(job.getInt(StrokeColor, 0));
      
      this.scale2 = (double)imageWidth * imageHeight /
          ((double)(fileMbr.x2 - fileMbr.x1) * (fileMbr.y2 - fileMbr.y1));
    }

    @Override
    public void reduce(Prism cellInfo, Iterator<Shape> values,
        OutputCollector<Prism, ImageWritable> output, Reporter reporter)
        throws IOException {
      try {
        // Initialize the image
        int image_x1 = (int) ((cellInfo.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        int image_y1 = (int) ((cellInfo.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        int image_x2 = (int) (((cellInfo.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        int image_y2 = (int) (((cellInfo.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
        int tile_width = image_x2 - image_x1;
        int tile_height = image_y2 - image_y1;
        if (tile_width == 0 || tile_height == 0)
          return;
        BufferedImage image = new BufferedImage(tile_width, tile_height,
            BufferedImage.TYPE_INT_ARGB);
        Color bg_color = new Color(0,0,0,0);

        Graphics2D graphics;
        try {
          graphics = image.createGraphics();
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
        graphics.setBackground(bg_color);
        graphics.clearRect(0, 0, tile_width, tile_height);
        graphics.setColor(strokeColor);
        graphics.translate(-image_x1, -image_y1);

        while (values.hasNext()) {
          Shape s = values.next();
          drawShape(graphics, s, fileMbr, imageWidth, imageHeight, scale2);
        }
        
        graphics.dispose();
        
        sharedValue.setImage(image);
        output.collect(cellInfo, sharedValue);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
  
  public static class PlotOutputCommitter extends FileOutputCommitter {
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      JobConf job = context.getJobConf();
      Path outFile = ImageOutputFormat.getOutputPath(job);
      int width = ImageOutputFormat.getImageWidth(job);
      int height = ImageOutputFormat.getImageHeight(job);
      
      // Combine all images in one file
      // Rename output file
      // Combine all output files into one file as we do with grid files
      FileSystem outFs = outFile.getFileSystem(job);
      Path temp = new Path(outFile.toUri().getPath()+"_temp");
      outFs.rename(outFile, temp);
      FileStatus[] resultFiles = outFs.listStatus(temp, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return path.toUri().getPath().contains("part-");
        }
      });
      
      if (resultFiles.length == 1) {
        // Only one output file
        outFs.rename(resultFiles[0].getPath(), outFile);
      } else {
        // Merge all images into one image (overlay)
        BufferedImage finalImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        for (FileStatus resultFile : resultFiles) {
          FSDataInputStream imageFile = outFs.open(resultFile.getPath());
          BufferedImage tileImage = ImageIO.read(imageFile);
          imageFile.close();

          Graphics2D graphics;
          try {
            graphics = finalImage.createGraphics();
          } catch (Throwable e) {
            graphics = new SimpleGraphics(finalImage);
          }
          graphics.drawImage(tileImage, 0, 0, null);
          graphics.dispose();
        }
        
        // Finally, write the resulting image to the given output path
        LOG.info("Writing final image");
        OutputStream outputImage = outFs.create(outFile);
        ImageIO.write(finalImage, "png", outputImage);
        outputImage.close();
      }
      
      outFs.delete(temp, true);
    }
  }
  
  
  /**Last submitted Plot job*/
  public static RunningJob lastSubmittedJob;
  
  static int min_value = Integer.MAX_VALUE;
  static int max_value = Integer.MIN_VALUE;
  
  public static void drawShape(Graphics2D graphics, Shape s, Prism fileMbr,
      int imageWidth, int imageHeight, double scale) {
    if (s instanceof Point3d) {
      Point3d pt = (Point3d) s;
      int x = (int) ((pt.x - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int y = (int) ((pt.y - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      
      if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight)
        graphics.fillRect(x, y, 1, 1);
    } else if (s instanceof Prism) {
      Prism r = (Prism) s;
      int s_x1 = (int) ((r.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y1 = (int) ((r.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      int s_x2 = (int) (((r.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y2 = (int) (((r.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      graphics.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    } else {
      LOG.warn("Cannot draw a shape of type: "+s.getClass());
      Prism r = s.getMBR();
      int s_x1 = (int) ((r.x1 - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y1 = (int) ((r.y1 - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      int s_x2 = (int) (((r.x2) - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
      int s_y2 = (int) (((r.y2) - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      if (s_x1 >= 0 && s_x1 < imageWidth && s_y1 >= 0 && s_y1 < imageHeight)
        graphics.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
    }
  }

  /**
   * Plots a Geometry from the library JTS into the given image.
   * @param graphics
   * @param geom
   * @param fileMbr
   * @param imageWidth
   * @param imageHeight
   * @param scale
   * @param shape_color
   */
  private static void drawJTSShape(Graphics2D graphics, Geometry geom,
      Prism fileMbr, int imageWidth, int imageHeight, double scale,
      Color shape_color) {
    if (geom instanceof GeometryCollection) {
      GeometryCollection geom_coll = (GeometryCollection) geom;
      for (int i = 0; i < geom_coll.getNumGeometries(); i++) {
        Geometry sub_geom = geom_coll.getGeometryN(i);
        // Recursive call to draw each geometry
        drawJTSShape(graphics, sub_geom, fileMbr, imageWidth, imageHeight, scale, shape_color);
      }
    } else if (geom instanceof com.vividsolutions.jts.geom.Polygon) {
      com.vividsolutions.jts.geom.Polygon poly = (com.vividsolutions.jts.geom.Polygon) geom;

      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        LineString ring = poly.getInteriorRingN(i);
        drawJTSShape(graphics, ring, fileMbr, imageWidth, imageHeight, scale, shape_color);
      }
      
      drawJTSShape(graphics, poly.getExteriorRing(), fileMbr, imageWidth, imageHeight, scale, shape_color);
    } else if (geom instanceof LineString) {
      LineString line = (LineString) geom;
      double geom_alpha = line.getLength() * scale;
      int color_alpha = geom_alpha > 1.0 ? 255 : (int) Math.round(geom_alpha * 255);
      if (color_alpha == 0)
        return;
      
      int[] xpoints = new int[line.getNumPoints()];
      int[] ypoints = new int[line.getNumPoints()];

      for (int i = 0; i < xpoints.length; i++) {
        double px = line.getPointN(i).getX();
        double py = line.getPointN(i).getY();
        
        // Transform a point in the polygon to image coordinates
        xpoints[i] = (int) Math.round((px - fileMbr.x1) * imageWidth / (fileMbr.x2 - fileMbr.x1));
        ypoints[i] = (int) Math.round((py - fileMbr.y1) * imageHeight / (fileMbr.y2 - fileMbr.y1));
      }
      
      // Draw the polygon
      graphics.setColor(new Color((shape_color.getRGB() & 0x00FFFFFF) | (color_alpha << 24), true));
      graphics.drawPolyline(xpoints, ypoints, xpoints.length);
    }
  }
  
  public static <S extends Shape> void plotMapReduce(Path inFile, Path outFile,
      Shape shape, int width, int height, Color color, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount, boolean background)  throws IOException {
    JobConf job = new JobConf(Plot.class);
    job.setJobName("Plot");
    
    job.setMapperClass(PlotMap.class);
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setReducerClass(PlotReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
    job.setMapOutputKeyClass(Prism.class);
    SpatialSite.setShapeClass(job, shape.getClass());
    job.setMapOutputValueClass(shape.getClass());
    
    FileSystem inFs = inFile.getFileSystem(job);
    Prism fileMbr = FileMBR.fileMBRMapReduce(inFs, inFile, shape, false);
    FileStatus inFileStatus = inFs.getFileStatus(inFile);
    
    CellInfo[] cellInfos;
    GlobalIndex<Partition> gindex = SpatialSite.getGlobalIndex(inFs, inFile);
    if (gindex == null) {
      // A heap file. The map function should partition the file
      GridInfo gridInfo = new GridInfo(fileMbr.t1, fileMbr.x1, fileMbr.y1, fileMbr.t2, fileMbr.x2,
          fileMbr.y2);
      gridInfo.calculateCellDimensions(inFileStatus.getLen(),
          inFileStatus.getBlockSize());
      cellInfos = gridInfo.getAllCells();
      // Doesn't make sense to show any partition information in a heap file
      showBorders = showBlockCount = showRecordCount = false;
    } else {
      cellInfos = SpatialSite.cellsOf(inFs, inFile);
    }
    
    // Set cell information in the job configuration to be used by the mapper
    SpatialSite.setCells(job, cellInfos);
    
    // Adjust width and height to maintain aspect ratio
    if ((fileMbr.x2 - fileMbr.x1) / (fileMbr.y2 - fileMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((fileMbr.y2 - fileMbr.y1) * width / (fileMbr.x2 - fileMbr.x1));
    } else {
      width = (int) ((fileMbr.x2 - fileMbr.x1) * height / (fileMbr.y2 - fileMbr.y1));
    }
    LOG.info("Creating an image of size "+width+"x"+height);
    ImageOutputFormat.setFileMBR(job, fileMbr);
    ImageOutputFormat.setImageWidth(job, width);
    ImageOutputFormat.setImageHeight(job, height);
    job.setBoolean(ShowBorders, showBorders);
    job.setBoolean(ShowBlockCount, showBlockCount);
    job.setBoolean(ShowRecordCount, showRecordCount);
    job.setInt(StrokeColor, color.getRGB());
    
    // Set input and output
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    // Set output committer which will stitch images together after all reducers
    // finish
    job.setOutputCommitter(PlotOutputCommitter.class);
    
    job.setOutputFormat(ImageOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outFile);
    
    if (background) {
      JobClient jc = new JobClient(job);
      lastSubmittedJob = jc.submitJob(job);
    } else {
      lastSubmittedJob = JobClient.runJob(job);
    }
  }
  
  public static <S extends Shape> void plotLocal(Path inFile, Path outFile,
      S shape, int width, int height, Color color, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount)
          throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    Prism fileMbr = FileMBR.fileMBRLocal(inFs, inFile, shape);
    LOG.info("FileMBR: "+fileMbr);
    
    // Adjust width and height to maintain aspect ratio
    if ((fileMbr.x2 - fileMbr.x1) / (fileMbr.y2 - fileMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((fileMbr.y2 - fileMbr.y1) * width / (fileMbr.x2 - fileMbr.x1));
    } else {
      width = (int) ((fileMbr.x2 - fileMbr.x1) * height / (fileMbr.y2 - fileMbr.y1));
    }
    
    double scale2 = (double) width * height
        / ((double) (fileMbr.x2 - fileMbr.x1) * (fileMbr.y2 - fileMbr.y1));

    // Create an image
    BufferedImage image = new BufferedImage(width, height,
        BufferedImage.TYPE_INT_ARGB);
    Graphics2D graphics = image.createGraphics();
    Color bg_color = new Color(0,0,0,0);
    graphics.setBackground(bg_color);
    graphics.clearRect(0, 0, width, height);
    graphics.setColor(color);

    long fileLength = inFs.getFileStatus(inFile).getLen();
    ShapeRecordReader<S> reader =
      new ShapeRecordReader<S>(inFs.open(inFile), 0, fileLength);
    
    Prism cell = reader.createKey();
    while (reader.next(cell, shape)) {
      drawShape(graphics, shape, fileMbr, width, height, scale2);
    }
    
    reader.close();
    graphics.dispose();
    FileSystem outFs = outFile.getFileSystem(new Configuration());
    OutputStream out = outFs.create(outFile, true);
    ImageIO.write(image, "png", out);
    out.close();
  }
  
  public static <S extends Shape> void plot(Path inFile, Path outFile,
      S shape, int width, int height, Color color, boolean showBorders,
      boolean showBlockCount, boolean showRecordCount)
          throws IOException {
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileStatus inFStatus = inFs.getFileStatus(inFile);
    if (inFStatus.isDir() || inFStatus.getLen() > 300 * inFStatus.getBlockSize()) {
      plotMapReduce(inFile, outFile, shape, width, height, color, showBorders, showBlockCount, showRecordCount, false);
    } else {
      plotLocal(inFile, outFile, shape, width, height, color, showBorders, showBlockCount, showRecordCount);
    }
  }

  /**
   * Combines images of different datasets into one image that is displayed
   * to users.
   * @param fs The file system that contains the datasets and images
   * @param files Paths to directories which contains the datasets
   * @param includeBoundaries Also plot the indexing boundaries of datasets
   * @return An image that is the combination of all datasets images
   * @throws IOException
   */
  public static BufferedImage combineImages(Configuration conf, Path[] files,
      boolean includeBoundaries, int width, int height) throws IOException {
    BufferedImage result = null;
    // Retrieve the MBRs of all datasets
    Prism allMbr = new Prism(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (Path file : files) {
      FileSystem fs = file.getFileSystem(conf);
      Prism mbr = FileMBR.fileMBR(fs, file, null);
      allMbr.expand(mbr);
    }

    // Adjust width and height to maintain aspect ratio
    if ((allMbr.x2 - allMbr.x1) / (allMbr.y2 - allMbr.y1) > (double) width / height) {
      // Fix width and change height
      height = (int) ((allMbr.y2 - allMbr.y1) * width / (allMbr.x2 - allMbr.x1));
    } else {
      width = (int) ((allMbr.x2 - allMbr.x1) * height / (allMbr.y2 - allMbr.y1));
    }
    result = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

      
    for (Path file : files) {
      FileSystem fs = file.getFileSystem(conf);
      if (fs.getFileStatus(file).isDir()) {
        // Retrieve the MBR of this dataset
        Prism mbr = FileMBR.fileMBR(fs, file, null);
        // Compute the coordinates of this image in the whole picture
        mbr.x1 = (mbr.x1 - allMbr.x1) * width / allMbr.getWidth();
        mbr.x2 = (mbr.x2 - allMbr.x1) * width / allMbr.getWidth();
        mbr.y1 = (mbr.y1 - allMbr.y1) * height / allMbr.getHeight();
        mbr.y2 = (mbr.y2 - allMbr.y1) * height / allMbr.getHeight();
        // Retrieve the image of this dataset
        Path imagePath = new Path(file, "_data.png");
        if (!fs.exists(imagePath))
          throw new RuntimeException("Image "+imagePath+" not ready");
        FSDataInputStream imageFile = fs.open(imagePath);
        BufferedImage image = ImageIO.read(imageFile);
        imageFile.close();
        // Draw the image
        Graphics graphics = result.getGraphics();
        graphics.drawImage(image, (int) mbr.x1, (int) mbr.y1,
            (int) mbr.getWidth(), (int) mbr.getHeight(), null);
        graphics.dispose();
        
        if (includeBoundaries) {
          // Plot also the image of the boundaries
          // Retrieve the image of the dataset boundaries
          imagePath = new Path(file, "_partitions.png");
          if (fs.exists(imagePath)) {
            imageFile = fs.open(imagePath);
            image = ImageIO.read(imageFile);
            imageFile.close();
            // Draw the image
            graphics = result.getGraphics();
            graphics.drawImage(image, (int) mbr.x1, (int) mbr.y1,
                (int) mbr.getWidth(), (int) mbr.getHeight(), null);
            graphics.dispose();
          }
        }
      }
    }
    
    return result;
  }
  
  
  private static void printUsage() {
    System.out.println("Plots all shapes to an image");
    System.out.println("Parameters: (* marks required parameters)");
    System.out.println("<input file> - (*) Path to input file");
    System.out.println("<output file> - (*) Path to output file");
    System.out.println("shape:<point|Prism|polygon|ogc> - (*) Type of shapes stored in input file");
    System.out.println("width:<w> - Maximum width of the image (1000,1000)");
    System.out.println("height:<h> - Maximum height of the image (1000,1000)");
//    System.out.println("color:<c> - Main color used to draw the picture (black)");
    System.out.println("-borders: For globally indexed files, draws the borders of partitions");
    System.out.println("-showblockcount: For globally indexed files, draws the borders of partitions");
    System.out.println("-showrecordcount: Show number of records in each partition");
    System.out.println("-overwrite: Override output file without notice");
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    System.setProperty("java.awt.headless", "true");
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Plot.class);
    Path[] files = cla.getPaths();
    if (files.length < 2) {
      printUsage();
      throw new RuntimeException("Illegal arguments. File names missing");
    }
    
    Path inFile = files[0];
    FileSystem inFs = inFile.getFileSystem(conf);
    if (!inFs.exists(inFile)) {
      printUsage();
      throw new RuntimeException("Input file does not exist");
    }
    
    boolean overwrite = cla.isOverwrite();
    Path outFile = files[1];
    FileSystem outFs = outFile.getFileSystem(conf);
    if (outFs.exists(outFile)) {
      if (overwrite)
        outFs.delete(outFile, true);
      else
        throw new RuntimeException("Output file exists and overwrite flag is not set");
    }

    boolean showBorders = cla.is("borders");
    boolean showBlockCount = cla.is("showblockcount");
    boolean showRecordCount = cla.is("showrecordcount");
    Shape shape = cla.getShape(true);
    
    int width = cla.getWidth(1000);
    int height = cla.getHeight(1000);
    
    Color color = cla.getColor();

    plot(inFile, outFile, shape, width, height, color, showBorders, showBlockCount, showRecordCount);
    
    System.out.println("Values range: ["+min_value+","+max_value+"]");
  }

}
