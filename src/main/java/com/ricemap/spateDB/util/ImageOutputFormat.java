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
package com.ricemap.spateDB.util;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.ricemap.spateDB.core.SpatialSite;
import com.ricemap.spateDB.shape.Prism;

/**
 * An output format that is used to plot ImageWritable to PNG image.
 * @author tonyren, Ahmed Eldawy
 *
 */
public class ImageOutputFormat extends FileOutputFormat<Prism, ImageWritable> {
  /**MBR of the input file*/
  private static final String InputFileMBR = "plot.file_mbr";
  
  /**Configuration line for image width*/
  private static final String ImageWidth = "plot.image_width";

  /**Configuration line for image height*/
  private static final String ImageHeight = "plot.image_height";

  /**Used to indicate the progress*/
  private Progressable progress;
  
  class ImageRecordWriter implements RecordWriter<Prism, ImageWritable> {

    private final FileSystem outFs;
    private final Path out;
    private final int image_width;
    private final Prism fileMbr;
    private final int image_height;
    
    private BufferedImage image;

    ImageRecordWriter(Path out, FileSystem outFs,
        int width, int height, Prism fileMbr) {
      System.setProperty("java.awt.headless", "true");
      this.out = out;
      this.outFs = outFs;
      this.image_width = width;
      this.image_height = height;
      this.fileMbr = fileMbr;
      this.image = new BufferedImage(image_width, image_height,
          BufferedImage.TYPE_INT_ARGB);
    }

    @Override
    public void write(Prism cell, ImageWritable value) throws IOException {
      progress.progress();
      int tile_x = (int) ((cell.x1 - fileMbr.x1) * image_width / (fileMbr.x2 - fileMbr.x1));
      int tile_y = (int) ((cell.y1 - fileMbr.y1) * image_height / (fileMbr.y2 - fileMbr.y1));
      Graphics2D graphics;
      try {
        graphics = image.createGraphics();
      } catch (Throwable e) {
        graphics = new SimpleGraphics(image);
      }
      graphics.drawImage(value.getImage(), tile_x, tile_y, null);
      graphics.dispose();
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      OutputStream output = outFs.create(out);
      ImageIO.write(image, "png", output);
      output.close();
    }
  }

  @Override
  public RecordWriter<Prism, ImageWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    this.progress = progress;
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);
    Prism fileMbr = getFileMBR(job);
    int imageWidth = getImageWidth(job);
    int imageHeight = getImageHeight(job);
    

    return new ImageRecordWriter(file, fs, imageWidth, imageHeight, fileMbr);
  }

  public static void setImageWidth(Configuration conf, int width) {
    conf.setInt(ImageWidth, width);
  }

  public static void setImageHeight(Configuration conf, int height) {
    conf.setInt(ImageHeight, height);
  }
  
  public static void setFileMBR(Configuration conf, Prism mbr) {
    SpatialSite.setShape(conf, InputFileMBR, mbr);
  }
  
  public static int getImageWidth(Configuration conf) {
    return conf.getInt(ImageWidth, 1000);
  }

  public static int getImageHeight(Configuration conf) {
    return conf.getInt(ImageHeight, 1000);
  }
  
  public static Prism getFileMBR(Configuration conf) {
    return (Prism) SpatialSite.getShape(conf, InputFileMBR);
  }
}
