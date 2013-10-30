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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.ricemap.spateDB.io.TextSerializerHelper;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;


/**
 * Information about a specific cell in a grid.
 * Note: Whenever you change the instance variables that need to
 * be stored in disk, you have to manually fix the implementation of class
 * BlockListAsLongs
 * @author tonyren, aseldawy
 *
 */
public class CellInfo extends Prism {
  
  /**
   * A unique ID for this cell in a file. This must be set initially when
   * cells for a file are created. It cannot be guessed from cell dimensions.
   */
  public long cellId;

  /**
   * Loads a cell serialized to the given stream
   * @param in
   * @throws IOException 
   */
  public CellInfo(DataInput in) throws IOException {
    this.readFields(in);
  }

  public CellInfo(String in) {
    this.fromText(new Text(in));
  }

  public CellInfo() {
    super();
  }

  public CellInfo(long id, double t1, double x1, double y1, double t2, double x2, double y2) {
    super(t1, x1, y1, t2, x2, y2);
    this.cellId = id;
  }

  public CellInfo(long id, Prism cellInfo) {
    this(id, cellInfo.t1, cellInfo.x1, cellInfo.y1, cellInfo.t2, cellInfo.x2, cellInfo.y2);
    if (id == 0)
      throw new RuntimeException("Invalid cell id: -1");
  }
  
  public void set(CellInfo c) {
    if (c == null) {
      this.cellId = 0; // Invalid number
    } else {
      super.set(c); // Set Prism
      this.cellId = c.cellId; // Set cellId
    }
  }
  
  @Override
  public String toString() {
    return "Cell #"+cellId+" "+super.toString();
  }
  
  @Override
  public CellInfo clone() {
    return new CellInfo(cellId, t1, x1, y1, t2, x2, y2);
  }
  
  @Override
  public boolean equals(Object obj) {
    return ((CellInfo)obj).cellId == this.cellId;
  }
  
  @Override
  public int hashCode() {
    return (int) this.cellId;
  }
  
  @Override
  public int compareTo(Shape s) {
    return (int) (this.cellId - ((CellInfo)s).cellId);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(cellId);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.cellId = in.readLong();
    super.readFields(in);
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeLong(cellId, text, ',');
    return super.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    this.cellId = TextSerializerHelper.consumeLong(text, ',');
    super.fromText(text);
  }
}
