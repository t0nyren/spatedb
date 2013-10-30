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

/**
 * Stores grid information that can be used with spatial files. The grid is
 * uniform which means all cells have the same width and the same height.
 * 
 * @author tonyren, Ahmed Eldawy
 * 
 */
public class GridInfo extends Prism {
	public int layers, columns, rows;

	public GridInfo() {
	}

	public GridInfo(double t1, double x1, double y1, double t2, double x2,
			double y2) {
		super(t1, x1, y1, t2, x2, y2);
		this.layers = 0;
		this.columns = 0;
		this.rows = 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(layers);
		out.writeInt(columns);
		out.writeInt(rows);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		layers = in.readInt();
		columns = in.readInt();
		rows = in.readInt();
	}

	@Override
	public String toString() {
		return "grid: " + t1 + "," + x1 + "," + y1 + "," + x2 + "," + y2 + ", "
				+ "cell: " + getAverageCellDepth() + ","
				+ getAverageCellWidth() + "," + getAverageCellHeight() + "("
				+ layers + "x" + columns + "x" + rows + ")";
	}

	public double getAverageCellDepth() {
		return (t2 - t1) / Math.max(layers, 1);
	}

	public double getAverageCellHeight() {
		return (y2 - y1) / Math.max(rows, 1);
	}

	public double getAverageCellWidth() {
		return (x2 - x1) / Math.max(columns, 1);
	}

	@Override
	public boolean equals(Object obj) {
		GridInfo gi = (GridInfo) obj;
		return super.equals(obj) && this.layers == gi.layers
				&& this.columns == gi.columns && this.rows == gi.rows;
	}

	public void calculateCellDimensions(long totalFileSize, long blockSize) {
		// An empirical number for the expected overhead in grid file due to
		// replication
		int numBlocks = (int) Math.ceil((double) totalFileSize / blockSize);
		calculateCellDimensions(numBlocks);
	}

	public void calculateCellDimensions(int numCells) {
		int gridLayers = 1;
		int gridCols = 1;
		int gridRows = 1;

		while (gridLayers * gridRows * gridCols < numCells) {
			// ( cellWidth > cellHeight )
			if ((t2 - t2) / gridLayers > (x2 - x1) / gridCols) {
				gridLayers++;
			} else if ((x2 - x1) / gridCols > (y2 - y1) / gridRows) {
				gridCols++;
			} else {
				gridRows++;
			}
		}
		layers = gridLayers;
		columns = gridCols;
		rows = gridRows;
	}

	@Override
	public Text toText(Text text) {
		final byte[] Comma = ",".getBytes();
		super.toText(text);
		text.append(Comma, 0, Comma.length);
		TextSerializerHelper.serializeLong(layers, text, ',');
		TextSerializerHelper.serializeLong(columns, text, ',');
		TextSerializerHelper.serializeLong(rows, text, '\0');
		return text;
	}

	@Override
	public void fromText(Text text) {
		super.fromText(text);
		if (text.getLength() > 0) {
			// Remove the first comma
			System.arraycopy(text.getBytes(), 1, text.getBytes(), 0,
					text.getLength() - 1);
			layers = (int) TextSerializerHelper.consumeInt(text, ',');
			columns = (int) TextSerializerHelper.consumeInt(text, ',');
			rows = (int) TextSerializerHelper.consumeInt(text, '\0');
		}
	}

	public CellInfo[] getAllCells() {
		int cellIndex = 0;
		CellInfo[] cells = new CellInfo[layers * columns * rows];
		double ystart = y1;
		for (int row = 0; row < rows; row++) {
			double yend = y1 + (y2 - y1) * (row + 1) / rows;
			double xstart = x1;
			for (int col = 0; col < columns; col++) {
				double xend = x1 + (x2 - x1) * (col + 1) / columns;
				double tstart = t1;
				for (int lay = 0; lay < layers; lay++){
					double tend = t1 + (t2 - t1) * (lay + 1) / layers;
					cells[cellIndex] = new CellInfo(++cellIndex, tstart, xstart, ystart, tend,
						xend, yend);
					tstart = tend;
				}

				xstart = xend;
			}
			ystart = yend;
		}
		return cells;
	}

	public Prism getOverlappingCells(Prism rect) {
		int lay1, lay2, col1, col2, row1, row2;
		lay1 = (int) Math.floor((rect.t1 - this.t1) / (this.t2 - this.t1)
				* layers);
		lay2 = (int) Math.floor((rect.t2 - this.t1) / (this.t2 - this.t1)
				* layers);
		if (lay2 == layers)
			lay2--;
		col1 = (int) Math.floor((rect.x1 - this.x1) / (this.x2 - this.x1)
				* columns);
		col2 = (int) Math.floor((rect.x2 - this.x1) / (this.x2 - this.x1)
				* columns);
		if (col2 == columns)
			col2--;
		row1 = (int) Math.floor((rect.y1 - this.y1) / (this.y2 - this.y1)
				* rows);
		row2 = (int) Math.floor((rect.y2 - this.y1) / (this.y2 - this.y1)
				* rows);
		if (row2 == rows)
			row2--;
		return new Prism(lay1, col1, row1, lay2 - lay1 + 1, col2 - col1 + 1, row2 - row1 + 1);
	}
	
	
	
	public CellInfo getCell(int cellId) {
		int lay = ((cellId - 1) % columns) % rows;
		int col = ((cellId - 1) % columns) / rows;
		int row = (cellId - 1) / columns;
		
		double tstart = t1 + (t2 - t1) * lay / layers;
		double tend = lay == layers -1 ? t2 : (t1 + (t2 - t1) * (lay + 1) / layers);
		double xstart = x1 + (x2 - x1) * col / columns;
		double xend = col == columns - 1 ? x2 : (x1 + (x2 - x1) * (col + 1)
				/ columns);
		double ystart = y1 + (y2 - y1) * row / rows;
		double yend = (row == rows - 1) ? y2 : (y1 + (y2 - y1) * (row + 1)
				/ rows);
		return new CellInfo(cellId, tstart, xstart, ystart, tend, xend, yend);
	}
}
