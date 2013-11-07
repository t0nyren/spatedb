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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.QuickSort;

import com.ricemap.spateDB.io.MemoryInputStream;
import com.ricemap.spateDB.io.Text2;
import com.ricemap.spateDB.shape.Point3d;
import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;

/**
 * An RTree loaded in bulk and never changed after that. It cannot by
 * dynamically manipulated by either insertion or deletion. It only works with
 * 2-dimensional objects (keys).
 * 
 * @author tonyren, eldawy
 * 
 */
public class RTree<T extends Shape> implements Writable, Iterable<T> {
	public static enum FIELD_TYPE{NULL, Integer, Long, Double};
	/** Logger */
	private static final Log LOG = LogFactory.getLog(RTree.class);

	/** Size of tree header on disk. Height + Degree + Number of records + isColumnar*/
	public static final int TreeHeaderSize = 4 + 4 + 4 + 4;

	/** Size of a node. Offset of first child + dimensions (x, y, width, height) */
	public static final int NodeSize = 4 + 8 * 6;
	
	/** t, x ,y */
	public static final int IndexUnitSize = 8 * 3;

	/** An instance of T that can be used to deserialize objects from disk */
	T stockObject;

	public boolean columnar;
	
	/** Height of the tree (number of levels) */
	private int height;

	/** Degree of internal nodes in the tree */
	private int degree;

	/** Total number of nodes in the tree */
	private int nodeCount;
	
	

	/** Number of leaf nodes */
	private int leafNodeCount;

	/** Number of non-leaf nodes */
	private int nonLeafNodeCount;

	/** Number of elements in the tree */
	private int elementCount;

	/** An input stream that is used to read node structure (i.e., nodes) */
	private FSDataInputStream structure;

	/** Input stream to tree data */
	private FSDataInputStream data;

	/** The start offset of the tree in the data stream */
	private long treeStartOffset;

	/**
	 * Total tree size (header + structure + data) used to read the data in the
	 * last leaf node correctly
	 */
	private int treeSize;
	


	public RTree() {
	}

	/**
	 * Builds the RTree given a serialized list of elements. It uses the given
	 * stockObject to deserialize these elements and build the tree. Also writes
	 * the created tree to the disk directly.
	 * 
	 * @param elements
	 *            - serialization of elements to be written
	 * @param offset
	 *            - index of the first element to use in the elements array
	 * @param len
	 *            - number of bytes to user from the elements array
	 * @param bytesAvailable
	 *            - size available (in bytes) to store the tree structures
	 * @param dataOut
	 *            - an output to use for writing the tree to
	 * @param fast_sort
	 *            - setting this to <code>true</code> allows the method to run
	 *            faster by materializing the offset of each element in the list
	 *            which speeds up the comparison. However, this requires an
	 *            additional 16 bytes per element. So, for each 1M elements, the
	 *            method will require an additional 16 M bytes (approximately).
	 */
	public void bulkLoadWrite(final byte[] element_bytes, final int offset,
			final int len, final int degree, DataOutput dataOut,
			final boolean fast_sort, final boolean columnarStorage) {
		try {
			columnar = columnarStorage;
			//TODO: the order of fields should be stable under Oracle JVM, but not guaranteed
			Field[] fields = stockObject.getClass().getDeclaredFields();
			
			// Count number of elements in the given text
			int i_start = offset;
			final Text line = new Text();
			while (i_start < offset + len) {
				int i_end = skipToEOL(element_bytes, i_start);
				// Extract the line without end of line character
				line.set(element_bytes, i_start, i_end - i_start - 1);
				stockObject.fromText(line);
				
				elementCount++;
				i_start = i_end;
			}
			LOG.info("Bulk loading an RTree with " + elementCount + " elements");

			// It turns out the findBestDegree returns the best degree when the
			// whole
			// tree is loaded to memory when processed. However, as current
			// algorithms
			// process the tree while it's on disk, a higher degree should be
			// selected
			// such that a node fits one file block (assumed to be 4K).
			// final int degree = findBestDegree(bytesAvailable, elementCount);
			LOG.info("Writing an RTree with degree " + degree);

			int height = Math.max(1,
					(int) Math.ceil(Math.log(elementCount) / Math.log(degree)));
			int leafNodeCount = (int) Math.pow(degree, height - 1);
			if (elementCount < 2 * leafNodeCount && height > 1) {
				height--;
				leafNodeCount = (int) Math.pow(degree, height - 1);
			}
			int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
			int nonLeafNodeCount = nodeCount - leafNodeCount;

			// Keep track of the offset of each element in the text
			final int[] offsets = new int[elementCount];
			final int[] ids = new int[elementCount];
			final double[] ts = fast_sort ? new double[elementCount] : null;
			final double[] xs = fast_sort ? new double[elementCount] : null;
			final double[] ys = fast_sort ? new double[elementCount] : null;
			
			//initialize columnar data output
			ByteArrayOutputStream index_bos = new ByteArrayOutputStream();
			DataOutputStream index_dos = new DataOutputStream(index_bos);
			ByteArrayOutputStream[] bos = new ByteArrayOutputStream[fields.length];
			DataOutputStream[] dos = new DataOutputStream[fields.length];
			for (int i = 0; i < bos.length; i++){
				bos[i] = new ByteArrayOutputStream();
				dos[i] = new DataOutputStream(bos[i]);
			}

			i_start = offset;
			line.clear();
			for (int i = 0; i < elementCount; i++) {
				offsets[i] = i_start;
				ids[i] = i;
				int i_end = skipToEOL(element_bytes, i_start);
				if (xs != null) {
					// Extract the line with end of line character
					line.set(element_bytes, i_start, i_end - i_start - 1);
					stockObject.fromText(line);
					// Sample center of the shape
					ts[i] = (stockObject.getMBR().t1 + stockObject.getMBR().t2) / 2;
					xs[i] = (stockObject.getMBR().x1 + stockObject.getMBR().x2) / 2;
					ys[i] = (stockObject.getMBR().y1 + stockObject.getMBR().y2) / 2;
					
					//build columnar storage
					if (stockObject instanceof Point3d){
						index_dos.writeDouble(ts[i]);
						index_dos.writeDouble(xs[i]);
						index_dos.writeDouble(ys[i]);
					}
					else{
						throw new RuntimeException("Indexing non-point shape with RTREE is not supported yet");
					}
					
					for (int j = 0 ; j < fields.length; j++){
						if (fields[j].getType().equals(Integer.TYPE)){
							dos[j].writeInt(fields[j].getInt(stockObject));
						}
						else if (fields[j].getType().equals(Double.TYPE)){
							dos[j].writeDouble(fields[j].getDouble(stockObject));
						}
						else if (fields[j].getType().equals(Long.TYPE)){
							dos[j].writeLong(fields[j].getLong(stockObject));
						}
						else{
							continue;
							//throw new RuntimeException("Field type is not supported yet");
						}
					}
				}
				i_start = i_end;
			}
			index_dos.close();
			for (int i = 0; i < dos.length; i++){
				dos[i].close();
			}

			/** A struct to store information about a split */
			class SplitStruct extends Prism {
				/** Start and end index for this split */
				int index1, index2;
				/** Direction of this split */
				byte direction;
				/** Index of first element on disk */
				int offsetOfFirstElement;

				static final byte DIRECTION_T = 0;
				static final byte DIRECTION_X = 1;
				static final byte DIRECTION_Y = 2;

				SplitStruct(int index1, int index2, byte direction) {
					this.index1 = index1;
					this.index2 = index2;
					this.direction = direction;
				}

				@Override
				public void write(DataOutput out) throws IOException {
					//
					if (columnarStorage)
						out.writeInt(index1);
					else
						out.writeInt(offsetOfFirstElement);
					super.write(out);
				}

				void partition(Queue<SplitStruct> toBePartitioned) {
					IndexedSortable sortableT;
					IndexedSortable sortableX;
					IndexedSortable sortableY;

					if (fast_sort) {
						// Use materialized xs[] and ys[] to do the comparisons
						sortableT = new IndexedSortable() {
							@Override
							public void swap(int i, int j) {
								// Swap ts
								double tempt = ts[i];
								ts[i] = ts[j];
								ts[j] = tempt;
								// Swap xs
								double tempx = xs[i];
								xs[i] = xs[j];
								xs[j] = tempx;
								// Swap ys
								double tempY = ys[i];
								ys[i] = ys[j];
								ys[j] = tempY;
								// Swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
								
								tempid = ids[i];
								ids[i] = ids[j];
								ids[j] = tempid;
							}

							@Override
							public int compare(int i, int j) {
								if (ts[i] < ts[j])
									return -1;
								if (ts[i] > ts[j])
									return 1;
								return 0;
							}
						};
						sortableX = new IndexedSortable() {
							@Override
							public void swap(int i, int j) {
								// Swap ts
								double tempt = ts[i];
								ts[i] = ts[j];
								ts[j] = tempt;
								// Swap xs
								double tempx = xs[i];
								xs[i] = xs[j];
								xs[j] = tempx;
								// Swap ys
								double tempY = ys[i];
								ys[i] = ys[j];
								ys[j] = tempY;
								// Swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
								tempid = ids[i];
								ids[i] = ids[j];
								ids[j] = tempid;
							}

							@Override
							public int compare(int i, int j) {
								if (ts[i] < ts[j])
									return -1;
								if (xs[i] < xs[j])
									return -1;
								if (xs[i] > xs[j])
									return 1;
								return 0;
							}
						};

						sortableY = new IndexedSortable() {
							@Override
							public void swap(int i, int j) {
								// Swap ts
								double tempt = ts[i];
								ts[i] = ts[j];
								ts[j] = tempt;
								// Swap xs
								double tempx = xs[i];
								xs[i] = xs[j];
								xs[j] = tempx;
								// Swap ys
								double tempY = ys[i];
								ys[i] = ys[j];
								ys[j] = tempY;
								// Swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
								
								tempid = ids[i];
								ids[i] = ids[j];
								ids[j] = tempid;
							}

							@Override
							public int compare(int i, int j) {
								if (ys[i] < ys[j])
									return -1;
								if (ys[i] > ys[j])
									return 1;
								return 0;
							}
						};
					} else {
						// No materialized xs and ys. Always deserialize objects
						// to compare
						sortableT = new IndexedSortable() {
							@Override
							public void swap(int i, int j) {
								// Swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
								
								tempid = ids[i];
								ids[i] = ids[j];
								ids[j] = tempid;
							}

							@Override
							public int compare(int i, int j) {
								// Get end of line
								int eol = skipToEOL(element_bytes, offsets[i]);
								line.set(element_bytes, offsets[i], eol
										- offsets[i] - 1);
								stockObject.fromText(line);
								double ti = (stockObject.getMBR().t1 + stockObject
										.getMBR().t2) / 2;
								
								eol = skipToEOL(element_bytes, offsets[j]);
								line.set(element_bytes, offsets[j], eol
										- offsets[j] - 1);
								stockObject.fromText(line);
								double tj = (stockObject.getMBR().t1 + stockObject
										.getMBR().t2) / 2;
								if (ti < tj)
									return -1;
								if (ti > tj)
									return 1;
								return 0;
							}
						};
						sortableX = new IndexedSortable() {
							@Override
							public void swap(int i, int j) {
								// Swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
								
								tempid = ids[i];
								ids[i] = ids[j];
								ids[j] = tempid;
							}

							@Override
							public int compare(int i, int j) {
								// Get end of line
								int eol = skipToEOL(element_bytes, offsets[i]);
								line.set(element_bytes, offsets[i], eol
										- offsets[i] - 1);
								stockObject.fromText(line);
								double xi = (stockObject.getMBR().x1 + stockObject
										.getMBR().x2) / 2;

								eol = skipToEOL(element_bytes, offsets[j]);
								line.set(element_bytes, offsets[j], eol
										- offsets[j] - 1);
								stockObject.fromText(line);
								double xj = (stockObject.getMBR().x1 + stockObject
										.getMBR().x2) / 2;
								if (xi < xj)
									return -1;
								if (xi > xj)
									return 1;
								return 0;
							}
						};

						sortableY = new IndexedSortable() {
							@Override
							public void swap(int i, int j) {
								// Swap id
								int tempid = offsets[i];
								offsets[i] = offsets[j];
								offsets[j] = tempid;
								
								tempid = ids[i];
								ids[i] = ids[j];
								ids[j] = tempid;
							}

							@Override
							public int compare(int i, int j) {
								int eol = skipToEOL(element_bytes, offsets[i]);
								line.set(element_bytes, offsets[i], eol
										- offsets[i] - 1);
								stockObject.fromText(line);
								double yi = (stockObject.getMBR().y1 + stockObject
										.getMBR().y2) / 2;

								eol = skipToEOL(element_bytes, offsets[j]);
								line.set(element_bytes, offsets[j], eol
										- offsets[j] - 1);
								stockObject.fromText(line);
								double yj = (stockObject.getMBR().y1 + stockObject
										.getMBR().y2) / 2;
								if (yi < yj)
									return -1;
								if (yi > yj)
									return 1;
								return 0;
							}
						};
					}

					final IndexedSorter sorter = new QuickSort();

					final IndexedSortable[] sortables = new IndexedSortable[3];
					sortables[SplitStruct.DIRECTION_T] = sortableT;
					sortables[SplitStruct.DIRECTION_X] = sortableX;
					sortables[SplitStruct.DIRECTION_Y] = sortableY;

					sorter.sort(sortables[direction], index1, index2);

					// Partition into maxEntries partitions (equally) and
					// create a SplitStruct for each partition
					int i1 = index1;
					for (int iSplit = 0; iSplit < degree; iSplit++) {
						int i2 = index1 + (index2 - index1) * (iSplit + 1)
								/ degree;
						SplitStruct newSplit;
						if (direction == 0){
							 newSplit = new SplitStruct(i1, i2,
									(byte) 1);
						}
						else if (direction == 1){
							 newSplit = new SplitStruct(i1, i2,
									(byte) 2);
						}
						else{
							 newSplit = new SplitStruct(i1, i2,
									(byte) 0);
						}
						toBePartitioned.add(newSplit);
						i1 = i2;
					}
				}
			}

			// All nodes stored in level-order traversal
			Vector<SplitStruct> nodes = new Vector<SplitStruct>();
			final Queue<SplitStruct> toBePartitioned = new LinkedList<SplitStruct>();
			toBePartitioned.add(new SplitStruct(0, elementCount,
					SplitStruct.DIRECTION_X));

			while (!toBePartitioned.isEmpty()) {
				SplitStruct split = toBePartitioned.poll();
				if (nodes.size() < nonLeafNodeCount) {
					// This is a non-leaf
					split.partition(toBePartitioned);
				}
				nodes.add(split);
			}

			if (nodes.size() != nodeCount) {
				throw new RuntimeException("Expected node count: " + nodeCount
						+ ". Real node count: " + nodes.size());
			}

			// Now we have our data sorted in the required order. Start building
			// the tree.
			// Store the offset of each leaf node in the tree
			FSDataOutputStream fakeOut = new FSDataOutputStream(
					new java.io.OutputStream() {
						// Null output stream
						@Override
						public void write(int b) throws IOException {
							// Do nothing
						}

						@Override
						public void write(byte[] b, int off, int len)
								throws IOException {
							// Do nothing
						}

						@Override
						public void write(byte[] b) throws IOException {
							// Do nothing
						}
					}, null, TreeHeaderSize + nodes.size() * NodeSize);
			for (int i_leaf = nonLeafNodeCount, i = 0; i_leaf < nodes.size(); i_leaf++) {
				nodes.elementAt(i_leaf).offsetOfFirstElement = (int) fakeOut
						.getPos();
				if (i != nodes.elementAt(i_leaf).index1)
					throw new RuntimeException();
				double t1, x1, y1, t2, x2, y2;

				// Initialize MBR to first object
				int eol = skipToEOL(element_bytes, offsets[i]);
				fakeOut.write(element_bytes, offsets[i], eol - offsets[i]);
				line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
				stockObject.fromText(line);
				Prism mbr = stockObject.getMBR();
				t1 = mbr.t1;
				x1 = mbr.x1;
				y1 = mbr.y1;
				t2 = mbr.t2;
				x2 = mbr.x2;
				y2 = mbr.y2;
				i++;

				while (i < nodes.elementAt(i_leaf).index2) {
					eol = skipToEOL(element_bytes, offsets[i]);
					fakeOut.write(element_bytes, offsets[i], eol - offsets[i]);
					line.set(element_bytes, offsets[i], eol - offsets[i] - 1);
					stockObject.fromText(line);
					mbr = stockObject.getMBR();
					if (mbr.t1 < t1)
						t1 = mbr.t1;
					if (mbr.x1 < x1)
						x1 = mbr.x1;
					if (mbr.y1 < y1)
						y1 = mbr.y1;
					if (mbr.t2 > t2)
						t2 = mbr.t2;
					if (mbr.x2 > x2)
						x2 = mbr.x2;
					if (mbr.y2 > y2)
						y2 = mbr.y2;
					i++;
				}
				nodes.elementAt(i_leaf).set(t1, x1, y1, t2, x2, y2);
			}
			fakeOut.close();
			fakeOut = null;

			// Calculate MBR and offsetOfFirstElement for non-leaves
			for (int i_node = nonLeafNodeCount - 1; i_node >= 0; i_node--) {
				int i_first_child = i_node * degree + 1;
				nodes.elementAt(i_node).offsetOfFirstElement = nodes
						.elementAt(i_first_child).offsetOfFirstElement;
				int i_child = 0;
				Prism mbr;
				mbr = nodes.elementAt(i_first_child + i_child);
				double t1 = mbr.t1;
				double x1 = mbr.x1;
				double y1 = mbr.y1;
				double t2 = mbr.t2;
				double x2 = mbr.x2;
				double y2 = mbr.y2;
				i_child++;

				while (i_child < degree) {
					mbr = nodes.elementAt(i_first_child + i_child);
					if (mbr.t1 < t1)
						t1 = mbr.t1;
					if (mbr.x1 < x1)
						x1 = mbr.x1;
					if (mbr.y1 < y1)
						y1 = mbr.y1;
					if (mbr.t2 > t2)
						t2 = mbr.t2;
					if (mbr.x2 > x2)
						x2 = mbr.x2;
					if (mbr.y2 > y2)
						y2 = mbr.y2;
					i_child++;
				}
				nodes.elementAt(i_node).set(t1, x1, y1, t2, x2, y2);
			}

			// Start writing the tree
			// write tree header (including size)
			// Total tree size. (== Total bytes written - 8 bytes for the size
			// itself)
			dataOut.writeInt(TreeHeaderSize + NodeSize * nodeCount + len);
			// Tree height
			dataOut.writeInt(height);
			// Degree
			dataOut.writeInt(degree);
			dataOut.writeInt(elementCount);
			
			//isColumnar
			dataOut.writeInt(columnarStorage ? 1 : 0);

			// write nodes
			for (SplitStruct node : nodes) {
				node.write(dataOut);
			}
			// write elements
			if (columnarStorage){
				byte[] index_bs = index_bos.toByteArray();
				byte[][] bss = new byte[bos.length][];
				for (int i = 0; i < bss.length; i++){
					bss[i] = bos[i].toByteArray();
				}
				for (int element_i = 0; element_i < elementCount; element_i++) {
					//int eol = skipToEOL(element_bytes, offsets[element_i]);
					//dataOut.write(element_bytes, offsets[element_i], eol - offsets[element_i]);
					dataOut.write(index_bs, ids[element_i]*IndexUnitSize, IndexUnitSize);
				}
				
				for (int i = 0; i < fields.length; i++){
					int fieldSize = 0;
					if (fields[i].getType().equals(Integer.TYPE)){
						fieldSize = 4;
					}
					else if (fields[i].getType().equals(Long.TYPE)){
						fieldSize = 8;
					}
					else if (fields[i].getType().equals(Double.TYPE)){
						fieldSize = 8;
					}
					else{
						//throw new RuntimeException("Unsupported field type: " + fields[i].getType().getName());
						continue;
					}
					for (int element_i = 0; element_i < elementCount; element_i++) {
						//int eol = skipToEOL(element_bytes, offsets[element_i]);
						//dataOut.write(element_bytes, offsets[element_i], eol - offsets[element_i]);
						dataOut.write(bss[i], ids[element_i]*fieldSize, fieldSize);
					}
				}
			}
			else{
				for (int element_i = 0; element_i < elementCount; element_i++) {
					int eol = skipToEOL(element_bytes, offsets[element_i]);
					dataOut.write(element_bytes, offsets[element_i], eol - offsets[element_i]);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new RuntimeException("write is no longer supported. "
				+ "Please use bulkLoadWrite to write the RTree.");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Tree size (Header + structure + data)
		treeSize = in.readInt();
		if (treeSize == 0) {
			height = elementCount = 0;
			return;
		}

		// Read only the tree structure in memory while actual records remain on
		// disk and loaded when necessary
		height = in.readInt();
		if (height == 0)
			return;
		degree = in.readInt();
		elementCount = in.readInt();
		columnar = in.readInt()==1;

		// Keep only tree structure in memory
		nodeCount = (int) ((powInt(degree, height) - 1) / (degree - 1));
		int structureSize = nodeCount * NodeSize;
		byte[] treeStructure = new byte[structureSize];
		in.readFully(treeStructure, 0, structureSize);
		structure = new FSDataInputStream(new MemoryInputStream(treeStructure));
		if (in instanceof FSDataInputStream) {
			this.treeStartOffset = ((FSDataInputStream) in).getPos()
					- structureSize - TreeHeaderSize;
			this.data = (FSDataInputStream) in;
		} else {
			// Load all tree data in memory
			this.treeStartOffset = 0 - structureSize - TreeHeaderSize;
			int treeDataSize = treeSize - TreeHeaderSize - structureSize;
			byte[] treeData = new byte[treeDataSize];
			in.readFully(treeData, 0, treeDataSize);
			this.data = new FSDataInputStream(new MemoryInputStream(treeData));
		}
		nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
		leafNodeCount = (int) Math.pow(degree, height - 1);
		nonLeafNodeCount = nodeCount - leafNodeCount;
	}

	/**
	 * Reads and skips the header of the tree returning the total number of
	 * bytes skipped from the stream. This is used as a preparatory function to
	 * read all elements in the tree without the index part.
	 * 
	 * @param in
	 * @return - Total number of bytes read and skipped
	 * @throws IOException
	 */
	public static int skipHeader(InputStream in) throws IOException {
		DataInput dataIn = in instanceof DataInput ? (DataInput) in
				: new DataInputStream(in);
		int skippedBytes = 0;
		/* int treeSize = */dataIn.readInt();
		skippedBytes += 4;
		int height = dataIn.readInt();
		skippedBytes += 4;
		if (height == 0) {
			// Empty tree. No results
			return skippedBytes;
		}
		int degree = dataIn.readInt();
		skippedBytes += 4;
		int nodeCount = (int) ((powInt(degree, height) - 1) / (degree - 1));
		/* int elementCount = */dataIn.readInt();
		skippedBytes += 4;
		// Skip all nodes
		dataIn.skipBytes(nodeCount * NodeSize);
		skippedBytes += nodeCount * NodeSize;
		return skippedBytes;
	}

	/**
	 * Returns the total size of the header (including the index) in bytes.
	 * Assume that the input is aligned to the start offset of the tree
	 * (header). Note that the part of the header is consumed from the given
	 * input to be able to determine header size.
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static int getHeaderSize(DataInput in) throws IOException {
		int header_size = 0;
		/* int treeSize = */in.readInt();
		header_size += 4;
		int height = in.readInt();
		header_size += 4;
		if (height == 0) {
			// Empty tree. No results
			return header_size;
		}
		int degree = in.readInt();
		header_size += 4;
		int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
		/* int elementCount = */in.readInt();
		header_size += 4;
		// Add the size of all nodes
		header_size += nodeCount * NodeSize;
		return header_size;
	}

	/**
	 * Returns total number of elements
	 * 
	 * @return
	 */
	public int getElementCount() {
		return elementCount;
	}

	/**
	 * Returns the MBR of the root
	 * 
	 * @return
	 */
	public Prism getMBR() {
		Prism mbr = null;
		try {
			// MBR of the tree is the MBR of the root node
			structure.seek(0);
			mbr = new Prism();
			/* int offset = */structure.readInt();
			mbr.readFields(structure);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return mbr;
	}

	/**
	 * Reads and returns the element with the given index
	 * 
	 * @param i
	 * @return
	 * @throws IOException
	 */
	public T readElement(int i) {
		Iterator<T> iter = iterator();
		while (i-- > 0 && iter.hasNext()) {
			iter.next();
		}
		return iter.next();
	}

	public void setStockObject(T stockObject) {
		this.stockObject = stockObject;
	}

	/**
	 * Create Prisms that together pack all points in sample such that each
	 * Prism contains roughly the same number of points. In other words it tries
	 * to balance number of points in each Prism. Works similar to the logic of
	 * bulkLoad but does only one level of Prisms.
	 * 
	 * @param samples
	 * @param gridInfo
	 *            - Used as a hint for number of Prisms per row or column
	 * @return
	 */
	public static Prism[] packInPrisms(GridInfo gridInfo, final Point3d[] sample) {
		Prism[] Prisms = new Prism[gridInfo.layers * gridInfo.columns * gridInfo.rows];
		int iPrism = 0;
		// Sort in t direction
		final IndexedSortable sortableT = new IndexedSortable() {
			@Override
			public void swap(int i, int j) {
				Point3d temp = sample[i];
				sample[i] = sample[j];
				sample[j] = temp;
			}

			@Override
			public int compare(int i, int j) {
				if (sample[i].t < sample[j].t)
					return -1;
				if (sample[i].t > sample[j].t)
					return 1;
				return 0;
			}
		};
		// Sort in x direction
		final IndexedSortable sortableX = new IndexedSortable() {
			@Override
			public void swap(int i, int j) {
				Point3d temp = sample[i];
				sample[i] = sample[j];
				sample[j] = temp;
			}

			@Override
			public int compare(int i, int j) {
				if (sample[i].x < sample[j].x)
					return -1;
				if (sample[i].x > sample[j].x)
					return 1;
				return 0;
			}
		};

		// Sort in y direction
		final IndexedSortable sortableY = new IndexedSortable() {
			@Override
			public void swap(int i, int j) {
				Point3d temp = sample[i];
				sample[i] = sample[j];
				sample[j] = temp;
			}

			@Override
			public int compare(int i, int j) {
				if (sample[i].y < sample[j].y)
					return -1;
				if (sample[i].y > sample[j].y)
					return 1;
				return 0;
			}
		};

		final QuickSort quickSort = new QuickSort();

		quickSort.sort(sortableT, 0, sample.length);
		//tony
		int tindex1 = 0;
		double t1 = gridInfo.t1;
		for (int lay = 0; lay < gridInfo.layers; lay++){
			int tindex2 = sample.length * (lay + 1) / gridInfo.layers;
			
			double t2 = lay == gridInfo.layers - 1 ? gridInfo.t2 : sample[tindex2 - 1].t;
		
			quickSort.sort(sortableX, tindex1, tindex2);

			int xindex1 = tindex1;
			double x1 = gridInfo.x1;
			for (int col = 0; col < gridInfo.columns; col++) {
				int xindex2 = sample.length * (col + 1) / gridInfo.columns;
	
				// Determine extents for all Prisms in this column
				double x2 = col == gridInfo.columns - 1 ? gridInfo.x2
						: sample[xindex2 - 1].x;
	
				// Sort all points in this column according to its y-coordinate
				quickSort.sort(sortableY, xindex1, xindex2);
	
				// Create Prisms in this column
				double y1 = gridInfo.y1;
				for (int row = 0; row < gridInfo.rows; row++) {
					int yindex2 = xindex1 + (xindex2 - xindex1) * (row + 1)
							/ gridInfo.rows;
					double y2 = row == gridInfo.rows - 1 ? gridInfo.y2
							: sample[yindex2 - 1].y;
	
					Prisms[iPrism++] = new Prism(t1, x1, y1, t2, x2, y2);
					y1 = y2;
				}
	
				xindex1 = xindex2;
				x1 = x2;
			}
		}
		return Prisms;
	}

	/**
	 * An iterator that goes over all elements in the tree in no particular
	 * order
	 * 
	 * @author tonyren, eldawy
	 * 
	 */
	class RTreeIterator implements Iterator<T> {

		/** Current offset in the data stream */
		int offset;

		/** Temporary text that holds one line to deserialize objects */
		Text line;

		/** A stock object to read from stream */
		T _stockObject;

		/** A reader to read lines from the tree */
		LineReader reader;

		RTreeIterator() throws IOException {
			offset = TreeHeaderSize + NodeSize * RTree.this.nodeCount;
			_stockObject = (T) RTree.this.stockObject.clone();
			line = new Text();
			RTree.this.data.seek(offset + RTree.this.treeStartOffset);
			reader = new LineReader(RTree.this.data);
		}

		@Override
		public boolean hasNext() {
			return offset < RTree.this.treeSize;
		}

		@Override
		public T next() {
			try {
				offset += reader.readLine(line);
				_stockObject.fromText(line);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			return _stockObject;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Not supported");
		}
	}

	/**
	 * Skip bytes until the end of line
	 * 
	 * @param bytes
	 * @param startOffset
	 * @return
	 */
	public static int skipToEOL(byte[] bytes, int startOffset) {
		int eol = startOffset;
		while (eol < bytes.length && (bytes[eol] != '\n' && bytes[eol] != '\r'))
			eol++;
		while (eol < bytes.length && (bytes[eol] == '\n' || bytes[eol] == '\r'))
			eol++;
		return eol;
	}

	@Override
	public Iterator<T> iterator() {
		try {
			return new RTreeIterator();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Given a block size, record size and a required tree degree, this function
	 * calculates the maximum number of records that can be stored in this block
	 * taking into consideration the overhead needed by node structure.
	 * 
	 * @param blockSize
	 * @param degree
	 * @param recordSize
	 * @return
	 */
	public static int getBlockCapacity(long blockSize, int degree,
			int recordSize) {
		double a = (double) NodeSize / (degree - 1);
		double ratio = (blockSize + a) / (recordSize + a);
		double break_even_height = Math.log(ratio) / Math.log(degree);
		double h_min = Math.floor(break_even_height);
		double capacity1 = Math.floor(Math.pow(degree, h_min));
		double structure_size = 4 + TreeHeaderSize + a
				* (capacity1 * degree - 1);
		double capacity2 = Math
				.floor((blockSize - structure_size) / recordSize);
		return Math.max((int) capacity1, (int) capacity2);
	}

	/**
	 * Searches the RTree starting from the given start position. This is either
	 * a node number or offset of an element. If it's a node number, it performs
	 * the search in the subtree rooted at this node. If it's an offset number,
	 * it searches only the object found there. It is assumed that the
	 * openQuery() has been called before this function and that endQuery() will
	 * be called afterwards.
	 * 
	 * @param query_mbr
	 * @param output
	 * @param start
	 *            - where to start searching
	 * @param end
	 *            - where to end searching. Only used when start is an offset of
	 *            an object.
	 * @return
	 * @throws IOException
	 */
	protected int searchColumnar(Shape query_shape, ResultCollector<Writable> output,
			int start, int end, String field) throws IOException {
		if (output == null){
			throw new RuntimeException("Output is NULL");
		}
		//build search field
		int fieldOffset = 0;	
		int fieldSize = -1;
		FIELD_TYPE fieldType = FIELD_TYPE.NULL;
		//get fields
		Field[] fields = stockObject.getClass().getDeclaredFields();
		
		for (int i = 0; i < fields.length; i++){
			if (fields[i].getName().equals(field)){
				if ( fields[i].getType().equals(Integer.TYPE)){
					fieldSize =  4;
					fieldType = FIELD_TYPE.Integer;
					
				}
				else if ( fields[i].getType().equals(Long.TYPE)){
					fieldSize =  8;
					fieldType = FIELD_TYPE.Long;
				}
				else if ( fields[i].getType().equals(Double.TYPE)){
					fieldSize =  8;
					fieldType = FIELD_TYPE.Double;
				}
				else{
					//throw new RuntimeException("Unsupported type: " + fields[i].getType());
				}
				break;
			}
			else{
				if ( fields[i].getType().equals(Integer.TYPE)){
					fieldOffset += elementCount * 4;
				}
				else if ( fields[i].getType().equals(Long.TYPE) ||  fields[i].getType().equals(Double.TYPE)){
					fieldOffset += elementCount * 8;
				}
				else{
					//throw new RuntimeException("Unsupported type: " + fields[i].getType());
				}
			}
		}
		
		
		
		Prism query_mbr = query_shape.getMBR();
		int resultSize = 0;
		// Special case for an empty tree
		if (height == 0)
			return 0;

		Stack<Integer> toBeSearched = new Stack<Integer>();
		// Start from the given node
		toBeSearched.push(start);
		if (start >= nodeCount) {
			toBeSearched.push(end);
		}

		Prism node_mbr = new Prism();

		// Holds one data line from tree data
		Text line = new Text2();

		while (!toBeSearched.isEmpty()) {
			int searchNumber = toBeSearched.pop();
			int mbrsToTest = searchNumber == 0 ? 1 : degree;

			if (searchNumber < nodeCount) {
				long nodeOffset = NodeSize * searchNumber;
				structure.seek(nodeOffset);
				int dataOffset = structure.readInt();

				for (int i = 0; i < mbrsToTest; i++) {
					node_mbr.readFields(structure);
					int lastOffset = (searchNumber + i) == nodeCount - 1 ? elementCount - 1
							: structure.readInt();
					if (query_mbr.contains(node_mbr)) {
						// The node is full contained in the query range.
						// Save the time and do full scan for this node
						
						// Checks if this node is the last node in its level
						// This can be easily detected because the next node in
						// the level
						// order traversal will be the first node in the next
						// level
						// which means it will have an offset less than this
						// node
						if (lastOffset <= dataOffset)
							lastOffset = elementCount;
						
						data.seek(treeStartOffset + TreeHeaderSize + nodeCount * NodeSize + elementCount * IndexUnitSize + fieldOffset + dataOffset * fieldSize);
						for (int j = 0; j < lastOffset - dataOffset; j++){
							switch (fieldType){
							case Integer:
								output.collect(new IntWritable(data.readInt()));
								break;
							case Long:
								output.collect(new LongWritable(data.readLong()));
								break;
							case Double:
								output.collect(new DoubleWritable(data.readDouble()));
								break;
							default:
								output.collect(new Point3d(data.readDouble(), data.readDouble(), data.readDouble()));
								break;
							}
							resultSize++;
						}

					} else if (query_mbr.isIntersected(node_mbr)) {
						// Node partially overlaps with query. Go deep under
						// this node
						if (searchNumber < nonLeafNodeCount) {
							// Search child nodes
							toBeSearched.push((searchNumber + i) * degree + 1);
						} else {
							// Search all elements in this node
							//toBeSearched.push(dataOffset);
							// Checks if this node is the last node in its level
							// This can be easily detected because the next node
							// in the level
							// order traversal will be the first node in the
							// next level
							// which means it will have an offset less than this
							// node
							if (lastOffset <= dataOffset)
								lastOffset = elementCount;
							//toBeSearched.push(lastOffset);
							data.seek(treeStartOffset + TreeHeaderSize + nodeCount * NodeSize + dataOffset * IndexUnitSize);
							boolean report[] = new boolean[lastOffset - dataOffset];
							Point3d point = new Point3d();
							for (int j = 0; j < lastOffset - dataOffset; j++){
								point.t = data.readDouble();
								point.x = data.readDouble();
								point.y = data.readDouble();
								if (point.isIntersected(query_shape)){
									report[j] = true;
								}
								else
									report[j] = false;
							}
							data.seek(treeStartOffset + TreeHeaderSize + nodeCount * NodeSize + elementCount * IndexUnitSize + fieldOffset + dataOffset * fieldSize);
							for (int j = 0; j < lastOffset - dataOffset; j++){
								if (report[j]){
									switch (fieldType){
									case Integer:
										output.collect(new IntWritable(data.readInt()));
										break;
									case Long:
										output.collect(new LongWritable(data.readLong()));
										break;
									case Double:
										output.collect(new DoubleWritable(data.readDouble()));
										break;
									default:
										output.collect(new Point3d(data.readDouble(), data.readDouble(), data.readDouble()));
										break;
									}
									resultSize++;
								}
							}
						}
					}
					dataOffset = lastOffset;
				}
			} else {
				LOG.error("searchNumber > nodeCount, something is wrong");
				int firstOffset, lastOffset;
				// Search for data items (records)
				lastOffset = searchNumber;
				firstOffset = toBeSearched.pop();

				data.seek(firstOffset + treeStartOffset);
				LineReader lineReader = new LineReader(data);
				while (firstOffset < lastOffset) {
					firstOffset += lineReader.readLine(line);
					stockObject.fromText(line);
					if (stockObject.isIntersected(query_shape)) {
						resultSize++;
						if (output != null)
							output.collect(stockObject);
					}
				}
			}
		}
		return resultSize;
	}
	
	protected int search(Shape query_shape, ResultCollector<T> output,
			int start, int end) throws IOException {
		Prism query_mbr = query_shape.getMBR();
		int resultSize = 0;
		// Special case for an empty tree
		if (height == 0)
			return 0;

		Stack<Integer> toBeSearched = new Stack<Integer>();
		// Start from the given node
		toBeSearched.push(start);
		if (start >= nodeCount) {
			toBeSearched.push(end);
		}

		Prism node_mbr = new Prism();

		// Holds one data line from tree data
		Text line = new Text2();

		while (!toBeSearched.isEmpty()) {
			int searchNumber = toBeSearched.pop();
			int mbrsToTest = searchNumber == 0 ? 1 : degree;

			if (searchNumber < nodeCount) {
				long nodeOffset = NodeSize * searchNumber;
				structure.seek(nodeOffset);
				int dataOffset = structure.readInt();

				for (int i = 0; i < mbrsToTest; i++) {
					node_mbr.readFields(structure);
					int lastOffset = (searchNumber + i) == nodeCount - 1 ? treeSize
							: structure.readInt();
					if (query_mbr.contains(node_mbr)) {
						// The node is full contained in the query range.
						// Save the time and do full scan for this node
						toBeSearched.push(dataOffset);
						// Checks if this node is the last node in its level
						// This can be easily detected because the next node in
						// the level
						// order traversal will be the first node in the next
						// level
						// which means it will have an offset less than this
						// node
						if (lastOffset <= dataOffset)
							lastOffset = treeSize;
						toBeSearched.push(lastOffset);
					} else if (query_mbr.isIntersected(node_mbr)) {
						// Node partially overlaps with query. Go deep under
						// this node
						if (searchNumber < nonLeafNodeCount) {
							// Search child nodes
							toBeSearched.push((searchNumber + i) * degree + 1);
						} else {
							// Search all elements in this node
							toBeSearched.push(dataOffset);
							// Checks if this node is the last node in its level
							// This can be easily detected because the next node
							// in the level
							// order traversal will be the first node in the
							// next level
							// which means it will have an offset less than this
							// node
							if (lastOffset <= dataOffset)
								lastOffset = treeSize;
							toBeSearched.push(lastOffset);
						}
					}
					dataOffset = lastOffset;
				}
			} else {
				int firstOffset, lastOffset;
				// Search for data items (records)
				lastOffset = searchNumber;
				firstOffset = toBeSearched.pop();

				data.seek(firstOffset + treeStartOffset);
				LineReader lineReader = new LineReader(data);
				while (firstOffset < lastOffset) {
					firstOffset += lineReader.readLine(line);
					stockObject.fromText(line);
					if (stockObject.isIntersected(query_shape)) {
						resultSize++;
						if (output != null)
							output.collect(stockObject);
					}
				}
			}
		}
		return resultSize;
	}
	/**
	 * Performs a range query over this tree using the given query range.
	 * 
	 * @param query
	 *            - The query Prism to use (TODO make it any shape not just
	 *            Prism)
	 * @param output
	 *            - Shapes found are reported to this output. If null, results
	 *            are not reported
	 * @return - Total number of records found
	 */
	public int searchColumnar(Shape query, ResultCollector<Writable> output, String field) {
		int resultCount = 0;

		try {
				resultCount = searchColumnar(query, output, 0, 0, field);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultCount;
	}
	
	public int search(Shape query, ResultCollector<T> output, String field) {
		int resultCount = 0;

		try {
				resultCount = search(query, output, 0, 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultCount;
	}

	/**
	 * k nearest neighbor query Note: Current algorithm is approximate just for
	 * simplicity. Writing an exact algorithm is on our TODO list
	 * 
	 * @param qx
	 * @param qy
	 * @param k
	 * @param output
	 */
	public int knn(final double qt, final double qx, final double qy, int k,
			final ResultCollector2<T, Double> output) {
		double query_area = ((getMBR().x2 - getMBR().x1) * (getMBR().y2 - getMBR().y1))
				* k / getElementCount();
		double query_radius = Math.sqrt(query_area / Math.PI);

		boolean result_correct;
		final Vector<Double> distances = new Vector<Double>();
		final Vector<T> shapes = new Vector<T>();
		// Find results in the range and increase this range if needed to ensure
		// correctness of the answer
		do {
			// Initialize result and query range
			distances.clear();
			shapes.clear();
			Prism queryRange = new Prism();
			queryRange.x1 = qx - query_radius / 2;
			queryRange.y1 = qy - query_radius / 2;
			queryRange.x2 = qx + query_radius / 2;
			queryRange.y2 = qy + query_radius / 2;
			// Retrieve all results in range
			searchColumnar(queryRange, new ResultCollector<Writable>() {
				@Override
				public void collect(Writable shape) {
					distances.add(((T)shape).distanceTo(qt, qx, qy));
					shapes.add((T) ((T) shape).clone());
				}
			}, null);
			if (shapes.size() < k) {
				// Didn't find k elements in range, double the range to get more
				// items
				if (shapes.size() == getElementCount()) {
					// Already returned all possible elements
					result_correct = true;
				} else {
					query_radius *= 2;
					result_correct = false;
				}
			} else {
				// Sort items by distance to get the kth neighbor
				IndexedSortable s = new IndexedSortable() {
					@Override
					public void swap(int i, int j) {
						double temp_distance = distances.elementAt(i);
						distances.set(i, distances.elementAt(j));
						distances.set(j, temp_distance);

						T temp_shape = shapes.elementAt(i);
						shapes.set(i, shapes.elementAt(j));
						shapes.set(j, temp_shape);
					}

					@Override
					public int compare(int i, int j) {
						// Note. Equality is not important to check because
						// items with the
						// same distance can be ordered anyway.
						if (distances.elementAt(i) < distances.elementAt(j))
							return -1;
						return 1;
					}
				};
				IndexedSorter sorter = new QuickSort();
				sorter.sort(s, 0, shapes.size());
				if (distances.elementAt(k - 1) > query_radius) {
					result_correct = false;
					query_radius = distances.elementAt(k);
				} else {
					result_correct = true;
				}
			}
		} while (!result_correct);

		int result_size = Math.min(k, shapes.size());
		if (output != null) {
			for (int i = 0; i < result_size; i++) {
				output.collect(shapes.elementAt(i), distances.elementAt(i));
			}
		}
		return result_size;
	}

	protected static <S1 extends Shape, S2 extends Shape> int spatialJoinMemory(
			final RTree<S1> R, final RTree<S2> S,
			final ResultCollector2<S1, S2> output) throws IOException {
		S1[] rs = (S1[]) Array.newInstance(R.stockObject.getClass(),
				R.getElementCount());
		int i = 0;
		for (S1 r : R)
			rs[i++] = (S1) r.clone();
		if (i != rs.length)
			throw new RuntimeException(i + "!=" + rs.length);

		S2[] ss = (S2[]) Array.newInstance(S.stockObject.getClass(),
				S.getElementCount());
		i = 0;
		for (S2 s : S)
			ss[i++] = (S2) s.clone();
		if (i != ss.length)
			throw new RuntimeException(i + "!=" + ss.length);

		return SpatialAlgorithms.SpatialJoin_planeSweep(rs, ss, output);
	}

	// LRU cache used to avoid deserializing the same records again and again
	static class LruCache<A, B> extends LinkedHashMap<A, B> {
		private static final long serialVersionUID = 702044567572914544L;
		private final int maxEntries;
		private B unusedEntry;

		public LruCache(final int maxEntries) {
			super(maxEntries + 1, 1.0f, true);
			this.maxEntries = maxEntries;
		}

		@Override
		protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
			if (super.size() > maxEntries) {
				unusedEntry = eldest.getValue();
				return true;
			}
			return false;
		}

		public B popUnusedEntry() {
			B temp = unusedEntry;
			unusedEntry = null;
			return temp;
		}
	}

	/**
	 * Performs a spatial join between records in two R-trees
	 * 
	 * @param R
	 * @param S
	 * @param output
	 * @return
	 * @throws IOException
	 */
	protected static <S1 extends Shape, S2 extends Shape> int spatialJoinDisk(
			final RTree<S1> R, final RTree<S2> S,
			final ResultCollector2<S1, S2> output) throws IOException {
		// Reserve locations for nodes MBRs and data offset [start, end)
		final Prism[] r_nodes = new Prism[R.degree];
		for (int i = 0; i < r_nodes.length; i++)
			r_nodes[i] = new Prism();
		final int[] r_data_offset = new int[R.degree + 1];

		final Prism[] s_nodes = new Prism[S.degree];
		for (int i = 0; i < s_nodes.length; i++)
			s_nodes[i] = new Prism();
		final int[] s_data_offset = new int[S.degree + 1];

		PriorityQueue<Long> nodesToJoin = new PriorityQueue<Long>() {
			{
				initialize(R.leafNodeCount + S.leafNodeCount);
			}

			@Override
			protected boolean lessThan(Object a, Object b) {
				return ((Long) a) < ((Long) b);
			}
		};

		nodesToJoin.put(0L);

		LruCache<Integer, Shape[]> r_records_cache = new LruCache<Integer, Shape[]>(
				R.degree * 2);
		LruCache<Integer, Shape[]> s_records_cache = new LruCache<Integer, Shape[]>(
				S.degree * R.degree * 4);

		Text line = new Text2();

		int result_count = 0;

		LineReader r_lr = null, s_lr = null;
		// Last offset read from r and s
		int r_last_offset = 0;
		int s_last_offset = 0;

		while (nodesToJoin.size() > 0) {
			long nodes_to_join = nodesToJoin.pop();
			int r_node = (int) (nodes_to_join >>> 32);
			int s_node = (int) (nodes_to_join & 0xFFFFFFFF);
			// Read all R nodes
			int r_mbrsToTest = r_node == 0 ? 1 : R.degree;
			boolean r_leaf = r_node * R.degree + 1 >= R.nodeCount;

			long nodeOffset = NodeSize * r_node;
			R.structure.seek(nodeOffset);

			for (int i = 0; i < r_mbrsToTest; i++) {
				r_data_offset[i] = R.structure.readInt();
				r_nodes[i].readFields(R.structure);
			}
			r_data_offset[r_mbrsToTest] = (r_node + r_mbrsToTest) == R.nodeCount ? R.treeSize
					: R.structure.readInt();

			// Read all S nodes
			int s_mbrsToTest = s_node == 0 ? 1 : S.degree;
			boolean s_leaf = s_node * S.degree + 1 >= S.nodeCount;

			if (r_leaf != s_leaf) {
				// This case happens when the two trees are of different heights
				if (r_leaf)
					r_mbrsToTest = 1;
				else
					s_mbrsToTest = 1;
			}

			nodeOffset = NodeSize * s_node;
			S.structure.seek(nodeOffset);

			for (int i = 0; i < s_mbrsToTest; i++) {
				s_data_offset[i] = S.structure.readInt();
				s_nodes[i].readFields(S.structure);
			}
			s_data_offset[s_mbrsToTest] = (s_node + s_mbrsToTest) == S.nodeCount ? S.treeSize
					: S.structure.readInt();

			// Find overlapping nodes by Cartesian product
			for (int i = 0; i < r_mbrsToTest; i++) {
				for (int j = 0; j < s_mbrsToTest; j++) {
					if (r_nodes[i].isIntersected(s_nodes[j])) {
						if (r_leaf && s_leaf) {
							// Reached leaf nodes in both trees. Start comparing
							// records
							int r_start_offset = r_data_offset[i];
							int r_end_offset = r_data_offset[i + 1];

							int s_start_offset = s_data_offset[j];
							int s_end_offset = s_data_offset[j + 1];

							// /////////////////////////////////////////////////////////////////
							// Read or retrieve r_records
							Shape[] r_records = r_records_cache
									.get(r_start_offset);
							if (r_records == null) {
								int cache_key = r_start_offset;
								r_records = r_records_cache.popUnusedEntry();
								if (r_records == null) {
									r_records = new Shape[R.degree * 2];
								}

								// Need to read it from stream
								if (r_last_offset != r_start_offset) {
									long seekTo = r_start_offset
											+ R.treeStartOffset;
									R.data.seek(seekTo);
									r_lr = new LineReader(R.data);
								}
								int record_i = 0;
								while (r_start_offset < r_end_offset) {
									r_start_offset += r_lr.readLine(line);
									if (r_records[record_i] == null)
										r_records[record_i] = R.stockObject
												.clone();
									r_records[record_i].fromText(line);
									record_i++;
								}
								r_last_offset = r_start_offset;
								// Nullify other records
								while (record_i < r_records.length)
									r_records[record_i++] = null;
								r_records_cache.put(cache_key, r_records);
							}

							// Read or retrieve s_records
							Shape[] s_records = s_records_cache
									.get(s_start_offset);
							if (s_records == null) {
								int cache_key = s_start_offset;

								// Need to read it from stream
								if (s_lr == null
										|| s_last_offset != s_start_offset) {
									// Need to reposition s_lr (LineReader of S)
									long seekTo = s_start_offset
											+ S.treeStartOffset;
									S.data.seek(seekTo);
									s_lr = new LineReader(S.data);
								}
								s_records = s_records_cache.popUnusedEntry();
								if (s_records == null) {
									s_records = new Shape[S.degree * 2];
								}
								int record_i = 0;
								while (s_start_offset < s_end_offset) {
									s_start_offset += s_lr.readLine(line);
									if (s_records[record_i] == null)
										s_records[record_i] = S.stockObject
												.clone();
									s_records[record_i].fromText(line);
									record_i++;
								}
								// Nullify other records
								while (record_i < s_records.length)
									s_records[record_i++] = null;
								// Put in cache
								s_records_cache.put(cache_key, s_records);
								s_last_offset = s_start_offset;
							}

							// Do Cartesian product between records to find
							// overlapping pairs
							for (int i_r = 0; i_r < r_records.length
									&& r_records[i_r] != null; i_r++) {
								for (int i_s = 0; i_s < s_records.length
										&& s_records[i_s] != null; i_s++) {
									if (r_records[i_r]
											.isIntersected(s_records[i_s])) {
										result_count++;
										if (output != null) {
											output.collect((S1) r_records[i_r],
													(S2) s_records[i_s]);
										}
									}
								}
							}
							// /////////////////////////////////////////////////////////////////

						} else {
							// Add a new pair to node pairs to be tested
							// Go down one level if possible
							int new_r_node, new_s_node;
							if (!r_leaf) {
								new_r_node = (r_node + i) * R.degree + 1;
							} else {
								new_r_node = r_node + i;
							}
							if (!s_leaf) {
								new_s_node = (s_node + j) * S.degree + 1;
							} else {
								new_s_node = s_node + j;
							}
							long new_pair = (((long) new_r_node) << 32)
									| new_s_node;
							nodesToJoin.put(new_pair);
						}
					}
				}
			}
		}
		return result_count;
	}

	public static <S1 extends Shape, S2 extends Shape> int spatialJoin(
			final RTree<S1> R, final RTree<S2> S,
			final ResultCollector2<S1, S2> output) throws IOException {
		if (R.treeStartOffset >= 0 && S.treeStartOffset >= 0) {
			// Both trees are read from disk
			return spatialJoinDisk(R, S, output);
		} else {
			return spatialJoinMemory(R, S, output);
		}
	}

	/**
	 * Calculate the storage overhead required to build an RTree for the given
	 * number of nodes.
	 * 
	 * @return - storage overhead in bytes
	 */
	public static int calculateStorageOverhead(int elementCount, int degree) {
		// Update storage overhead
		int height = Math.max(1,
				(int) Math.ceil(Math.log(elementCount) / Math.log(degree)));
		int leafNodeCount = (int) Math.pow(degree, height - 1);
		if (elementCount <= 2 * leafNodeCount && height > 1) {
			height--;
			leafNodeCount = (int) Math.pow(degree, height - 1);
		}
		int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
		int storage_overhead = 4 + TreeHeaderSize + nodeCount * NodeSize;
		return storage_overhead;
	}

	/**
	 * Find log to the base 2 quickly
	 * 
	 * @param x
	 * @return
	 */
	public static int log2Floor(int x) {
		if (x == 0)
			return -1;
		int pos = 0;
		if ((x & 0xFFFF0000) != 0) {
			pos += 16;
			x >>>= 16;
		}
		if ((x & 0xFF00) != 0) {
			pos += 8;
			x >>>= 8;
		}
		if ((x & 0xF0) != 0) {
			pos += 4;
			x >>>= 4;
		}
		if ((x & 0xC) != 0) {
			pos += 2;
			x >>>= 2;
		}
		if ((x & 0x2) != 0) {
			pos++;
			x >>>= 1;
		}

		return pos;
	}

	public static int powInt(int base, int exponent) {
		int pow = 1;
		while (exponent != 0) {
			if ((exponent & 1) != 0)
				pow *= base;
			exponent >>>= 1;
			base *= base;
		}
		return pow;
	}

	private static final double LogLookupTable[];

	static {
		int count = 100;
		LogLookupTable = new double[count];
		for (int i = 0; i < count; i++) {
			LogLookupTable[i] = Math.log(i);
		}
	}

	public static double fastLog(int x) {
		if (x < LogLookupTable.length) {
			return LogLookupTable[x];
		}
		return Math.log(x);
	}

	public static double fastPow(double a, double b) {
		final long tmp = (long) (9076650 * (a - 1)
				/ (a + 1 + 4 * (Math.sqrt(a))) * b + 1072632447);
		return Double.longBitsToDouble(tmp << 32);
	}

	/**
	 * Find the best (minimum) degree that can index the given number of records
	 * such that the whole tree structure can be stored in the given bytes
	 * available.
	 * 
	 * @param bytesAvailable
	 * @param recordCount
	 * @return
	 */
	public static int findBestDegree(int bytesAvailable, int recordCount) {
		// Maximum number of nodes that can be stored in the bytesAvailable
		int maxNodeCount = (bytesAvailable - TreeHeaderSize) / NodeSize;
		// Calculate maximum possible tree height to store the given record
		// count
		int h_max = log2Floor(recordCount / 2);
		// Minimum height is always 1 (degree = recordCount)
		int h_min = 2;
		// Best degree is the minimum degree
		int d_best = Integer.MAX_VALUE;
		double log_recordcount_e = Math.log(recordCount / 2);
		double log_recordcount_2 = log_recordcount_e / fastLog(2);
		// Find the best height among all possible heights
		for (int h = h_min; h <= h_max; h++) {
			// Find the minimum degree for the given height (h)
			// This approximation is good enough for our case.
			// Not proven but tested with millions of random cases
			int d_min = (int) Math.ceil(fastPow(2.0, log_recordcount_2
					/ (h + 1)));
			// Some heights are invalid, recalculate the height to ensure it's
			// valid
			int h_recalculated = (int) Math.floor(log_recordcount_e
					/ fastLog(d_min));
			if (h != h_recalculated)
				continue;
			int nodeCount = (int) ((powInt(d_min, h + 1) - 1) / (d_min - 1));
			if (nodeCount < maxNodeCount && d_min < d_best)
				d_best = d_min;
		}

		return d_best;
	}

	public static int calculateTreeStorage(int elementCount, int degree) {
		int height = Math.max(1,
				(int) Math.ceil(Math.log(elementCount) / Math.log(degree)));
		int leafNodeCount = (int) Math.pow(degree, height - 1);
		if (elementCount < 2 * leafNodeCount && height > 1) {
			height--;
			leafNodeCount = (int) Math.pow(degree, height - 1);
		}
		int nodeCount = (int) ((Math.pow(degree, height) - 1) / (degree - 1));
		return TreeHeaderSize + nodeCount * NodeSize;
	}
}
