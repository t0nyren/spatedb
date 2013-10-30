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
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;

import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;

/**
 * A very simple spatial index that provides some spatial operations based on an
 * array storage.
 * 
 * @author tonyren, Ahmed Eldawy
 * 
 * @param <S>
 */
public class GlobalIndex<S extends Shape> implements Writable, Iterable<S> {

	/** A stock instance of S used to deserialize objects from disk */
	protected S stockShape;

	/** All underlying shapes in no specific order */
	protected S[] shapes;

	/** Whether partitions in this global index are compact (minimal) or not */
	private boolean compact;

	/** Whether objects are allowed to replicated in different partitions or not */
	private boolean replicated;

	public GlobalIndex() {
	}

	@SuppressWarnings("unchecked")
	public void bulkLoad(S[] shapes) {
		// Create a shallow copy
		this.shapes = shapes.clone();
		// Change it into a deep copy by cloning each instance
		for (int i = 0; i < this.shapes.length; i++) {
			this.shapes[i] = (S) this.shapes[i].clone();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(shapes.length);
		for (int i = 0; i < shapes.length; i++) {
			shapes[i].write(out);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		this.shapes = (S[]) new Shape[length];
		for (int i = 0; i < length; i++) {
			this.shapes[i] = (S) stockShape.clone();
			this.shapes[i].readFields(in);
		}
	}

	public int rangeQuery(Shape queryRange, ResultCollector<S> output) {
		int result_count = 0;
		for (S shape : shapes) {
			if (shape.isIntersected(queryRange)) {
				result_count++;
				if (output != null) {
					output.collect(shape);
				}
			}
		}
		return result_count;
	}

	public static <S1 extends Shape, S2 extends Shape> int spatialJoin(
			GlobalIndex<S1> s1, GlobalIndex<S2> s2,
			final ResultCollector2<S1, S2> output) {
		return SpatialAlgorithms.SpatialJoin_planeSweep(s1.shapes, s2.shapes,
				output);
	}

	/**
	 * A simple iterator over all shapes in this index
	 * 
	 * @author tonyren, eldawy
	 * 
	 */
	class SimpleIterator implements Iterator<S> {

		/** Current index */
		int i = 0;

		@Override
		public boolean hasNext() {
			return i < shapes.length;
		}

		@Override
		public S next() {
			return shapes[i++];
		}

		@Override
		public void remove() {
			throw new RuntimeException("Not implemented");
		}

	}

	@Override
	public Iterator<S> iterator() {
		return new SimpleIterator();
	}

	/**
	 * Number of objects stored in the index
	 * 
	 * @return
	 */
	public int size() {
		return shapes.length;
	}

	/**
	 * Returns the minimal bounding Prism of all objects in the index. If the
	 * index is empty, <code>null</code> is returned.
	 * 
	 * @return - The MBR of all objects or <code>null</code> if empty
	 */
	public Prism getMBR() {
		double t1, x1, y1, t2, x2, y2;
		Iterator<S> i = this.iterator();
		if (!i.hasNext())
			return null;
		Prism mbr = i.next().getMBR();
		t1 = mbr.t1;
		x1 = mbr.x1;
		t2 = mbr.t2;
		y1 = mbr.y1;
		x2 = mbr.x2;
		y2 = mbr.y2;
		while (i.hasNext()) {
			mbr = i.next().getMBR();
			if (mbr.t1 < t1)
				t1 = mbr.t1;
			if (mbr.x1 < x1)
				x1 = mbr.x1;
			if (mbr.y1 < y1)
				y1 = mbr.y1;
			if (mbr.x2 > x2)
				t2 = mbr.t2;
			if (mbr.x2 > x2)
				x2 = mbr.x2;
			if (mbr.y2 > y2)
				y2 = mbr.y2;
		}
		return new Prism(t1, x1, y1, t2, x2, y2);
	}

	public int knn(final double qt, final double qx, final double qy, int k,
			ResultCollector2<S, Double> output) {
		double query_area = ((getMBR().t2 - getMBR().t1) * (getMBR().x2 - getMBR().x1) * (getMBR().y2 - getMBR().y1))
				* k / size();
		double query_radius = Math.sqrt(query_area / Math.PI);

		boolean result_correct;
		final Vector<Double> distances = new Vector<Double>();
		final Vector<S> shapes = new Vector<S>();
		// Find results in the range and increase this range if needed to ensure
		// correctness of the answer
		do {
			// Initialize result and query range
			distances.clear();
			shapes.clear();
			Prism queryRange = new Prism();
			queryRange.t1 = qt - query_radius / 2;
			queryRange.x1 = qx - query_radius / 2;
			queryRange.y1 = qy - query_radius / 2;
			queryRange.t2 = qt + query_radius / 2;
			queryRange.x2 = qx + query_radius / 2;
			queryRange.y2 = qy + query_radius / 2;
			// Retrieve all results in range
			rangeQuery(queryRange, new ResultCollector<S>() {
				@Override
				public void collect(S shape) {
					distances.add(shape.distanceTo(qt, qx, qy));
					shapes.add((S) shape.clone());
				}
			});
			if (shapes.size() <= k) {
				// Didn't find k elements in range, double the range to get more
				// items
				if (shapes.size() == size() || shapes.size() == k) {
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

						S temp_shape = shapes.elementAt(i);
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

	/**
	 * Returns true if the partitions are compact (minimal) around its contents
	 * 
	 * @return
	 */
	public boolean isCompact() {
		return this.compact;
	}

	public void setCompact(boolean compact) {
		this.compact = compact;
	}

	public void setReplicated(boolean r) {
		this.replicated = r;
	}

	public boolean isReplicated() {
		return replicated;
	}
}
