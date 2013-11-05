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
package com.ricemap.spateDB.shape;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.ricemap.spateDB.io.TextSerializerHelper;

/**
 * A class that holds coordinates of a Prism. For predicate test functions
 * (e.g. intersection), the Prism is considered open-ended. This means that
 * the right and top edge are outside the Prism.
 * 
 * @author tonyren, Ahmed Eldawy
 * 
 */
public class Prism implements Shape, WritableComparable<Prism> {
	public double t1;
	public double x1;
	public double y1;
	public double t2;
	public double x2;
	public double y2;

	public Prism() {
		this(0, 0, 0, 0, 0, 0);
	}

	/**
	 * Constructs a new <code>Prism</code>, initialized to match the values
	 * of the specified <code>Prism</code>.
	 * 
	 * @param r
	 *            the <code>Prism</code> from which to copy initial values
	 *            to a newly constructed <code>Prism</code>
	 * @since 1.1
	 */
	public Prism(Prism r) {
		this(r.t1, r.x1, r.y1, r.t2, r.x2, r.y2);
	}

	public Prism(double t1, double x1, double y1, double t2, double x2, double y2) {
		this.set(t1, x1, y1, t2, x2, y2);
	}

	public void set(Shape s) {
		Prism mbr = s.getMBR();
		set(mbr.t1, mbr.x1, mbr.y1,mbr.t2,  mbr.x2, mbr.y2);
	}

	public void set(double t1, double x1, double y1, double t2, double x2, double y2) {
		this.t1 = t1;
		this.x1 = x1;
		this.y1 = y1;
		this.t2 = t2;
		this.x2 = x2;
		this.y2 = y2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(t1);
		out.writeDouble(x1);
		out.writeDouble(y1);
		out.writeDouble(t2);
		out.writeDouble(x2);
		out.writeDouble(y2);
	}

	public void readFields(DataInput in) throws IOException {
		this.t1 = in.readDouble();
		this.x1 = in.readDouble();
		this.y1 = in.readDouble();
		this.t2 = in.readDouble();
		this.x2 = in.readDouble();
		this.y2 = in.readDouble();
	}

	/**
	 * Comparison is done by lexicographic ordering of attributes < x1, y1, x2,
	 * y2>
	 */
	public int compareTo(Shape s) {
		Prism rect2 = (Prism) s;

		// Sort by t1 then x1 then y1
		if (this.t1 < rect2.t1)
			return -1;
		if (this.t1 > rect2.t1)
			return 1;
		if (this.x1 < rect2.x1)
			return -1;
		if (this.x1 > rect2.x1)
			return 1;
		if (this.y1 < rect2.y1)
			return -1;
		if (this.y1 > rect2.y1)
			return 1;

		// Sort by t2 then x2 then y2
		if (this.t2 < rect2.t2)
			return -1;
		if (this.t2 > rect2.t2)
			return 1;
		if (this.x2 < rect2.x2)
			return -1;
		if (this.x2 > rect2.x2)
			return 1;
		if (this.y2 < rect2.y2)
			return -1;
		if (this.y2 > rect2.y2)
			return 1;
		return 0;
	}

	public boolean equals(Object obj) {
		Prism r2 = (Prism) obj;
		boolean result = this.t1 == r2.t1 && this.x1 == r2.x1 && this.y1 == r2.y1 && this.t2 == r2.t2
				&& this.x2 == r2.x2 && this.y2 == r2.y2;
		return result;
	}

	@Override
	public double distanceTo(double pt, double px, double py) {
		return this.getMaxDistanceTo(pt, px, py);
	}

	/**
	 * Maximum distance to the perimeter of the Prism
	 * 
	 * @param px
	 * @param py
	 * @return
	 */
	public double getMaxDistanceTo(double pt, double px, double py) {
		double dt = Math.max(pt - this.t1, this.t2 - pt);
		double dx = Math.max(px - this.x1, this.x2 - px);
		double dy = Math.max(py - this.y1, this.y2 - py);

		return Math.sqrt(dt * dt + dx * dx + dy * dy);
	}

	public double getMinDistanceTo(double pt, double px, double py) {
		if (this.contains(pt, px, py))
			return 0;

		double dt = Math.min(Math.abs(pt - this.t1), Math.abs(this.t2 - pt));
		double dx = Math.min(Math.abs(px - this.x1), Math.abs(this.x2 - px));
		double dy = Math.min(Math.abs(py - this.y1), Math.abs(this.y2 - py));

		if ((pt < this.t1 || pt > this.t2) && (px < this.x1 || px > this.x2) && (py < this.y1 || py > this.y2)) {
			return Math.sqrt(dt * dt + dx * dx + dy * dy);
		}

		return Math.min(dt, Math.min(dx, dy));
	}

	public double getMinDistance(Prism r2) {
		// dx is the horizontal gap between the two Prisms. If their x
		// ranges
		// overlap, dx is zero
		double dt = 0;
		if (r2.t1 > this.t2)
			dt = r2.t1 - this.t2;
		else if (this.t1 > r2.t2)
			dt = this.t1 - r2.t2;
		
		double dx = 0;
		if (r2.x1 > this.x2)
			dx = r2.x1 - this.x2;
		else if (this.x1 > r2.x2)
			dx = this.x1 - r2.x2;

		double dy = 0;
		if (r2.y1 > this.y2)
			dy = r2.y1 - this.y2;
		else if (this.y1 > r2.y2)
			dy = this.y1 - r2.y2;

		// Case 1: Overlapping Prisms
		if (dt == 0 && dx == 0 && dy == 0)
			return 0;

		// Case 2: Not overlapping in any dimension
		return Math.sqrt(dt * dt + dx * dx + dy * dy);
	}

	public double getMaxDistance(Prism r2) {
		double tmin = Math.min(this.t1, r2.t1);
		double tmax = Math.max(this.t2, r2.t2);
		double xmin = Math.min(this.x1, r2.x1);
		double xmax = Math.max(this.x2, r2.x2);
		double ymin = Math.min(this.y1, r2.y1);
		double ymax = Math.max(this.y2, r2.y2);
		double dt = tmax - tmin;
		double dx = xmax - xmin;
		double dy = ymax - ymin;
		return Math.sqrt(dt * dt + dx * dx + dy * dy);
	}

	@Override
	public Prism clone() {
		return new Prism(this);
	}

	@Override
	public Prism getMBR() {
		return this;
	}

	public boolean isIntersected(Shape s) {
		if (s instanceof Point3d) {
			Point3d pt = (Point3d) s;
			return pt.t >= t1 && pt.t <=t2 && pt.x >= x1 && pt.x <= x2 && pt.y >= y1 && pt.y <= y2;
		}
		Prism r = s.getMBR();
		if (r == null)
			return false;
		return (this.t2 > r.t1 && r.t2 > this.t1 && this.x2 > r.x1 && r.x2 > this.x1 && this.y2 > r.y1 && r.y2 > this.y1);
	}

	public Prism getIntersection(Shape s) {
		if (!s.isIntersected(this))
			return null;
		Prism r = s.getMBR();
		double it1 = Math.max(this.t1, r.t1);
		double it2 = Math.min(this.t2, r.t2);
		double ix1 = Math.max(this.x1, r.x1);
		double ix2 = Math.min(this.x2, r.x2);
		double iy1 = Math.max(this.y1, r.y1);
		double iy2 = Math.min(this.y2, r.y2);
		return new Prism(it1, ix1, iy1, it2, ix2, iy2);
	}

	public boolean contains(Point3d p) {
		return contains(p.t, p.x, p.y);
	}

	public boolean contains(double t, double x, double y) {
		return t >= t1 && t < t2 && x >= x1 && x < x2 && y >= y1 && y < y2;
	}

	public boolean contains(Prism r) {
		return contains(r.t1, r.x1, r.y1, r.t2, r.x2, r.y2);
	}

	public Prism union(final Shape s) {
		Prism r = s.getMBR();
		double ut1 = Math.min(t1, r.t1);
		double ut2 = Math.max(t2, r.t2);
		double ux1 = Math.min(x1, r.x1);
		double ux2 = Math.max(x2, r.x2);
		double uy1 = Math.min(y1, r.y1);
		double uy2 = Math.max(y2, r.y2);
		return new Prism(ut1, ux1, uy1, ut2, ux2, uy2);
	}

	public void expand(final Shape s) {
		Prism r = s.getMBR();
		if (r.t1 < this.t1)
			this.t1 = r.t1;
		if (r.t2 > this.t2)
			this.t2 = r.t2;
		if (r.x1 < this.x1)
			this.x1 = r.x1;
		if (r.x2 > this.x2)
			this.x2 = r.x2;
		if (r.y1 < this.y1)
			this.y1 = r.y1;
		if (r.y2 > this.y2)
			this.y2 = r.y2;
	}

	public boolean contains(double rt1, double rx1, double ry1, double rt2, double rx2, double ry2) {
		return rt1 >= t1 && rt2 <= t2 && rx1 >= x1 && rx2 <= x2 && ry2 >= y1 && ry2 <= y2;
	}

	public Point3d getCenterPoint() {
    return new Point3d((t1 + t2) /2, (x1 + x2) /2, (y1 + y2)/2);
  }

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeDouble(t1, text, ',');
		TextSerializerHelper.serializeDouble(x1, text, ',');
		TextSerializerHelper.serializeDouble(y1, text, ',');
		TextSerializerHelper.serializeDouble(t2, text, ',');
		TextSerializerHelper.serializeDouble(x2, text, ',');
		TextSerializerHelper.serializeDouble(y2, text, '\0');
		return text;
	}

	@Override
	public void fromText(Text text) {
		t1 = TextSerializerHelper.consumeDouble(text, ',');
		x1 = TextSerializerHelper.consumeDouble(text, ',');
		y1 = TextSerializerHelper.consumeDouble(text, ',');
		t2 = TextSerializerHelper.consumeDouble(text, ',');
		x2 = TextSerializerHelper.consumeDouble(text, ',');
		y2 = TextSerializerHelper.consumeDouble(text, '\0');
	}

	@Override
	public String toString() {
		return "Prism: (" + t1 + "," + x1 + "," + y1 + ")-(" + t2 + "," + x2 + "," + y2 + ")";
	}

	public boolean isValid() {
		return !Double.isNaN(t1);
	}

	public void invalidate() {
		this.t1 = Double.NaN;
	}
	
	public double getDepth(){
		return t2 - t1;
	}

	public double getHeight() {
		return y2 - y1;
	}

	public double getWidth() {
		return x2 - x1;
	}

	@Override
	public int compareTo(Prism r2) {
		if (this.t1 < r2.t1)
			return -1;
		if (this.t1 > r2.t1)
			return 1;
		if (this.x1 < r2.x1)
			return -1;
		if (this.x1 > r2.x1)
			return 1;
		if (this.y1 < r2.y1)
			return -1;
		if (this.y1 > r2.y1)
			return 1;

		if (this.t2 < r2.t2)
			return -1;
		if (this.t2 > r2.t2)
			return 1;
		if (this.x2 < r2.x2)
			return -1;
		if (this.x2 > r2.x2)
			return 1;
		if (this.y2 < r2.y2)
			return -1;
		if (this.y2 > r2.y2)
			return 1;

		return 0;
	}

	@Override
	public int getSizeofAllFields() {
		return 48;
	}

}
