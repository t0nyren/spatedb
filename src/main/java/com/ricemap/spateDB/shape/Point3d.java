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

import com.ricemap.spateDB.io.TextSerializerHelper;

/**
 * A class that holds coordinates of a point.
 * 
 * @author tonyren, aseldawy
 * 
 */
public class Point3d implements Shape, Comparable<Point3d> {
	public double t;
	public double x;
	public double y;

	public Point3d() {
		this(0, 0, 0);
	}

	public Point3d(double t, double x, double y) {
		set(t, x, y);
	}

	public void set(double t, double x, double y) {
		this.x = x;
		this.y = y;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(t);
		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {
		this.t = in.readDouble();
		this.x = in.readDouble();
		this.y = in.readDouble();
	}

	public int compareTo(Shape s) {
		Point3d pt2 = (Point3d) s;

		// Sort by id
		double difference = this.t - pt2.t;
		if (difference == 0)
			difference = this.x - pt2.x;
		else if (difference == 0) {
			difference = this.y - pt2.y;
		}
		if (difference == 0)
			return 0;
		return difference > 0 ? 1 : -1;
	}

	public boolean equals(Object obj) {
		Point3d r2 = (Point3d) obj;
		return this.t == r2.t && this.x == r2.x && this.y == r2.y;
	}

	public double distanceTo(Point3d s) {
		double dt = s.t - this.t;
		double dx = s.x - this.x;
		double dy = s.y - this.y;
		return Math.sqrt(dt * dt + dx * dx + dy * dy);
	}

	@Override
	public Point3d clone() {
		return new Point3d(this.t, this.x, this.y);
	}

	@Override
	public Prism getMBR() {
		return new Prism(t, x, y, t, x, y);
	}

	@Override
	public double distanceTo(double pt, double px, double py) {
		double dt = t - pt;
		double dx = x - px;
		double dy = y - py;
		return Math.sqrt(dt * dt + dx * dx + dy * dy);
	}

	public Shape getIntersection(Shape s) {
		return getMBR().getIntersection(s);
	}

	@Override
	public boolean isIntersected(Shape s) {
		return getMBR().isIntersected(s);
	}

	@Override
	public String toString() {
		return "Point: (" + t + "," + x + "," + y + ")";
	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeDouble(t, text, ',');
		TextSerializerHelper.serializeDouble(x, text, ',');
		TextSerializerHelper.serializeDouble(y, text, '\0');
		return text;
	}

	@Override
	public void fromText(Text text) {
		t = TextSerializerHelper.consumeDouble(text, ',');
		x = TextSerializerHelper.consumeDouble(text, ',');
		y = TextSerializerHelper.consumeDouble(text, '\0');
	}

	@Override
	public int compareTo(Point3d o) {
		if (t < o.t)
			return -1;
		if (t > o.t)
			return 1;
		if (x < o.x)
			return -1;
		if (x > o.x)
			return 1;
		if (y < o.y)
			return -1;
		if (y > o.y)
			return 1;
		return 0;
	}

}
