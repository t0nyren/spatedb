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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ricemap.spateDB.shape.Prism;
import com.ricemap.spateDB.shape.Shape;


/**
 * Performs simple algorithms for spatial data.
 * 
 * @author tonyren, aseldawy
 * 
 */
class PrismNN implements Comparable<PrismNN>   {
	Prism r;
	float dist;
	public PrismNN(Prism r, float dist){
		this.r =r ;
		this.dist =dist;	   
	}

	public int compareTo(PrismNN rect2) {
		float difference = this.dist - rect2.dist;
		if (difference < 0) {
			return -1;
		} 
		if (difference > 0) {
			return 1;
		}
		return 0;

	}

}
class TOPK {
	public TreeSet<PrismNN> heap;
	public int k;

	public TOPK(int k) {
		heap = new TreeSet<PrismNN>();
		this.k = k;
	}

	public void add(Prism r,float dist) {
		heap.add(new PrismNN(r, dist));
		if (this.heap.size() > k) {
			// Remove largest element in set (to keep it of size k)
			this.heap.last();
		}

	}
}

public class SpatialAlgorithms {
  public static final Log LOG = LogFactory.getLog(SpatialAlgorithms.class);
  
  /**
   * @param R
   * @param S
   * @param output
   * @return
   * @throws IOException
   */
  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      List<S1> R, List<S2> S, ResultCollector2<S1, S2> output)
      throws IOException {
    int count = 0;

    Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };

    LOG.info("Joining "+ R.size()+" with "+S.size());
    Collections.sort(R, comparator);
    Collections.sort(S, comparator);

		int i = 0, j = 0;

    while (i < R.size() && j < S.size()) {
      S1 r;
      S2 s;
      if (comparator.compare(R.get(i), S.get(j)) < 0) {
        r = R.get(i);
        int jj = j;

        while ((jj < S.size())
            && ((s = S.get(jj)).getMBR().x1 <= r.getMBR().x2)) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          jj++;
        }
        i++;
      } else {
        s = S.get(j);
        int ii = i;

        while ((ii < R.size())
            && ((r = R.get(ii)).getMBR().x1 <= s.getMBR().x2)) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          ii++;
        }
        j++;
      }
    }
    LOG.info("Finished plane sweep and found "+count+" pairs");
    return count;
	}

  public static<S1 extends Shape, S2 extends Shape> int SpatialJoin_planeSweep(
      final S1[] R, final S2[] S, ResultCollector2<S1, S2> output) {
    int count = 0;

    final Comparator<Shape> comparator = new Comparator<Shape>() {
      @Override
      public int compare(Shape o1, Shape o2) {
        return o1.getMBR().x1 < o2.getMBR().x1 ? -1 : 1;
      }
    };
    
    LOG.info("Joining "+ R.length+" with "+S.length);
    Arrays.sort(R, comparator);
    Arrays.sort(S, comparator);

    int i = 0, j = 0;

    while (i < R.length && j < S.length) {
      S1 r;
      S2 s;
      if (comparator.compare(R[i], S[j]) < 0) {
        r = R[i];
        int jj = j;

        while ((jj < S.length)
            && ((s = S[jj]).getMBR().x1 <= r.getMBR().x2)) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          jj++;
        }
        i++;
      } else {
        s = S[j];
        int ii = i;

        while ((ii < R.length)
            && ((r = R[ii]).getMBR().x1 <= s.getMBR().x2)) {
          if (r.isIntersected(s)) {
            if (output != null)
              output.collect(r, s);
            count++;
          }
          ii++;
        }
        j++;
      }
    }
    LOG.info("Finished plane sweep and found "+count+" pairs");
    return count;
  }
}
