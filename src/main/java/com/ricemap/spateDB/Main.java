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
package com.ricemap.spateDB;

import org.apache.hadoop.util.ProgramDriver;

import com.ricemap.spateDB.operations.FileMBR;
import com.ricemap.spateDB.operations.Plot;
import com.ricemap.spateDB.operations.RangeQuery;
import com.ricemap.spateDB.operations.Repartition;
import com.ricemap.spateDB.operations.Sampler;
import com.ricemap.spateDB.util.RandomSpatialGenerator;
import com.ricemap.spateDB.util.ReadFile;

/**
 * The main entry point to all queries.
 * @author tonyren, eldawy
 *
 */
public class Main {
  
  public static void main(String[] args) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      
      pgd.addClass("rangequery", RangeQuery.class,
          "Finds all objects in the query range given by a Prism");
      pgd.addClass("index", Repartition.class,
          "Builds an index on an input file");
      pgd.addClass("mbr", FileMBR.class,
          "Finds the minimal bounding Prism of an input file");
      pgd.addClass("readfile", ReadFile.class,
          "Retrieve some information about the global index of a file");

      pgd.addClass("sample", Sampler.class,
          "Reads a random sample from the input file");

      pgd.addClass("generate", RandomSpatialGenerator.class,
          "Generates a random file containing spatial data");


      pgd.addClass("plot", Plot.class,
          "Plots a file to an image");

      

      pgd.driver(args);
      
      // Success
      exitCode = 0;
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
