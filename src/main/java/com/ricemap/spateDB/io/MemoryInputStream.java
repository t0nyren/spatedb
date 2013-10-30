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
package com.ricemap.spateDB.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class MemoryInputStream extends ByteArrayInputStream
implements Seekable, PositionedReadable {

  int originalOffset;
  
  public MemoryInputStream(byte[] buf, int offset, int length) {
    super(buf, offset, length);
    originalOffset = offset;
  }

  public MemoryInputStream(byte[] buf) {
    super(buf);
  }

  public long getPos() {
    return pos - originalOffset;
  }

  @Override
  public void seek(long pos) throws IOException {
    this.mark = originalOffset;
    this.reset();
    this.skip(pos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    System.arraycopy(buf, (int)(originalOffset+position), buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
