package org.apache.lucene.search.suggest.fst;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.search.suggest.fst.Sort.ByteSequencesReader;
import org.apache.lucene.util.BytesRef;

/**
 * Builds and iterates over sequences stored on disk.
 */
public class ExternalRefSorter implements BytesRefSorter, Closeable {
  private final Sort sort;
  private Sort.ByteSequencesWriter writer;
  private File input;
  private File sorted; 

  /**
   * Will buffer all sequences to a temporary file and then sort (all on-disk).
   */
  public ExternalRefSorter(Sort sort) throws IOException {
    this.sort = sort;
    this.input = File.createTempFile("RefSorter-", ".raw", Sort.defaultTempDir());
    this.writer = new Sort.ByteSequencesWriter(input);
  }

  @Override
  public void add(BytesRef utf8) throws IOException {
    if (writer == null)
      throw new IllegalStateException();
    writer.write(utf8);
  }

  @Override
  public Iterator<BytesRef> iterator() throws IOException {
    if (sorted == null) {
      closeWriter();

      sorted = File.createTempFile("RefSorter-", ".sorted", Sort.defaultTempDir());
      sort.sort(input, sorted);

      input.delete();
      input = null;
    }

    return new ByteSequenceIterator(new Sort.ByteSequencesReader(sorted));
  }

  private void closeWriter() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  /**
   * Removes any written temporary files.
   */
  @Override
  public void close() throws IOException {
    try {
      closeWriter();
    } finally {
      if (input != null) input.delete();
      if (sorted != null) sorted.delete();
    }
  }

  /**
   * Iterate over byte refs in a file.
   */
  class ByteSequenceIterator implements Iterator<BytesRef> {
    private ByteSequencesReader reader;
    private byte[] next;

    public ByteSequenceIterator(ByteSequencesReader reader) throws IOException {
      this.reader = reader;
      this.next = reader.read();
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }
    
    @Override
    public BytesRef next() {
      if (next == null) throw new NoSuchElementException();
      BytesRef r = new BytesRef(next);
      try {
        next = reader.read();
        if (next == null) {
          reader.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return r;
    }

    @Override
    public void remove() { throw new UnsupportedOperationException(); }
  }
}
