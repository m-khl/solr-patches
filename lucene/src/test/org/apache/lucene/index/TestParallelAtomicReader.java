package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestParallelAtomicReader extends LuceneTestCase {

  private IndexSearcher parallel, single;
  private Directory dir, dir1, dir2;

  public void testQueries() throws Exception {
    single = single(random);
    parallel = parallel(random);
    
    queryTest(new TermQuery(new Term("f1", "v1")));
    queryTest(new TermQuery(new Term("f1", "v2")));
    queryTest(new TermQuery(new Term("f2", "v1")));
    queryTest(new TermQuery(new Term("f2", "v2")));
    queryTest(new TermQuery(new Term("f3", "v1")));
    queryTest(new TermQuery(new Term("f3", "v2")));
    queryTest(new TermQuery(new Term("f4", "v1")));
    queryTest(new TermQuery(new Term("f4", "v2")));

    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(new TermQuery(new Term("f1", "v1")), Occur.MUST);
    bq1.add(new TermQuery(new Term("f4", "v1")), Occur.MUST);
    queryTest(bq1);
    
    single.getIndexReader().close(); single = null;
    parallel.getIndexReader().close(); parallel = null;
    dir.close(); dir = null;
    dir1.close(); dir1 = null;
    dir2.close(); dir2 = null;
  }

  public void testFieldNames() throws Exception {
    Directory dir1 = getDir1(random);
    Directory dir2 = getDir2(random);
    ParallelAtomicReader pr = new ParallelAtomicReader.Builder()
      .add(SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1)))
      .add(SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir2)))
      .build();
    FieldInfos fieldInfos = pr.getFieldInfos();
    assertEquals(4, fieldInfos.size());
    assertNotNull(fieldInfos.fieldInfo("f1"));
    assertNotNull(fieldInfos.fieldInfo("f2"));
    assertNotNull(fieldInfos.fieldInfo("f3"));
    assertNotNull(fieldInfos.fieldInfo("f4"));
    pr.close();
    dir1.close();
    dir2.close();
  }
  
  public void testRefCounts1() throws IOException {
    Directory dir1 = getDir1(random);
    Directory dir2 = getDir2(random);
    AtomicReader ir1, ir2;
    // close subreaders, ParallelReader will not change refCounts, but close on its own close
    ParallelAtomicReader pr = new ParallelAtomicReader.Builder(true)
      .add(ir1 = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1)))
      .add(ir2 = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir2)))
      .build();
    // check RefCounts
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    pr.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();    
  }
  
  public void testRefCounts2() throws IOException {
    Directory dir1 = getDir1(random);
    Directory dir2 = getDir2(random);
    AtomicReader ir1, ir2;
    // don't close subreaders, so ParallelReader will increment refcounts
    ParallelAtomicReader pr = new ParallelAtomicReader.Builder(false)
      .add(ir1 = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1)))
      .add(ir2 = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir2)))
      .build();
    // check RefCounts
    assertEquals(2, ir1.getRefCount());
    assertEquals(2, ir2.getRefCount());
    pr.close();
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();    
  }
  
  public void testIncompatibleIndexes() throws IOException {
    // two documents:
    Directory dir1 = getDir1(random);

    // one document only:
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d3 = new Document();

    d3.add(newField("f3", "v1", TextField.TYPE_STORED));
    w2.addDocument(d3);
    w2.close();
    
    AtomicReader ir1 = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1)),
        ir2 = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir2));;
    ParallelAtomicReader.Builder builder = new ParallelAtomicReader.Builder(false).add(ir1);
    
    try {
      builder.add(ir2);
      fail("didn't get exptected exception: indexes don't have same number of documents");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    ParallelAtomicReader pr = builder.build();
    // check RefCounts
    assertEquals(2, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    pr.close();
    assertEquals(1, ir1.getRefCount());
    assertEquals(1, ir2.getRefCount());
    ir1.close();
    ir2.close();
    assertEquals(0, ir1.getRefCount());
    assertEquals(0, ir2.getRefCount());
    dir1.close();
    dir2.close();
  }
  
  public void testignoreStoredFields() throws IOException {
    Directory dir1 = getDir1(random);
    Directory dir2 = getDir2(random);
    ParallelAtomicReader pr = new ParallelAtomicReader.Builder()
      .add(SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1)), false)
      .add(SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir2)), true)
      .build();
    assertEquals("v1", pr.document(0).get("f1"));
    assertEquals("v1", pr.document(0).get("f2"));
    assertNull(pr.document(0).get("f3"));
    assertNull(pr.document(0).get("f4"));
    pr.close();
    dir1.close();
    dir2.close();
  }
  
  private void queryTest(Query query) throws IOException {
    ScoreDoc[] parallelHits = parallel.search(query, null, 1000).scoreDocs;
    ScoreDoc[] singleHits = single.search(query, null, 1000).scoreDocs;
    assertEquals(parallelHits.length, singleHits.length);
    for(int i = 0; i < parallelHits.length; i++) {
      assertEquals(parallelHits[i].score, singleHits[i].score, 0.001f);
      Document docParallel = parallel.doc(parallelHits[i].doc);
      Document docSingle = single.doc(singleHits[i].doc);
      assertEquals(docParallel.get("f1"), docSingle.get("f1"));
      assertEquals(docParallel.get("f2"), docSingle.get("f2"));
      assertEquals(docParallel.get("f3"), docSingle.get("f3"));
      assertEquals(docParallel.get("f4"), docSingle.get("f4"));
    }
  }

  // Fields 1-4 indexed together:
  private IndexSearcher single(Random random) throws IOException {
    dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d1 = new Document();
    d1.add(newField("f1", "v1", TextField.TYPE_STORED));
    d1.add(newField("f2", "v1", TextField.TYPE_STORED));
    d1.add(newField("f3", "v1", TextField.TYPE_STORED));
    d1.add(newField("f4", "v1", TextField.TYPE_STORED));
    w.addDocument(d1);
    Document d2 = new Document();
    d2.add(newField("f1", "v2", TextField.TYPE_STORED));
    d2.add(newField("f2", "v2", TextField.TYPE_STORED));
    d2.add(newField("f3", "v2", TextField.TYPE_STORED));
    d2.add(newField("f4", "v2", TextField.TYPE_STORED));
    w.addDocument(d2);
    w.close();

    DirectoryReader ir = DirectoryReader.open(dir);
    return newSearcher(ir);
  }

  // Fields 1 & 2 in one index, 3 & 4 in other, with ParallelReader:
  private IndexSearcher parallel(Random random) throws IOException {
    dir1 = getDir1(random);
    dir2 = getDir2(random);
    ParallelAtomicReader pr = new ParallelAtomicReader.Builder()
      .add(SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir1)))
      .add(SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir2)))
      .build();
    return newSearcher(pr);
  }

  private Directory getDir1(Random random) throws IOException {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d1 = new Document();
    d1.add(newField("f1", "v1", TextField.TYPE_STORED));
    d1.add(newField("f2", "v1", TextField.TYPE_STORED));
    w1.addDocument(d1);
    Document d2 = new Document();
    d2.add(newField("f1", "v2", TextField.TYPE_STORED));
    d2.add(newField("f2", "v2", TextField.TYPE_STORED));
    w1.addDocument(d2);
    w1.close();
    return dir1;
  }

  private Directory getDir2(Random random) throws IOException {
    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document d3 = new Document();
    d3.add(newField("f3", "v1", TextField.TYPE_STORED));
    d3.add(newField("f4", "v1", TextField.TYPE_STORED));
    w2.addDocument(d3);
    Document d4 = new Document();
    d4.add(newField("f3", "v2", TextField.TYPE_STORED));
    d4.add(newField("f4", "v2", TextField.TYPE_STORED));
    w2.addDocument(d4);
    w2.close();
    return dir2;
  }

}
