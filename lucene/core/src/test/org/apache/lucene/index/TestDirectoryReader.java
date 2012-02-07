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
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestDirectoryReader extends LuceneTestCase {
  protected Directory dir;
  private Document doc1;
  private Document doc2;
  protected SegmentReader [] readers = new SegmentReader[2];
  protected SegmentInfos sis;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = createDirectory();
    doc1 = new Document();
    doc2 = new Document();
    DocHelper.setupDoc(doc1);
    DocHelper.setupDoc(doc2);
    DocHelper.writeDoc(random, dir, doc1);
    DocHelper.writeDoc(random, dir, doc2);
    sis = new SegmentInfos();
    sis.read(dir);
  }
  
  @Override
  public void tearDown() throws Exception {
    if (readers[0] != null) readers[0].close();
    if (readers[1] != null) readers[1].close();
    dir.close();
    super.tearDown();
  }
  
  protected Directory createDirectory() throws IOException {
    return newDirectory();
  }

  protected IndexReader openReader() throws IOException {
    IndexReader reader;
    reader = IndexReader.open(dir);
    assertTrue(reader instanceof DirectoryReader);

    assertTrue(dir != null);
    assertTrue(sis != null);
    assertTrue(reader != null);
    
    return reader;
  }

  public void testDocument() throws IOException {
    sis.read(dir);
    IndexReader reader = openReader();
    assertTrue(reader != null);
    Document newDoc1 = reader.document(0);
    assertTrue(newDoc1 != null);
    assertTrue(DocHelper.numFields(newDoc1) == DocHelper.numFields(doc1) - DocHelper.unstored.size());
    Document newDoc2 = reader.document(1);
    assertTrue(newDoc2 != null);
    assertTrue(DocHelper.numFields(newDoc2) == DocHelper.numFields(doc2) - DocHelper.unstored.size());
    Terms vector = reader.getTermVectors(0).terms(DocHelper.TEXT_FIELD_2_KEY);
    assertNotNull(vector);
    // TODO: pretty sure this check makes zero sense TestSegmentReader.checkNorms(reader);
    reader.close();
  }
        
  public void testIsCurrent() throws IOException {
    Directory ramDir=newDirectory();
    addDoc(random, ramDir, "test foo", true);
    DirectoryReader reader = DirectoryReader.open(ramDir);
    assertTrue(reader.isCurrent());   // just opened, must be current
    addDoc(random, ramDir, "more text", false);
    assertFalse(reader.isCurrent());   // has been modified, not current anymore
    reader.close();
    ramDir.close();
  }

  public void testMultiTermDocs() throws IOException {
    Directory ramDir1=newDirectory();
    addDoc(random, ramDir1, "test foo", true);
    Directory ramDir2=newDirectory();
    addDoc(random, ramDir2, "test blah", true);
    Directory ramDir3=newDirectory();
    addDoc(random, ramDir3, "test wow", true);

    IndexReader[] readers1 = new IndexReader[]{IndexReader.open(ramDir1), IndexReader.open(ramDir3)};
    IndexReader[] readers2 = new IndexReader[]{IndexReader.open(ramDir1), IndexReader.open(ramDir2), IndexReader.open(ramDir3)};
    MultiReader mr2 = new MultiReader(readers1);
    MultiReader mr3 = new MultiReader(readers2);

    // test mixing up TermDocs and TermEnums from different readers.
    TermsEnum te2 = MultiFields.getTerms(mr2, "body").iterator(null);
    te2.seekCeil(new BytesRef("wow"));
    DocsEnum td = _TestUtil.docs(random, mr2,
                                 "body",
                                 te2.term(),
                                 MultiFields.getLiveDocs(mr2),
                                 null,
                                 false);

    TermsEnum te3 = MultiFields.getTerms(mr3, "body").iterator(null);
    te3.seekCeil(new BytesRef("wow"));
    td = _TestUtil.docs(random, te3, MultiFields.getLiveDocs(mr3),
                        td,
                        false);
    
    int ret = 0;

    // This should blow up if we forget to check that the TermEnum is from the same
    // reader as the TermDocs.
    while (td.nextDoc() != td.NO_MORE_DOCS) ret += td.docID();

    // really a dummy assert to ensure that we got some docs and to ensure that
    // nothing is eliminated by hotspot
    assertTrue(ret > 0);
    readers1[0].close();
    readers1[1].close();
    readers2[0].close();
    readers2[1].close();
    readers2[2].close();
    ramDir1.close();
    ramDir2.close();
    ramDir3.close();
  }

  private void addDoc(Random random, Directory ramDir1, String s, boolean create) throws IOException {
    IndexWriter iw = new IndexWriter(ramDir1, newIndexWriterConfig( 
        TEST_VERSION_CURRENT, 
        new MockAnalyzer(random)).setOpenMode(
        create ? OpenMode.CREATE : OpenMode.APPEND));
    Document doc = new Document();
    doc.add(newField("body", s, TextField.TYPE_UNSTORED));
    iw.addDocument(doc);
    iw.close();
  }
}
