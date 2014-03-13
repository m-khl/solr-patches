package org.apache.lucene.search.join;

/*
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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class TestJoinIndexQuery extends LuceneTestCase {

  final static String idField = "id";
  
  public void testSimple() throws Exception {
    final String toField = "productId";

    List<Document> docs = new ArrayList<>();
    // 0
    Document doc = new Document();
    doc.add(new TextField("description", "random text", Field.Store.NO));
    doc.add(new TextField("name", "name1", Field.Store.NO));
    doc.add(new TextField(idField, "1", Field.Store.YES));
    docs.add(doc);

    // 1
    doc = new Document();
    doc.add(new TextField("price", "10.0", Field.Store.NO));
    doc.add(new TextField(idField, "2", Field.Store.YES));
    doc.add(new TextField(toField, "1", Field.Store.NO));
    docs.add(doc);

    // 2
    doc = new Document();
    doc.add(new TextField("price", "20.0", Field.Store.NO));
    doc.add(new TextField(idField, "3", Field.Store.YES));
    doc.add(new TextField(toField, "1", Field.Store.NO));
    docs.add(doc);

    // 3
    doc = new Document();
    doc.add(new TextField("description", "more random text", Field.Store.NO));
    doc.add(new TextField("name", "name2", Field.Store.NO));
    doc.add(new TextField(idField, "4", Field.Store.YES));
    docs.add(doc);

    // 4
    doc = new Document();
    doc.add(new TextField("price", "10.0", Field.Store.NO));
    doc.add(new TextField(idField, "5", Field.Store.YES));
    doc.add(new TextField(toField, "4", Field.Store.NO));
    docs.add(doc);
    // 5
    doc = new Document();
    doc.add(new TextField("price", "20.0", Field.Store.NO));
    doc.add(new TextField(idField, "6", Field.Store.YES));
    doc.add(new TextField(toField, "4", Field.Store.NO));
    docs.add(doc);
    Collections.shuffle(docs, random());

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    for(Document d:docs){
      w.addDocument(d);
      if(rarely()){
        w.commit();
      }
    }
    indexJoin(w, idField, toField);
    
    IndexSearcher indexSearcher = new IndexSearcher(w.getReader());
    w.close();

    // Search for product
    Query joinQuery =
        JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);

    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "5", "6");

    joinQuery = JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name1")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "2", "3");

    // Search for offer
    joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "5")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertIds(indexSearcher, result, "4");

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private void indexJoin(RandomIndexWriter w, String pkField, String fkField) throws IOException {
    final DirectoryReader reader = w.getReader();
    try{
      final IndexSearcher s = new IndexSearcher(reader);
      final Terms pkTerms = MultiFields.getTerms(reader, pkField);
      final Terms fkTerms = MultiFields.getTerms(reader, fkField);
      
      TermsEnum fks = fkTerms.iterator(null);
      BytesRef pk=null;
      BytesRef fk=null;
      boolean keepFk = false;
      for(TermsEnum pks = pkTerms.iterator(null);(pk=pks.next())!=null;){
        for(;keepFk || (fk=fks.next())!=null;){
          keepFk = false;
          final int cmp = pk.compareTo(fk);
          if(cmp==0){
            marryThem(w,s, pkField,pk, fkField,fk );
            break; // move both
          }else{
            if(cmp<0){
              marryThem(w,s, pkField,pk, fkField,null );
              keepFk=true;
              break; // move pk
            }else{
              marryThem(w,s, pkField,null, fkField,fk );
              // move fk
            }
          }
        }
        // fk is over
        if(fk==null){
          marryThem(w,s, pkField,pk, fkField,null );
        }
      }
      if(pk==null && fk!=null){
        while((fk=fks.next())!=null){
          marryThem(w,s, pkField,null, fkField,fk );
        }
      }
      // reach remain fks
    }finally{
      reader.close();
    }
  }

  private void marryThem(RandomIndexWriter w, IndexSearcher s, String pkField, BytesRef pk,
      String fkField, BytesRef fk) throws IOException {
    if(fk!=null){
      putPkToFk(w,s, pkField, pk, fkField, fk);
    }
    if(pk!=null){
      putPkToFk(w,s, fkField, fk, pkField, pk);
    }
  }

  private void putPkToFk(RandomIndexWriter w, IndexSearcher s, String pkField,
      BytesRef pk, String fkField, BytesRef fk) throws IOException {
    Long refs;
    if(pk==null){ // write missing
      refs = null;
    }else{
      int pkDoc;
      int prev;
      ByteArrayDataOutput out = new ByteArrayDataOutput();
      for( DocsEnum termDocsEnum = MultiFields.getTermDocsEnum(s.getIndexReader(), 
          MultiFields.getLiveDocs(s.getIndexReader()), pkField, pk);
          (pkDoc =termDocsEnum.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS;){
        
      }
      
    }
  }

  static void assertIds(IndexSearcher indexSearcher, TopDocs rez, String ... ids) throws IOException{
    Set<String> act = new HashSet<>();
    for(ScoreDoc sd:rez.scoreDocs){
      final boolean mut = act.add( indexSearcher.doc(sd.doc).getValues(idField)[0]);
      assert mut;
    }
    Set<String> exp = new HashSet<>(Arrays.asList(ids));
    assertEquals(exp, act);
    assertEquals(ids.length, rez.totalHits);
  }
}
