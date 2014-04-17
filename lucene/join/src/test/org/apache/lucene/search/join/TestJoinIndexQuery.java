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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
@SuppressCodecs({"Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestJoinIndexQuery extends LuceneTestCase {

  static final class DVJoinQuery extends Query {
    private final Filter parents;
    private final String joinField;
    private final Filter children;
    
    DVJoinQuery(Filter parents, Filter children, String joinField) {
      this.parents = parents;
      this.joinField = joinField;
      this.children = children;
    }
    
    @Override
    public String toString(String field) {
      return null;
    }
    
    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
     
      return new Weight() {
        
        @Override
        public Scorer scorer(final AtomicReaderContext context, boolean scoreDocsInOrder,
            boolean topScorer, Bits acceptDocs) throws IOException {
          
          final NumericDocValues numericDocValues = context.reader().getNumericDocValues(joinField);
          for(int i=0;i<context.reader().maxDoc();i++){
            final long childRefs = numericDocValues.get(i);
            System.out.println("["+i+"]="+childRefs);
          }
          System.out.println("children:"+children);
          final DocIdSet docIdSet = children.getDocIdSet(context, acceptDocs);
          if(docIdSet!=null){
            int doc;
            for(DocIdSetIterator iter = docIdSet.iterator();(doc=iter.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS;){
              System.out.print(doc);
              System.out.print(", ");
            }
            System.out.println();
          }
          
          final DocIdSet parentDocs = parents.getDocIdSet(context, acceptDocs);
          if(parentDocs==null){
            return null;
          }
          final DocIdSetIterator parentsIter = parentDocs.iterator();
          return new Scorer(this){

            private int docID=-1;

            @Override
            public float score() throws IOException {
              return 0;
            }

            @Override
            public int freq() throws IOException {
              return 0;
            }

            @Override
            public int docID() {
              return docID;
            }

            @Override
            public int nextDoc() throws IOException {
              while((docID = parentsIter.nextDoc())!=NO_MORE_DOCS && !checkChildren(context, docID, joinField, children)){
              }
              return docID;
            }

            private boolean checkChildren(final AtomicReaderContext context,
                int parentDoc, final String joinField, final Filter children)
                throws IOException {
              final NumericDocValues numericDocValues = context.reader().getNumericDocValues(joinField);
              final long childRefs = numericDocValues.get(parentDoc);
              final byte[] bytes = new byte[8];
              final ByteArrayDataOutput o = new ByteArrayDataOutput(bytes);
              o.writeLong(childRefs);
              final ByteArrayDataInput inp = new ByteArrayDataInput(bytes);
              final byte cnt = inp.readByte();
              int prev=0;
              for(int i=0;i<cnt;i++){
                final int referrer = inp.readVInt()+prev;
                prev = referrer;
                assert context.parent.isTopLevel;
                for(AtomicReaderContext arc:context.parent.leaves()){
                  if(referrer-arc.docBase<arc.reader().maxDoc()){
                    final DocIdSet childrenDocIdSet = children.getDocIdSet(arc, arc.reader().getLiveDocs());
                    if(childrenDocIdSet!=null){
                      final DocIdSetIterator disi = childrenDocIdSet.iterator();
                      final int advanced = disi.advance(referrer-arc.docBase);
                      if(advanced==referrer
                          -arc.docBase
                          ){
                        return true;
                      }
                    }
                    break;
                  }
                }
              }
              return false;
            }

            @Override
            public int advance(int target) throws IOException {
              for(docID = parentsIter.advance(target);
                  docID!=NO_MORE_DOCS && !checkChildren(context, docID, joinField, children);
                  docID = parentsIter.nextDoc()){
                
              }
              return docID;
            }

            @Override
            public long cost() {
              return 0;
            }
            
          };
        }
        
        @Override
        public void normalize(float norm, float topLevelBoost) {}
        
        @Override
        public float getValueForNormalization() throws IOException {
          return 0;
        }
        
        @Override
        public Query getQuery() {
          return null;
        }
        
        @Override
        public Explanation explain(AtomicReaderContext context, int doc)
            throws IOException {
          return null;
        }
      };
    }
  }

  final static String idField = "id";
  private Map<Term, String> updTracker = new LinkedHashMap<>();
  
  //@Repeat(iterations=1000)
  public void testSimple() throws Exception {
    final String toField = "productId";
    final String joinField = joinFieldName(idField, toField);

    List<Document> docs = new ArrayList<>();
    // 0
    Document doc = new Document();
    doc.add(new TextField("description", "random text", Field.Store.NO));
    doc.add(new TextField("name", "name1", Field.Store.NO));
    doc.add(new TextField(idField, "1", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 1
    doc = new Document();
    doc.add(new TextField("price", "10.0", Field.Store.NO));
    doc.add(new TextField(idField, "2", Field.Store.YES));
    doc.add(new TextField(toField, "1", Field.Store.NO));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 2
    doc = new Document();
    doc.add(new TextField("price", "20.0", Field.Store.NO));
    doc.add(new TextField(idField, "3", Field.Store.YES));
    doc.add(new TextField(toField, "1", Field.Store.NO));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 3
    doc = new Document();
    doc.add(new TextField("description", "more random text", Field.Store.NO));
    doc.add(new TextField("name", "name2", Field.Store.NO));
    doc.add(new TextField(idField, "4", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 4
    doc = new Document();
    doc.add(new TextField("price", "10.0", Field.Store.NO));
    doc.add(new TextField(idField, "5", Field.Store.YES));
    doc.add(new TextField(toField, "4", Field.Store.NO));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);
    // 5
    doc = new Document();
    doc.add(new TextField("price", "20.0", Field.Store.NO));
    doc.add(new TextField(idField, "6", Field.Store.YES));
    doc.add(new TextField(toField, "4", Field.Store.NO));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);
    Collections.shuffle(docs, random());

    Directory dir = newDirectory();
    System.out.println(dir);
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setInfoStream(System.out);
    // make sure random config doesn't flush on us
    conf.setMaxBufferedDocs(10);
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter w = new IndexWriter(dir, conf);

    for(Document d:docs){
      w.addDocument(d);
      if(usually()){
        w.commit();
      }
    }
    w.commit();
    System.out.println("docs are written");
    DirectoryReader reader = DirectoryReader.open(dir);
    indexJoin(w, reader, idField, toField);
    reader.close();
    System.out.println("flushing num updates");
    w.close();
    reader = DirectoryReader.open(dir);
    
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    //w.close();

    // Search for product
    Query joinQuery =
        JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);

    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "5", "6");
    
    BooleanQuery parentsQ = new BooleanQuery(){{
      add(new MatchAllDocsQuery(), Occur.MUST);
      add(new TermRangeQuery(toField,null,null,true,true), Occur.MUST_NOT);
    }};
    final Filter parents = new CachingWrapperFilter(
        new QueryWrapperFilter(parentsQ));
    final Filter children = new CachingWrapperFilter(new TermFilter(new Term("price", "20.0")));
    Query dvJoinQuery = new DVJoinQuery(parents, children, joinField);
    result = indexSearcher.search(dvJoinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "1", "4");
    
    Query dvPtoChJoinQuery = new DVJoinQuery(new CachingWrapperFilter(new TermRangeFilter(toField,null,null,true,true)),
                                            new TermFilter(new Term("name", "name2")), joinField);
    result = indexSearcher.search(dvPtoChJoinQuery, 10);
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

  private void indexJoin(IndexWriter w, DirectoryReader reader, String pkField, String fkField) throws IOException {
    updTracker.clear();
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
          if(fk==null){
            break;
          }
          final int cmp = pk.compareTo(fk);
          if(cmp==0){
            marryThem(w,s, pkField,pk, fkField,fk );
            keepFk = false;
            break; // move both
          }else{
            if(cmp<0){
              marryThem(w,s, pkField,pk, fkField,null );
              keepFk=true;
              break; // move pk
            }else{
              marryThem(w,s, pkField,null, fkField,fk );
              keepFk = false;
              // move fk
            }
          }
        }
        // fk is over
        if(fk==null){
          marryThem(w,s, pkField,pk, fkField,null );
          keepFk = true;
        }
      }
      if(pk==null && fk!=null){
        while((fk=fks.next())!=null){
          marryThem(w,s, pkField,null, fkField,fk );
        }
      }
      // reach remain fks
    }finally{
      System.out.println(updTracker);
    }
  }

  private void marryThem(IndexWriter w, IndexSearcher s, String pkField, BytesRef pk,
      String fkField, BytesRef fk) throws IOException {
    System.out.println(pkField+":"+(pk!=null? pk.utf8ToString():pk) +" <-> "+fkField+":"+(fk!=null?fk.utf8ToString():fk));
    if(fk!=null){
      putPkToFk(w,s, pkField, pk, fkField, fk, joinFieldName(pkField, fkField));
    }
    if(pk!=null){
      putPkToFk(w,s, fkField, fk, pkField, pk, joinFieldName(pkField, fkField));
    }
  }

  private String joinFieldName(String pkField, String fkField) {
    return fkField+"_to_"+pkField;
  }

  private void putPkToFk(IndexWriter w, IndexSearcher s, String pkField,
      final BytesRef pk, String fkField, final BytesRef fk, String fk_to_pk_field) throws IOException {
    Long refs;
    if(pk==null){ // write missing
      refs = null;
      System.out.println("skipping to write"+pkField+":"+
          (pk==null? null:pk.utf8ToString())+" into "+fkField+":"+
          (fk==null? null:fk.utf8ToString()));
      return;
    }else{
      int pkDoc;
      int prev = 0;
      byte eight[] = new byte[8];
      eight[0] = 0;
      ByteArrayDataOutput out = new ByteArrayDataOutput(eight,1,7);
      for( DocsEnum termDocsEnum = MultiFields.getTermDocsEnum(s.getIndexReader(), 
          MultiFields.getLiveDocs(s.getIndexReader()), pkField, pk);
          (pkDoc =termDocsEnum.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS;){
        out.writeVInt(pkDoc-prev);
        prev=pkDoc;
        eight[0]++;
      }
      final ByteArrayDataInput in = new ByteArrayDataInput(eight);
      refs = in.readLong();
    }
    BytesRef fkTerm = new BytesRef();
    fkTerm.copyBytes(fk);
    final Term referrers = new Term(fkField,fkTerm);
    w.updateNumericDocValue(referrers, fk_to_pk_field, refs);
    
    final String hexString = refs==null? null: Long.toHexString(refs);
    assert updTracker .put(referrers, hexString)==null:" replace "+updTracker.get(referrers)+" to "+hexString+" by "+referrers;
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
