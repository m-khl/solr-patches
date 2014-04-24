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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
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
import org.apache.lucene.util.GrowableByteArrayDataOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.schema.StrField;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
@SuppressCodecs({"Lucene40","Lucene41","Lucene42","Lucene45"})
@Repeat(iterations=1000)
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
        public Scorer scorer(AtomicReaderContext context, Bits acceptDocs)
            throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public BulkScorer bulkScorer(final AtomicReaderContext context,
            boolean scoreDocsInOrder, final Bits acceptDocs) throws IOException {
          return new BulkScorer() {
            
            @Override
            public boolean score(LeafCollector collector, int max) throws IOException {
              //dump("parents: ", context, parents,acceptDocs);
              //dump("children: ", context, children,acceptDocs);
              final DocIdSet parentDocs = parents.getDocIdSet(context, acceptDocs);
              if(parentDocs==null){
                return false;
              }
              
              final DocIdSetIterator parentsIter = parentDocs.iterator();
              if(parentsIter==null){
                return false;
              }
              
              collector.setScorer(new FakeScorer());
              int docID;
              while((docID = parentsIter.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS && docID < max){
                if(checkChildren(context, docID, joinField, children)){
                  collector.collect(docID);
                }
              }
              return docID!=DocIdSetIterator.NO_MORE_DOCS;
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
                  if(referrer-arc.docBase<arc.reader().maxDoc() && referrer-arc.docBase>=0){
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
  
  //@Repeat(iterations=1000)
  public void testSimple() throws Exception {
    final String toField = "productId";
    final String joinField = IndexJoiner.joinFieldName(idField, toField);

    List<Document> docs = new ArrayList<>();
    // 0
    Document doc = new Document();
    doc.add(newStringField("description", "random text", Field.Store.YES));
    doc.add(newStringField("name", "name1", Field.Store.YES));
    doc.add(newStringField(idField, "1", Field.Store.YES));
    doc.add(newStringField("type", "parent", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 1
    doc = new Document();
    doc.add(newStringField("price", "10.0", Field.Store.YES));
    doc.add(newStringField(idField, "2", Field.Store.YES));
    doc.add(newStringField(toField, "1", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 2
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "3", Field.Store.YES));
    doc.add(newStringField(toField, "1", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 3
    doc = new Document();
    doc.add(newStringField("description", "more random text", Field.Store.YES));
    doc.add(newStringField("name", "name2", Field.Store.YES));
    doc.add(newStringField(idField, "4", Field.Store.YES));
    doc.add(newStringField("type", "parent", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);

    // 4
    doc = new Document();
    doc.add(newStringField("price", "10.0", Field.Store.YES));
    doc.add(newStringField(idField, "5", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    doc.add(new NumericDocValuesField(joinField, -1));
    docs.add(doc);
    // 5
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "6", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
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
    conf.setMergePolicy(new LogDocMergePolicy());
    IndexWriter w = new IndexWriter(dir, conf);

    for(Document d:docs){
      w.addDocument(d);
      if(usually()){
        w.commit();
      }
    }
    
    for(Document d:docs){
      System.out.println(d);
    }
    
    w.commit();
    System.out.println("docs are written");
    DirectoryReader reader = DirectoryReader.open(dir);
    new IndexJoiner(w, reader, idField, toField).
    indexJoin();
    reader.close();
    System.out.println("flushing num updates");
    w.commit();
    w.close();
    reader = DirectoryReader.open(dir);
    
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    
    //verifyUpdates(indexSearcher);
    
    // Search for product
    Query joinQuery =
        JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);

    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "5", "6");
    
    Query parentsQ = new TermQuery(new Term("type", "parent"));
    final Filter parents = new CachingWrapperFilter(
        new QueryWrapperFilter(parentsQ));
    Filter children = //new CachingWrapperFilter(
        new TermFilter(new Term("price", "20.0")//)
    );
    if(random().nextBoolean()){
      children = new CachingWrapperFilter(children);
    }
    Query dvJoinQuery = new DVJoinQuery(parents, children, joinField);
    result = indexSearcher.search(dvJoinQuery, 10);
    assertIds(indexSearcher, result, "1", "4");
    assertEquals(2, result.totalHits);
    
    Query dvPtoChJoinQuery = new DVJoinQuery(new TermFilter(new Term("type", "child")), //parents,
                                            new TermFilter(new Term("name", "name2")), joinField);
    result = indexSearcher.search(dvPtoChJoinQuery, 10);
    assertIds(indexSearcher, result, "5", "6");
    assertEquals(2, result.totalHits);
    
    joinQuery =  //JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name1")), indexSearcher, ScoreMode.None);
        new DVJoinQuery(new TermFilter(new Term("type", "child")), //parents,
            new TermFilter(new Term("name", "name1")), joinField);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "2", "3");

    // Search for offer
    joinQuery = //JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "5")), indexSearcher, ScoreMode.None);
        new DVJoinQuery(new TermFilter(new Term("type", "parent")), //parents,
            new TermFilter(new Term("id", "5")), joinField);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertIds(indexSearcher, result, "4");

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  
  static class IndexJoiner {
    
    protected final IndexWriter writer;
    private final DirectoryReader reader;
    private final String pkField;
    private final String fkField;
    private final Map<Term,Long> updTracker = new LinkedHashMap<>();
    
    public IndexJoiner(IndexWriter w, DirectoryReader reader, String pkField,
        String fkField) {
      super();
      this.writer = w;
      this.reader = reader;
      this.pkField = pkField;
      this.fkField = fkField;
    }
    
    private void indexJoin() throws IOException {
      updTracker.clear();
      try {
        final Terms pkTerms = MultiFields.getTerms(reader, pkField);
        final Terms fkTerms = MultiFields.getTerms(reader, fkField);
        
        TermsEnum fks = fkTerms.iterator(null);
        BytesRef pk = null;
        BytesRef fk = null;
        boolean keepFk = false;
        for (TermsEnum pks = pkTerms.iterator(null); (pk = pks.next()) != null;) {
          for (; keepFk || (fk = fks.next()) != null;) {
            if (fk == null) {
              break;
            }
            final int cmp = pk.compareTo(fk);
            if (cmp == 0) {
              marryThem(pk, fk);
              keepFk = false;
              break; // move both
            } else {
              if (cmp < 0) {
                marryThem(pk, null);
                keepFk = true;
                break; // move pk
              } else {
                marryThem(null, fk);
                keepFk = false;
                // move fk
              }
            }
          }
          // fk is over
          if (fk == null) {
            marryThem(pk, null);
            keepFk = true;
          }
        }
        if (pk == null && fk != null) {
          while ((fk = fks.next()) != null) {
            marryThem(null, fk);
          }
        }
        // reach remain fks
      } finally {
        // for(Entry<Term, Long> e : updTracker.entrySet()){
        // System.out.print(e.getKey()+" ");
        // System.out.println(e.getValue()==null?"null":Long.toHexString(e.getValue()));
        // }
      }
    }
    
    private void marryThem(BytesRef pk, BytesRef fk) throws IOException {
      // System.out.println(pkField+":"+(pk!=null? pk.utf8ToString():pk)
      // +" <-> "+fkField+":"+(fk!=null?fk.utf8ToString():fk));
      if (fk != null) {
        putPkToFk(pkField, pk, fkField, fk);
      }
      if (pk != null) {
        putPkToFk(fkField, fk, pkField, pk);
      }
    }
    
    protected String joinFieldName() {
      return joinFieldName(pkField, fkField);
    }
    
    private void verifyUpdates(IndexSearcher indexSearcher) throws IOException {
      TreeMap<Integer,String> byDocNum = new TreeMap<>();
      for (Entry<Term,Long> e : updTracker.entrySet()) {
        final TopDocs search = indexSearcher.search(new TermQuery(e.getKey()),
            100);
        for (int d = 0; d < search.totalHits; d++) {
          for (AtomicReaderContext arc : indexSearcher.getTopReaderContext()
              .leaves()) {
            if (search.scoreDocs[d].doc - arc.docBase < arc.reader().maxDoc()
                && search.scoreDocs[d].doc - arc.docBase >= 0) {
              final NumericDocValues numericDocValues = arc.reader()
                  .getNumericDocValues("productId_to_id");
              final Long act = numericDocValues.get(search.scoreDocs[d].doc
                  - arc.docBase);
              assertEquals(
                  "for "
                      + search.scoreDocs[d].doc
                      + " under "
                      + e.getKey()
                      + " I expect "
                      + (e.getValue() == null ? "null" : Long.toHexString(e
                          .getValue())) + " but got "
                      + (act == null ? "null" : Long.toHexString(act)), act,
                  e.getValue());
              assertNull(byDocNum.put(
                  search.scoreDocs[d].doc,
                  (e.getValue() == null ? "null" : Long.toHexString(e
                      .getValue()))));
            }
          }
        }
      }
      System.out.println(byDocNum);
    }
    
    private void putPkToFk(String pkField, final BytesRef pk, String fkField,
        final BytesRef fk) throws IOException {
      Long refs;
      if (pk == null) { // write missing
        refs = null;
        // System.out.println("skipping to write"+pkField+":"+
        // (pk==null? null:pk.utf8ToString())+" into "+fkField+":"+
        // (fk==null? null:fk.utf8ToString()));
        return;
      } else {
        DocsEnum termDocsEnum = MultiFields.getTermDocsEnum(reader,
            MultiFields.getLiveDocs(reader), pkField, pk, DocsEnum.FLAG_NONE);
        
        writeRelation(fkField, fk, termDocsEnum);
      }
    }
    
    protected void writeRelation(String fkField, final BytesRef fk,
        DocsEnum referredDocs) throws IOException {
      Long refs;
      int pkDoc;
      int prev = 0;
      byte eight[] = new byte[8];
      eight[0] = 0;
      ByteArrayDataOutput out = new ByteArrayDataOutput(eight, 1, 7);
      for (; (pkDoc = referredDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS;) {
        out.writeVInt(pkDoc - prev);
        prev = pkDoc;
        eight[0]++;
      }
      final ByteArrayDataInput in = new ByteArrayDataInput(eight);
      refs = in.readLong();
      final Term referrers = copy(fkField, fk);
      writer.updateNumericDocValue(referrers, joinFieldName(), refs);
      
      final String hexString = refs == null ? null : Long.toHexString(refs);
      assert updTracker.put(referrers, refs) == null : " replace "
          + updTracker.get(referrers) + " to " + hexString + " by " + referrers;
    }

    protected Term copy(String fkField, final BytesRef fk) {
      BytesRef fkTerm = new BytesRef();
      fkTerm.copyBytes(fk);
      final Term referrers = new Term(fkField, fkTerm);
      return referrers;
    }
    
    private static String joinFieldName(String pkField, String fkField) {
      return fkField + "_to_" + pkField;
    }
  }

  static class BinaryIndexJoiner extends IndexJoiner{

    public BinaryIndexJoiner(IndexWriter w, DirectoryReader reader,
        String pkField, String fkField) {
      super(w, reader, pkField, fkField);
    }
    
    @Override
    protected void writeRelation(String fkField, BytesRef fk,
        DocsEnum referredDocs) throws IOException {
      
      final Term referrers = copy(fkField, fk);
      final GrowableByteArrayDataOutput out = new GrowableByteArrayDataOutput(10);
      {
        int pkDoc;
        int prev = 0;
        for (; (pkDoc = referredDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS;) {
          out.writeVInt(pkDoc - prev);
          prev = pkDoc;
        }
      }
      writer.updateBinaryDocValue(referrers, joinFieldName(),  new BytesRef(out.bytes, 0, out.length));
      
    }
  }
 
  static void assertIds(IndexSearcher indexSearcher, TopDocs rez, String ... ids) throws IOException{
    Map<String, Integer> act = new HashMap<>();
    for(ScoreDoc sd:rez.scoreDocs){
      final Integer mut = act.put( indexSearcher.doc(sd.doc).getValues(idField)[0], sd.doc);
      assert mut==null;
    }
    Set<String> exp = new HashSet<>(Arrays.asList(ids));
    assertEquals("got "+act,exp, act.keySet());
    assertEquals(ids.length, rez.totalHits);
  }

  private static void dump(String label,
      final AtomicReaderContext context, final Filter aFilter, Bits acceptDocs) throws IOException {
    System.out.println(label+aFilter);
    final DocIdSet docIdSet = aFilter.getDocIdSet(context, acceptDocs);
    if(docIdSet!=null){
      int doc;
      for(DocIdSetIterator iter = docIdSet.iterator();(doc=iter.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS;){
        System.out.print(doc);
        System.out.print(", ");
      }
      System.out.println();
    }
  }

  private static void dump(final AtomicReaderContext context,
      final String joinFieldName) throws IOException {
    final NumericDocValues numericDocValues = context.reader().getNumericDocValues(joinFieldName);
    System.out.println("dumping: "+joinFieldName);
    for(int i=0;i<context.reader().maxDoc();i++){
      final long childRefs = numericDocValues.get(i);
      System.out.print("["+(i+context.docBase)+"]=");
      {
        final byte[] bytes = new byte[8];
        final ByteArrayDataOutput o = new ByteArrayDataOutput(bytes);
        o.writeLong(childRefs);
        final ByteArrayDataInput inp = new ByteArrayDataInput(bytes);
        final byte cnt = inp.readByte();
        int prev=0;
        for(int r=0;r<cnt;r++){
          final int referrer = inp.readVInt()+prev;
          System.out.print(referrer+",");
          prev = referrer;
        }
      }
      System.out.println();
    }
  }
}
