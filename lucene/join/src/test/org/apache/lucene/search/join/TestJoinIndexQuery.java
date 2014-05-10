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
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.GrowableByteArrayDataOutput;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.fst.NoOutputs;
import org.apache.solr.schema.StrField;
import org.junit.Ignore;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
@SuppressCodecs({"Lucene40","Lucene41","Lucene42","Lucene45"})
//@Repeat(iterations=1000)
public class TestJoinIndexQuery extends LuceneTestCase {

  static class Relation extends DocIdSetIterator {
    
    int curDoc=-1;
    final int parentID;
    
    private ByteArrayDataInput seq;
    
    Relation(int parentID, BytesRef b){
      seq = new ByteArrayDataInput(b.bytes, b.offset, b.length);
      this.parentID = parentID;
    }
    
    @Override
    public int docID() {
      return curDoc;
    }
    
    @Override
    public int nextDoc() throws IOException {
      if(seq.eof()){
        return curDoc = NO_MORE_DOCS;
      }else{
        return curDoc = curDoc==-1 ? seq.readVInt() : curDoc + seq.readVInt();
      }
    }
    
    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }
    @Override
    public long cost() {
      return 0;
    }
  }

  static abstract  class DVJoinQuery extends Query {
    abstract class DVJoinWeight extends Weight {
      abstract class DVBulkScorer extends BulkScorer {
        protected final AtomicReaderContext context;
        protected final Bits acceptDocs;
        
        DVBulkScorer(AtomicReaderContext context, Bits acceptDocs) {
          this.context = context;
          this.acceptDocs = acceptDocs;
        }
        
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
        
        protected abstract boolean checkChildren(final AtomicReaderContext context,
            int parentDoc, final String joinField, final Filter children)
            throws IOException ;
      }
      
      @Override
      public Scorer scorer(AtomicReaderContext context, Bits acceptDocs)
          throws IOException {
        throw new UnsupportedOperationException();
      }
      
      @Override
      public abstract BulkScorer bulkScorer(final AtomicReaderContext context,
          boolean scoreDocsInOrder, final Bits acceptDocs) throws IOException ;
      
      @Override
      public void normalize(float norm, float topLevelBoost) {}
      
      @Override
      public float getValueForNormalization() throws IOException {
        return 0;
      }
      
      @Override
      public Query getQuery() {
        return DVJoinQuery.this;
      }
      
      @Override
      public Explanation explain(AtomicReaderContext context, int doc)
          throws IOException {
        return null;
      }
    }

    protected final Filter parents;
    protected final String joinField;
    protected final Filter children;
    
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
    public abstract Weight createWeight(IndexSearcher searcher) throws IOException;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((children == null) ? 0 : children.hashCode());
      result = prime * result
          + ((joinField == null) ? 0 : joinField.hashCode());
      result = prime * result + ((parents == null) ? 0 : parents.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      DVJoinQuery other = (DVJoinQuery) obj;
      if (children == null) {
        if (other.children != null) return false;
      } else if (!children.equals(other.children)) return false;
      if (joinField == null) {
        if (other.joinField != null) return false;
      } else if (!joinField.equals(other.joinField)) return false;
      if (parents == null) {
        if (other.parents != null) return false;
      } else if (!parents.equals(other.parents)) return false;
      return true;
    }
  }
  
  static final class BinDVJoinQuery extends DVJoinQuery {

    public BinDVJoinQuery(Filter parents, Filter children, String joinField) {
      super(parents, children, joinField);
    }
    
    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
      return new DVJoinWeight(){
        @Override
        public BulkScorer bulkScorer(AtomicReaderContext context,
            boolean scoreDocsInOrder, Bits acceptDocs) throws IOException {
          return new DVBulkScorer(context, acceptDocs){
            @Override
            protected boolean checkChildren(AtomicReaderContext context,
                int parentDoc, String joinField, Filter children)
                throws IOException {
              final BinaryDocValues numericDocValues = context.reader().getBinaryDocValues(joinField);
              final BytesRef bytes = new BytesRef();
              numericDocValues.get(parentDoc, bytes);
              
              final ByteArrayDataInput inp = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
              
              int prev=0;
              for(;!inp.eof();){
                final int referrer = inp.readVInt()+prev;
                prev = referrer;
                final List<AtomicReaderContext> leaves = ReaderUtil.getTopLevelContext(context).leaves();
                // TODO use first docs array method instead. reuse arc between referrers 
                final AtomicReaderContext arc = leaves.get(ReaderUtil.subIndex(referrer, leaves));
                  assert (referrer-arc.docBase<arc.reader().maxDoc() && referrer-arc.docBase>=0);
                  
                    final DocIdSet childrenDocIdSet = children.getDocIdSet(arc, arc.reader().getLiveDocs());
                    if(childrenDocIdSet!=null){
                      final DocIdSetIterator disi = childrenDocIdSet.iterator();
                      if(disi!=null){
                        final int advanced = disi.advance(referrer-arc.docBase);
                        if(advanced==referrer
                            -arc.docBase
                            ){
                          return true;
                        }
                      }
                }
              }
              return false;
            }
          };
        }
      };
    }
  }

  static final class BulkDVJoinQuery extends DVJoinQuery{

    private final int heapMaxSize;

    public BulkDVJoinQuery(Filter parents, Filter children, String joinField) {
      this(parents, children, joinField, 10000);
    }
    
    BulkDVJoinQuery(Filter parents, Filter children, String joinField, int perSegHeapSize) {
      super(parents, children, joinField);
      heapMaxSize = perSegHeapSize;
    }
    
    @Override
    public Weight createWeight(final IndexSearcher searcher) throws IOException {
      return new DVJoinWeight() {
        
        
        private final List<AtomicReaderContext> childCtxs;
        private final int[] childCtxDocBases//almost
        ;

        { // TODO contrib it back to ReaderUtil
          final IndexReaderContext top = searcher.getTopReaderContext();
          childCtxs = top.leaves();
          childCtxDocBases = new int[childCtxs.size()+1];
          for (int i = 0; i < childCtxs.size(); i++) {
            AtomicReaderContext context = childCtxs.get(i);
            childCtxDocBases[i] = context.docBase;
          }
          childCtxDocBases[childCtxs.size()] = top.reader().maxDoc();
        }
        
        @Override
        public BulkScorer bulkScorer(AtomicReaderContext context,
            boolean scoreDocsInOrder, Bits acceptDocs) throws IOException {
           assert !scoreDocsInOrder;
           
          return new DVBulkScorer(context, acceptDocs){
            
            // don't think that migration to sentinel objs to make it faster
            // unfortunately we can reuse the heap across segments, due to multithread search;
            // but why not otherwise?
            private final PriorityQueue<Relation> heap = new PriorityQueue<Relation>(heapMaxSize) {
              
              @Override
              protected boolean lessThan(Relation a, Relation b) {
                return a.docID() < b.docID();
              }
            };

            
            // TODO reuse heap across segments 
            
            @Override
            public boolean score(LeafCollector collector, int max)
                throws IOException {
              dump("parents ["+context.docBase+".."+(context.docBase+context.reader().maxDoc())+ "): "
                  , context, parents,acceptDocs);
              dump("children: ", context, children,acceptDocs);
              final DocIdSet parentDocs = parents.getDocIdSet(context, acceptDocs);
              if(parentDocs==null){
                return false;
              }
              
              final DocIdSetIterator parentsIter = parentDocs.iterator();
              if(parentsIter==null){
                return false;
              }
              
              final BinaryDocValues numericDocValues = context.reader().getBinaryDocValues(joinField);
              collector.setScorer(new FakeScorer());
              heap.clear();
              int docID;
              while((docID = parentsIter.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS && docID < max){
                final BytesRef bytes = new BytesRef();
                numericDocValues.get(docID, bytes);
                
                final Relation rel = new Relation(docID,bytes);
                if(rel.nextDoc()!=DocIdSetIterator.NO_MORE_DOCS){
                  heap.add(rel);
                  
                  if(heap.size() >= heapMaxSize){
                    flush( collector);
                  }
                }
              }
              flush( collector);
              return docID!=DocIdSetIterator.NO_MORE_DOCS;
            }
            
            private void flush(LeafCollector collector) throws IOException {
              
              for(;heap.size()>0;){ // i wonder why this leapfrog is so ugly?
                final int heapTop = heap.top().docID();
                int leafDoc=advanceLeafIter(heapTop);
                if(leafDoc==heapTop ){// there are matches on children 
                  collector.collect(heap.top().parentID);
                  // we don't care this relation anymore 
                  heap.pop();
                }else{
                  advanceHeap( leafDoc);
                }
              }
            }
            

            /** spins the top relation and the heap afterwards until top of the heap greater or equal to the given docnum that  
             */
            private void advanceHeap(int tillDoc) throws IOException {
              Relation rel = heap.top();
              assert tillDoc>rel.docID():"otherwise for what the "+tillDoc+" you advance "+rel.docID();
              while(heap.size()>0 && tillDoc>(rel=heap.top()).docID()){
                // whether, any miss match, spin top rel
                if(rel.advance(tillDoc)==DocIdSetIterator.NO_MORE_DOCS){
                  heap.pop();
                }else{
                  heap.updateTop();
                }
              }
            }

            BitSet untouched = new BitSet(childCtxs.size()){{
              set(0, childCtxs.size(), true);
            }};
            
            int [] segmentStarts = new int [childCtxs.size()];
            {
              for(int i=0;i<segmentStarts.length;i++){
                segmentStarts[i]=DocIdSetIterator.NO_MORE_DOCS;
              }
            }
            DocIdSet childDocsets[] = new DocIdSet[childCtxs.size()];
            DocIdSetIterator childDocsetIters[]  = new DocIdSetIterator[childCtxs.size()];
            int currentChildSegment = -1;
            
            
            int advanceLeafIter(
                int globalDoc)
                throws IOException {
              
              int possibleMatch = setChildCtxTo(globalDoc);
              if(possibleMatch!= globalDoc){
                assert possibleMatch > globalDoc;
                return possibleMatch;
              }
              // advance heap to the first doc at the child segment
              final int localTarget = globalDoc-childCtxs.get(currentChildSegment).docBase;

              // other wise lazily advance the child segment iter
                  if(localTarget >= childDocsetIters[currentChildSegment].docID()){
                    if(localTarget > childDocsetIters[currentChildSegment].docID()){//advance lazily
                      childDocsetIters[currentChildSegment].advance(localTarget);
                    }
                  }
      
                  DocIdSetIterator iter = childDocsetIters[currentChildSegment];
                  return iter.docID()==DocIdSetIterator.NO_MORE_DOCS ? nextSegment(currentChildSegment)
                        :iter.docID() + childCtxs.get(currentChildSegment).docBase;   
            }

            private int nextSegment(int s) {
              return childCtxs.get(s).docBase+childCtxs.get(s).reader().maxDoc();
            }

            private int setChildCtxTo(final int docID) throws IOException {
              // stay at the same segment
              if(currentChildSegment < 0 || docID< childCtxs.get(currentChildSegment).docBase ||
                  docID >= (childCtxs.get(currentChildSegment).docBase+childCtxs.get(currentChildSegment).reader().maxDoc()))
              {
                final int startSegment = (currentChildSegment < 0 || docID< childCtxs.get(currentChildSegment).docBase ) ? 0 : currentChildSegment +1;
                currentChildSegment = nextSegmentFor(docID, startSegment);
                initChildSegment(currentChildSegment);
              }
              assert docID>= childCtxs.get(currentChildSegment).docBase ||
                  docID < (childCtxs.get(currentChildSegment).docBase+childCtxs.get(currentChildSegment).reader().maxDoc());
              
              final int localTarget = docID-childCtxs.get(currentChildSegment).docBase;
              final boolean matchIsPossible = segmentStarts[currentChildSegment]!=DocIdSetIterator.NO_MORE_DOCS ;
                  //&& segmentStarts[currentChildSegment]<=localTarget;
              
              if(matchIsPossible){
                // do we need reset or not?
                if(segmentStarts[currentChildSegment]>localTarget){
                  return segmentStarts[currentChildSegment]+childCtxs.get(currentChildSegment).docBase;
                }else{
                  if((childDocsetIters[currentChildSegment].docID() > localTarget)){
                    //reset iter
                     childDocsetIters[currentChildSegment] = childDocsets[currentChildSegment].iterator();
                     //childDocsetIters[currentChildSegment].nextDoc();// TODO ohr'lly?
                  }
                  return docID;
                }
              } else{
                return nextSegment(currentChildSegment);
              }
            }

            private void initChildSegment(final int segment) throws IOException {
              if(untouched.get(segment)){
                //childDocsets
                AtomicReaderContext ctx = childCtxs.get(segment);
                childDocsets[segment] = children.getDocIdSet(ctx, ctx.reader().getLiveDocs());
                if(childDocsets[segment] != null){
                  childDocsetIters[segment] = childDocsets[segment].iterator();
                  if(childDocsetIters[segment]!=null){
                    segmentStarts[segment]=childDocsetIters[segment].nextDoc();
                  }
                }
                untouched.set(segment, false);
              }
            }
            
            // it's a copypaste from ReaderUtil, TODO contrib it back
            private int nextSegmentFor(final int docID, int startFrom) {
              // searcher/reader for doc n:
              int size = childCtxDocBases.length;
              int lo = startFrom; // search starts array
              int hi = size - 1; // for first element less than n, return its index
              while (hi >= lo) {
                int mid = (lo + hi) >>> 1;
                int midValue = childCtxDocBases[mid];
                if (docID < midValue)
                  hi = mid - 1;
                else if (docID > midValue)
                  lo = mid + 1;
                else { // found a match
                  while (mid + 1 < size && childCtxDocBases[mid + 1] == midValue) {
                    mid++; // scan to last match
                  }
                  return mid;
                }
              }
              return hi;
            }
            @Override
            protected boolean checkChildren(AtomicReaderContext context, int parentDoc,
                String joinField, Filter children) throws IOException {
              throw new UnsupportedOperationException();
            }
          };
        }
     
        @Override
        public boolean scoresDocsOutOfOrder() {
          return true;
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
    docs.add(doc);

    // 1
    doc = new Document();
    doc.add(newStringField("price", "10.0", Field.Store.YES));
    doc.add(newStringField(idField, "2", Field.Store.YES));
    doc.add(newStringField(toField, "1", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);

    // 2
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "3", Field.Store.YES));
    doc.add(newStringField(toField, "1", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);

    // 3
    doc = new Document();
    doc.add(newStringField("description", "more random text", Field.Store.YES));
    doc.add(newStringField("name", "name2", Field.Store.YES));
    doc.add(newStringField(idField, "4", Field.Store.YES));
    doc.add(newStringField("type", "parent", Field.Store.YES));
    docs.add(doc);

    // 4
    doc = new Document();
    doc.add(newStringField("price", "10.0", Field.Store.YES));
    doc.add(newStringField(idField, "5", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);
    // 5
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "6", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);
    
    for(Document d : docs){
      d.add(binDv(joinField));
    }
    Collections.shuffle(docs, random());

    Directory dir = newDirectory();
    //System.out.println(dir);
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    // conf.setInfoStream(System.out);
    // make sure random config doesn't flush on us
    conf.setMaxBufferedDocs(10);
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    conf.setMergePolicy(new LogDocMergePolicy());
    IndexWriter w = new IndexWriter(dir, conf);
    w.deleteAll();
    for(Document d:docs){
      w.addDocument(d);
      //if(VERBOSE){
      //  System.out.println(d);
      //}
      if(usually()){
        w.commit();
        //if(VERBOSE){
        //  System.out.println("commit");
        //}
      }
    }
    
    //for(Document d:docs){
    //  System.out.println(d);
    //}
    
    w.commit();
    System.out.println("docs are written");
    DirectoryReader reader = DirectoryReader.open(dir);
    new BinaryIndexJoiner(w, reader, idField, toField).
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
    Query dvJoinQuery = new BulkDVJoinQuery(parents, children, joinField);
    result = indexSearcher.search(dvJoinQuery, 10);
    assertIds(indexSearcher, result, "1", "4");
    assertEquals(2, result.totalHits);
    
    Query juPtoChJoinQuery = JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(juPtoChJoinQuery, 10);
    assertIds(indexSearcher, result, "5", "6");
    assertEquals(2, result.totalHits);
    
    Query dvPtoChJoinQuery = new BulkDVJoinQuery(new TermFilter(new Term("type", "child")), //parents,
                                            new TermFilter(new Term("name", "name2")), joinField);
    result = indexSearcher.search(dvPtoChJoinQuery, 10);
    assertIds(indexSearcher, result, "5", "6");
    assertEquals(2, result.totalHits);
    
    joinQuery =  //JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name1")), indexSearcher, ScoreMode.None);
        new BulkDVJoinQuery(new TermFilter(new Term("type", "child")), //parents,
            new TermFilter(new Term("name", "name1")), joinField);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertIds(indexSearcher, result, "2", "3");

    // Search for offer
    joinQuery = //JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "5")), indexSearcher, ScoreMode.None);
        new BulkDVJoinQuery(new TermFilter(new Term("type", "parent")), //parents,
            new TermFilter(new Term("id", "5")), joinField);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertIds(indexSearcher, result, "4");

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  public void testMultivalue() throws Exception {
    final String toField = "productId";
    final String joinField = IndexJoiner.joinFieldName(idField, toField);

    List<Document> docs = new ArrayList<>();
    // 0
    Document doc = new Document();
    doc.add(newStringField("description", "random text", Field.Store.YES));
    doc.add(newStringField("name", "name1", Field.Store.YES));
    doc.add(newStringField(idField, "1", Field.Store.YES));
    doc.add(newStringField("type", "parent", Field.Store.YES));
    docs.add(doc);

    // 1
    doc = new Document();
    doc.add(newStringField("price", "10.0", Field.Store.YES));
    doc.add(newStringField(idField, "2", Field.Store.YES));
    doc.add(newStringField(toField, "1", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);

    // 2
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "3", Field.Store.YES));
    doc.add(newStringField(toField, "1", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);

    // 3
    doc = new Document();
    doc.add(newStringField("description", "more random text", Field.Store.YES));
    doc.add(newStringField("name", "name2", Field.Store.YES));
    doc.add(newStringField(idField, "4", Field.Store.YES));
    doc.add(newStringField("type", "parent", Field.Store.YES));
    
    docs.add(doc);

    // 4
    doc = new Document();
    doc.add(newStringField("price", "10.0", Field.Store.YES));
    doc.add(newStringField(idField, "5", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);
    // 5
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "6", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);
    
    doc = new Document();
    doc.add(newStringField("price", "20.0", Field.Store.YES));
    doc.add(newStringField(idField, "7", Field.Store.YES));
    doc.add(newStringField(toField, "4", Field.Store.YES));
    // current indexJoiner can't write this relation
      // however, it's just because current update API, we could complicate it, and write it properly (we need to write it by pk term) 
      doc.add(newStringField(toField, "1", Field.Store.YES));

    doc.add(newStringField("type", "child", Field.Store.YES));
    docs.add(doc);
    
    // 0
    doc = new Document();
    doc.add(newStringField("description", "random text", Field.Store.YES));
    doc.add(newStringField("name", "name1", Field.Store.YES));
    doc.add(newStringField(idField, "9", Field.Store.YES));
    doc.add(newStringField("type", "parent", Field.Store.YES));
    docs.add(doc);
    
    for(Document d : docs){
      d.add(binDv(joinField));
    }
    //Collections.shuffle(docs, random());

    Directory dir = newDirectory();
    //System.out.println(dir);
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    // conf.setInfoStream(System.out);
    // make sure random config doesn't flush on us
    conf.setMaxBufferedDocs(10);
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    conf.setMergePolicy(new LogDocMergePolicy());
    IndexWriter w = new IndexWriter(dir, conf);

    w.deleteAll();
    for(Document d:docs){
      w.addDocument(d);
    }
    
    
    w.commit();
    System.out.println("docs are written");
    DirectoryReader reader = DirectoryReader.open(dir);
    new BinaryIndexJoiner(w, reader, idField, toField).
    indexJoin();
    reader.close();
    System.out.println("flushing num updates");
    w.commit();
    w.close();
    reader = DirectoryReader.open(dir);
    
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    
    dumpIndex(indexSearcher, "id", toField, joinField);
    
    {// simple child -> parent
      Query joinQuery = new BulkDVJoinQuery(new TermFilter(new Term("type", "parent")), //parents,
          new TermFilter(new Term("id", "5")), joinField);
      TopDocs result = indexSearcher.search(joinQuery, 10);
      assertEquals(1, result.totalHits);
      assertIds(indexSearcher, result, "4");
    }
    { // single value child -> parent
      TopDocs result = indexSearcher.search(
          JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "7")), indexSearcher, ScoreMode.None), 10);
      final String found = indexSearcher.doc(result.scoreDocs[0].doc).getValues("id")[0];
      assertTrue("but "+found, found.equals("1") || found.equals("4"));
      assertEquals("but "+result.totalHits,1,result.totalHits);
    }
    { // multi value child -> parent
      TopDocs result = indexSearcher.search(
          JoinUtil.createJoinQuery(toField, true, idField, new TermQuery(new Term("id", "7")), indexSearcher, ScoreMode.None), 10);
      assertEquals(2, result.totalHits);
      assertIds(indexSearcher, result, "1","4");
    }
    { // child ---*> parents[] works fine
      Query joinQuery = new BulkDVJoinQuery(new TermFilter(new Term("type", "parent")), //parents,
          new TermFilter(new Term("id", "7")), joinField);
      TopDocs result = indexSearcher.search(joinQuery, 10);
      assertIds(indexSearcher, result, "1","4");
      assertEquals(2, result.totalHits);
    }
    {
      Query joinQuery = new BulkDVJoinQuery(new TermFilter(new Term("type", "child")), //parents,
          new TermFilter(new Term("id", "4")), joinField);
      TopDocs result = indexSearcher.search(joinQuery, 10);
      assertIdsIncludes(indexSearcher, result, "7");
      assertIds(indexSearcher, result, "5","6","7");
      assertTrue("but "+result.totalHits,result.totalHits==3);
    }
    try{ // we can t carry association with "1", only with "4"
      Query joinQuery = new BulkDVJoinQuery(new TermFilter(new Term("type", "child")), //parents,
          new TermFilter(new Term("id", "1")), joinField);
      TopDocs result = indexSearcher.search(joinQuery, 10);
      assertTrue("but "+result.totalHits,result.totalHits>1);
      // It fails ever
      assertIdsIncludes(indexSearcher, result, "7");
      fail();
    }catch(AssertionError e){
      
    }
    {// childfree
      Query joinQuery = new BulkDVJoinQuery(new TermFilter(new Term("type", "parent")), //parents,
          new TermFilter(new Term("id", "9")), joinField);
      TopDocs result = indexSearcher.search(joinQuery, 10);
      assertEquals(0, result.totalHits);
    }
    indexSearcher.getIndexReader().close();
    dir.close();
    
  }

  private void dumpIndex(IndexSearcher indexSearcher, String pk,
      final String fk, final String joinField) throws IOException {
    if(VERBOSE){
      System.out.println("[d#]\t"+ "id"+
          "\t"+fk+
              "\t "+joinField);
      for(AtomicReaderContext arc : indexSearcher.getTopReaderContext().leaves()){
        final BinaryDocValues bdv = arc.reader().getBinaryDocValues(joinField);
        for(int d=0;d<arc.reader().maxDoc();d++){
          if(arc.reader().getLiveDocs()==null || arc.reader().getLiveDocs().get(d)){
            int gd = d+arc.docBase;
            final StoredDocument fields = indexSearcher.doc(gd);
            final BytesRef dv = new BytesRef();
            bdv.get(d, dv);
            System.out.println("["+gd+"]\t"+ Arrays.toString(fields.getValues(pk))+
                                       "\t"+ Arrays.toString(fields.getValues(fk))+
                                           "\t"+dv
                              );
          }
        }
        System.out.println("---");
      }
    }
  }
  
  private Field dv(final String joinField) {
    return new NumericDocValuesField(joinField, -1);
  }
  
  private Field binDv(final String joinField) {
    return new BinaryDocValuesField(joinField, new BytesRef());
  }

  
  static abstract  class IndexJoiner {
    
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
    
    void indexJoin() throws IOException {
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
        if (VERBOSE) {
           for(Entry<Term, Long> e : updTracker.entrySet()){
           System.out.print(e.getKey()+" ");
           System.out.println(e.getValue()==null?"null":Long.toHexString(e.getValue()));
           }
        }
      }
    }
    
    private void marryThem(BytesRef pk, BytesRef fk) throws IOException {
      if (VERBOSE) {
        System.out.println(pkField+":"+(pk!=null? pk.utf8ToString():pk)
         +" <-> "+fkField+":"+(fk!=null?fk.utf8ToString():fk));
      }
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
                  .getNumericDocValues(joinFieldName());
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
        refs = null;// TODO sort out the hell with childfree (at least), for now fixing by empty default
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
      final BytesRef br = new BytesRef(out.bytes, 0, out.length);
      writer.updateBinaryDocValue(referrers, joinFieldName(),  br);
      if(VERBOSE){
        System.out.println(referrers+" = "+ br);
      }
    }
  }
 
  static void assertIds(IndexSearcher indexSearcher, TopDocs rez, String ... ids) throws IOException{
    Map<String,Integer> act = actualIds(indexSearcher, rez);
    Set<String> exp = new HashSet<>(Arrays.asList(ids));
    assertEquals("got "+act,exp, act.keySet());
    assertEquals(ids.length, rez.totalHits);
  }
  
  static void assertIdsIncludes(IndexSearcher indexSearcher, TopDocs rez, String ... ids) throws IOException{
    Map<String,Integer> act = actualIds(indexSearcher, rez);
    Set<String> exp = new HashSet<>(Arrays.asList(ids));
    assertTrue("got "+act, act.keySet().containsAll(exp));
  }

  private static Map<String,Integer> actualIds(IndexSearcher indexSearcher,
      TopDocs rez) throws IOException {
    Map<String, Integer> act = new HashMap<>();
    for(ScoreDoc sd:rez.scoreDocs){
      final Integer mut = act.put( indexSearcher.doc(sd.doc).getValues(idField)[0], sd.doc);
      assert mut==null;
    }
    return act;
  }

  private static void dump(String label,
      final AtomicReaderContext context, final Filter aFilter, Bits acceptDocs) throws IOException {
    if (VERBOSE) {
      System.out.println(label+aFilter);
      final DocIdSet docIdSet = aFilter.getDocIdSet(context, acceptDocs);
      if(docIdSet!=null){
        int doc;
        for(DocIdSetIterator iter = docIdSet.iterator();iter!=null && (doc=iter.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS;){
          System.out.print(doc);
          System.out.print(", ");
        }
        System.out.println();
      }
    }
  }

  private static void dump(final AtomicReaderContext context,
      final String joinFieldName) throws IOException {
    if (VERBOSE) {
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
  
  
  @Test
 // @Slow
  public void testSingleValueRandomJoin() throws Exception {
    int maxIndexIter = TestUtil.nextInt(random(), 6, 12);
    int maxSearchIter = TestUtil.nextInt(random(), 13, 26);
    executeRandomJoin(false, maxIndexIter, maxSearchIter, TestUtil.nextInt(random(), 87, 764)
        );
  }

  @Test
  @Slow
  @Ignore // I couldn't manage it to work ever
   // This test really takes more time, that is why the number of iterations are smaller.
  public void testMultiValueRandomJoin() throws Exception {
    int maxIndexIter = TestUtil.nextInt(random(), 3, 6);
    int maxSearchIter = TestUtil.nextInt(random(), 6, 12);
    executeRandomJoin(true, maxIndexIter, maxSearchIter, TestUtil.nextInt(random(), 11, 57));
  }

  private void executeRandomJoin(boolean multipleValuesPerDocument, int maxIndexIter, int maxSearchIter, int numberOfDocumentsToIndex) throws Exception {
    for (int indexIter = 1; indexIter <= maxIndexIter; indexIter++) {
      if (VERBOSE) {
        System.out.println("indexIter=" + indexIter);
      }
      Directory dir = newDirectory();
      final IndexWriterConfig indexWriterConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
      indexWriterConfig.setInfoStream(InfoStream.NO_OUTPUT);
      RandomIndexWriter w = new RandomIndexWriter(
          random(),
          dir,
          indexWriterConfig.setMergePolicy(newLogMergePolicy())
      );
      final boolean scoreDocsInOrder = true;//TestJoinUtil.random().nextBoolean();
      IndexIterationContext context = createContext(numberOfDocumentsToIndex, w, multipleValuesPerDocument, scoreDocsInOrder);

      w.commit();
      DirectoryReader topLevelReader = w.getReader();
      
      new BinaryIndexJoiner(w.w, topLevelReader, "to", "from").
                          indexJoin();
      topLevelReader.close();
      w.commit();
      
      topLevelReader = w.getReader();
      
    //  w.close();
      
      w.shutdown();
      for (int searchIter = 1; searchIter <= maxSearchIter; searchIter++) {
        if (VERBOSE) {
          System.out.println("searchIter=" + searchIter +" indexIter=" + indexIter);
        }
        IndexSearcher indexSearcher = newSearcher(topLevelReader);

        int r = random().nextInt(context.randomUniqueValues.length);
        boolean from = context.randomFrom[r];
        String randomValue = context.randomUniqueValues[r];
        FixedBitSet expectedResult = createExpectedResult(randomValue, from, indexSearcher.getIndexReader(), context);

        final Query actualQuery = new TermQuery(new Term("value", randomValue));
        if (VERBOSE) {
          System.out.println("actualQuery=" + actualQuery);
        }
        final ScoreMode scoreMode = ScoreMode.None;//ScoreMode.values()[random().nextInt(ScoreMode.values().length)];
        if (VERBOSE) {
          System.out.println("scoreMode=" + scoreMode);
        }

        int heapSize = (1+random().nextInt(10)) * (rarely() ? 1000 : 1);
        final Query joinQuery;
        if (from) {
          //joinQuery = JoinUtil.createJoinQuery("from", multipleValuesPerDocument, "to", actualQuery, indexSearcher, scoreMode);
          
          joinQuery =  new BulkDVJoinQuery(new TermFilter(new Term("type", "to")),
                      new QueryWrapperFilter(actualQuery), IndexJoiner.joinFieldName( "to", "from"),
                      heapSize);
        } else {
          //joinQuery = JoinUtil.createJoinQuery("to", multipleValuesPerDocument, "from", actualQuery, indexSearcher, scoreMode);
          joinQuery =  new BulkDVJoinQuery(new TermFilter(new Term("type", "from")),
              new QueryWrapperFilter(actualQuery), IndexJoiner.joinFieldName( "to", "from"),
              heapSize);
        }
        if (VERBOSE) {
          System.out.println("joinQuery=" + joinQuery);
        }

        // Need to know all documents that have matches. TopDocs doesn't give me that and then I'd be also testing TopDocsCollector...
        final FixedBitSet actualResult = new FixedBitSet(indexSearcher.getIndexReader().maxDoc());
        final TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(10, false);
        indexSearcher.search(joinQuery//, new TermFilter(new Term("type", from ? "to":"from"))
              , new SimpleCollector() {

          int docBase;

          @Override
          public void collect(int doc) throws IOException {
            actualResult.set(doc + docBase);
            topScoreDocCollector.collect(doc);
          }

          @Override
          protected void doSetNextReader(AtomicReaderContext context) throws IOException {
            docBase = context.docBase;
            topScoreDocCollector.getLeafCollector(context);
          }

          @Override
          public void setScorer(Scorer scorer) throws IOException {
            topScoreDocCollector.setScorer(scorer);
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return scoreDocsInOrder;
          }
        }
        );
        // Asserting bit set...
        if (VERBOSE) {
          System.out.println("expected cardinality:" + expectedResult.cardinality());
          DocIdSetIterator iterator = expectedResult.iterator();
          for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            System.out.println(String.format(Locale.ROOT, "Expected doc[%d] with id value %s", doc, indexSearcher.doc(doc).get("id")));
          }
          System.out.println("actual cardinality:" + actualResult.cardinality());
          iterator = actualResult.iterator();
          for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            System.out.println(String.format(Locale.ROOT, "Actual doc[%d] with id value %s", doc, indexSearcher.doc(doc).get("id")));
          }
        }
        assertEquals(expectedResult, actualResult);

        // Asserting TopDocs...
        TopDocs expectedTopDocs = createExpectedTopDocs(randomValue, from, scoreMode, context);
        TopDocs actualTopDocs = topScoreDocCollector.topDocs();
        assertEquals(expectedTopDocs.totalHits, actualTopDocs.totalHits);
        assertEquals(expectedTopDocs.scoreDocs.length, actualTopDocs.scoreDocs.length);
        if (scoreMode == ScoreMode.None) {
          continue;
        }

        assertEquals(expectedTopDocs.getMaxScore(), actualTopDocs.getMaxScore(), 0.0f);
        for (int i = 0; i < expectedTopDocs.scoreDocs.length; i++) {
          if (VERBOSE) {
            System.out.printf(Locale.ENGLISH, "Expected doc: %d | Actual doc: %d\n", expectedTopDocs.scoreDocs[i].doc, actualTopDocs.scoreDocs[i].doc);
            System.out.printf(Locale.ENGLISH, "Expected score: %f | Actual score: %f\n", expectedTopDocs.scoreDocs[i].score, actualTopDocs.scoreDocs[i].score);
          }
          assertEquals(expectedTopDocs.scoreDocs[i].doc, actualTopDocs.scoreDocs[i].doc);
          assertEquals(expectedTopDocs.scoreDocs[i].score, actualTopDocs.scoreDocs[i].score, 0.0f);
          Explanation explanation = indexSearcher.explain(joinQuery, expectedTopDocs.scoreDocs[i].doc);
          assertEquals(expectedTopDocs.scoreDocs[i].score, explanation.getValue(), 0.0f);
        }
      }
      topLevelReader.close();
      dir.close();
    }
  }

  private IndexIterationContext createContext(int nDocs, RandomIndexWriter writer, boolean multipleValuesPerDocument, boolean scoreDocsInOrder) throws IOException {
    return createContext(nDocs, writer, writer, multipleValuesPerDocument, scoreDocsInOrder);
  }

  private IndexIterationContext createContext(int nDocs, RandomIndexWriter fromWriter, RandomIndexWriter toWriter, boolean multipleValuesPerDocument, boolean scoreDocsInOrder) throws IOException {
    IndexIterationContext context = new IndexIterationContext();
    int numRandomValues = nDocs / 2;
    context.randomUniqueValues = new String[numRandomValues];
    Set<String> trackSet = new HashSet<>();
    context.randomFrom = new boolean[numRandomValues];
    for (int i = 0; i < numRandomValues; i++) {
      String uniqueRandomValue;
      do {
        uniqueRandomValue = TestUtil.randomRealisticUnicodeString(random());
//        uniqueRandomValue = _TestUtil.randomSimpleString(random);
      } while ("".equals(uniqueRandomValue) || trackSet.contains(uniqueRandomValue));
      // Generate unique values and empty strings aren't allowed.
      trackSet.add(uniqueRandomValue);
      context.randomFrom[i] = random().nextBoolean();
      context.randomUniqueValues[i] = uniqueRandomValue;
    }

    RandomDoc[] docs = new RandomDoc[nDocs];
    for (int i = 0; i < nDocs; i++) {
      String id = Integer.toString(i);
      int randomI = random().nextInt(context.randomUniqueValues.length);
      String value = context.randomUniqueValues[randomI];
      Document document = new Document();
      document.add(newTextField(random(), "id", id, Field.Store.YES));
      document.add(newTextField(random(), "value", value, Field.Store.YES));

      boolean from = context.randomFrom[randomI];
      int numberOfLinkValues = multipleValuesPerDocument ? 2 + random().nextInt(10) : 1;
      docs[i] = new RandomDoc(id, numberOfLinkValues, value, from);
      for (int j = 0; j < numberOfLinkValues; j++) {
        String linkValue = context.randomUniqueValues[random().nextInt(context.randomUniqueValues.length)];
        docs[i].linkValues.add(linkValue);
        if (from) {
          if (!context.fromDocuments.containsKey(linkValue)) {
            context.fromDocuments.put(linkValue, new ArrayList<RandomDoc>());
          }
          if (!context.randomValueFromDocs.containsKey(value)) {
            context.randomValueFromDocs.put(value, new ArrayList<RandomDoc>());
          }

          context.fromDocuments.get(linkValue).add(docs[i]);
          context.randomValueFromDocs.get(value).add(docs[i]);
          document.add(newTextField(random(), "from", linkValue, Field.Store.NO));
          document.add(newStringField(random(), "type", "from", Field.Store.NO));
        } else {
          if (!context.toDocuments.containsKey(linkValue)) {
            context.toDocuments.put(linkValue, new ArrayList<RandomDoc>());
          }
          if (!context.randomValueToDocs.containsKey(value)) {
            context.randomValueToDocs.put(value, new ArrayList<RandomDoc>());
          }

          context.toDocuments.get(linkValue).add(docs[i]);
          context.randomValueToDocs.get(value).add(docs[i]);
          document.add(newTextField(random(), "to", linkValue, Field.Store.NO));
          document.add(newStringField(random(), "type", "to", Field.Store.NO));
        }
      }

      document.add(new BinaryDocValuesField(IndexJoiner.joinFieldName( "to", "from"), new BytesRef((char)-1)));
      final RandomIndexWriter w;
      if (from) {
        w = fromWriter;
      } else {
        w = toWriter;
      }

      w.addDocument(document);
      if (random().nextInt(10) == 4) {
        w.commit();
      }
      if (VERBOSE) {
        System.out.println("Added document[" + docs[i].id + "]: " + document);
      }
    }

    // Pre-compute all possible hits for all unique random values. On top of this also compute all possible score for
    // any ScoreMode.
    IndexSearcher fromSearcher = newSearcher(fromWriter.getReader());
    IndexSearcher toSearcher = newSearcher(toWriter.getReader());
    for (int i = 0; i < context.randomUniqueValues.length; i++) {
      String uniqueRandomValue = context.randomUniqueValues[i];
      final String fromField;
      final String toField;
      final Map<String, Map<Integer, JoinScore>> queryVals;
      if (context.randomFrom[i]) {
        fromField = "from";
        toField = "to";
        queryVals = context.fromHitsToJoinScore;
      } else {
        fromField = "to";
        toField = "from";
        queryVals = context.toHitsToJoinScore;
      }
      final Map<BytesRef, JoinScore> joinValueToJoinScores = new HashMap<>();
      if (multipleValuesPerDocument) {
        fromSearcher.search(new TermQuery(new Term("value", uniqueRandomValue)), new SimpleCollector() {

          private Scorer scorer;
          private SortedSetDocValues docTermOrds;
          final BytesRef joinValue = new BytesRef();

          @Override
          public void collect(int doc) throws IOException {
            docTermOrds.setDocument(doc);
            long ord;
            while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              docTermOrds.lookupOrd(ord, joinValue);
              JoinScore joinScore = joinValueToJoinScores.get(joinValue);
              if (joinScore == null) {
                joinValueToJoinScores.put(BytesRef.deepCopyOf(joinValue), joinScore = new JoinScore());
              }
              joinScore.addScore(scorer.score());
            }
          }

          @Override
          protected void doSetNextReader(AtomicReaderContext context) throws IOException {
            docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), fromField);
          }

          @Override
          public void setScorer(Scorer scorer) {
            this.scorer = scorer;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return false;
          }
        });
      } else {
        fromSearcher.search(new TermQuery(new Term("value", uniqueRandomValue)), new SimpleCollector() {

          private Scorer scorer;
          private BinaryDocValues terms;
          private Bits docsWithField;
          private final BytesRef spare = new BytesRef();

          @Override
          public void collect(int doc) throws IOException {
            terms.get(doc, spare);
            BytesRef joinValue = spare;
            if (joinValue.length == 0 && !docsWithField.get(doc)) {
              return;
            }

            JoinScore joinScore = joinValueToJoinScores.get(joinValue);
            if (joinScore == null) {
              joinValueToJoinScores.put(BytesRef.deepCopyOf(joinValue), joinScore = new JoinScore());
            }
            joinScore.addScore(scorer.score());
          }

          @Override
          protected void doSetNextReader(AtomicReaderContext context) throws IOException {
            terms = FieldCache.DEFAULT.getTerms(context.reader(), fromField, true);
            docsWithField = FieldCache.DEFAULT.getDocsWithField(context.reader(), fromField);
          }

          @Override
          public void setScorer(Scorer scorer) {
            this.scorer = scorer;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return false;
          }
        });
      }

      final Map<Integer, JoinScore> docToJoinScore = new HashMap<>();
      if (multipleValuesPerDocument) {
        if (scoreDocsInOrder) {
          AtomicReader slowCompositeReader = SlowCompositeReaderWrapper.wrap(toSearcher.getIndexReader());
          Terms terms = slowCompositeReader.terms(toField);
          if (terms != null) {
            DocsEnum docsEnum = null;
            TermsEnum termsEnum = null;
            SortedSet<BytesRef> joinValues = new TreeSet<>(BytesRef.getUTF8SortedAsUnicodeComparator());
            joinValues.addAll(joinValueToJoinScores.keySet());
            for (BytesRef joinValue : joinValues) {
              termsEnum = terms.iterator(termsEnum);
              if (termsEnum.seekExact(joinValue)) {
                docsEnum = termsEnum.docs(slowCompositeReader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                JoinScore joinScore = joinValueToJoinScores.get(joinValue);

                for (int doc = docsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docsEnum.nextDoc()) {
                  // First encountered join value determines the score.
                  // Something to keep in mind for many-to-many relations.
                  if (!docToJoinScore.containsKey(doc)) {
                    docToJoinScore.put(doc, joinScore);
                  }
                }
              }
            }
          }
        } else {
          toSearcher.search(new MatchAllDocsQuery(), new SimpleCollector() {

            private SortedSetDocValues docTermOrds;
            private final BytesRef scratch = new BytesRef();
            private int docBase;

            @Override
            public void collect(int doc) throws IOException {
              docTermOrds.setDocument(doc);
              long ord;
              while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                docTermOrds.lookupOrd(ord, scratch);
                JoinScore joinScore = joinValueToJoinScores.get(scratch);
                if (joinScore == null) {
                  continue;
                }
                Integer basedDoc = docBase + doc;
                // First encountered join value determines the score.
                // Something to keep in mind for many-to-many relations.
                if (!docToJoinScore.containsKey(basedDoc)) {
                  docToJoinScore.put(basedDoc, joinScore);
                }
              }
            }

            @Override
            protected void doSetNextReader(AtomicReaderContext context) throws IOException {
              docBase = context.docBase;
              docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), toField);
            }

            @Override
            public boolean acceptsDocsOutOfOrder() {return false;}
            @Override
            public void setScorer(Scorer scorer) {}
          });
        }
      } else {
        toSearcher.search(new MatchAllDocsQuery(), new SimpleCollector() {

          private BinaryDocValues terms;
          private int docBase;
          private final BytesRef spare = new BytesRef();

          @Override
          public void collect(int doc) {
            terms.get(doc, spare);
            JoinScore joinScore = joinValueToJoinScores.get(spare);
            if (joinScore == null) {
              return;
            }
            docToJoinScore.put(docBase + doc, joinScore);
          }

          @Override
          protected void doSetNextReader(AtomicReaderContext context) throws IOException {
            terms = FieldCache.DEFAULT.getTerms(context.reader(), toField, false);
            docBase = context.docBase;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {return false;}
          @Override
          public void setScorer(Scorer scorer) {}
        });
      }
      queryVals.put(uniqueRandomValue, docToJoinScore);
    }

    fromSearcher.getIndexReader().close();
    toSearcher.getIndexReader().close();

    return context;
  }

  private TopDocs createExpectedTopDocs(String queryValue,
                                        final boolean from,
                                        final ScoreMode scoreMode,
                                        IndexIterationContext context) {

    Map<Integer, JoinScore> hitsToJoinScores;
    if (from) {
      hitsToJoinScores = context.fromHitsToJoinScore.get(queryValue);
    } else {
      hitsToJoinScores = context.toHitsToJoinScore.get(queryValue);
    }
    List<Map.Entry<Integer,JoinScore>> hits = new ArrayList<>(hitsToJoinScores.entrySet());
    Collections.sort(hits, new Comparator<Map.Entry<Integer, JoinScore>>() {

      @Override
      public int compare(Map.Entry<Integer, JoinScore> hit1, Map.Entry<Integer, JoinScore> hit2) {
        float score1 = hit1.getValue().score(scoreMode);
        float score2 = hit2.getValue().score(scoreMode);

        int cmp = Float.compare(score2, score1);
        if (cmp != 0) {
          return cmp;
        }
        return hit1.getKey() - hit2.getKey();
      }

    });
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(10, hits.size())];
    for (int i = 0; i < scoreDocs.length; i++) {
      Map.Entry<Integer,JoinScore> hit = hits.get(i);
      scoreDocs[i] = new ScoreDoc(hit.getKey(), hit.getValue().score(scoreMode));
    }
    return new TopDocs(hits.size(), scoreDocs, hits.isEmpty() ? Float.NaN : hits.get(0).getValue().score(scoreMode));
  }

  private FixedBitSet createExpectedResult(String queryValue, boolean from, IndexReader topLevelReader, IndexIterationContext context) throws IOException {
    final Map<String, List<RandomDoc>> randomValueDocs;
    final Map<String, List<RandomDoc>> linkValueDocuments;
    if (from) {
      randomValueDocs = context.randomValueFromDocs;
      linkValueDocuments = context.toDocuments;
    } else {
      randomValueDocs = context.randomValueToDocs;
      linkValueDocuments = context.fromDocuments;
    }

    FixedBitSet expectedResult = new FixedBitSet(topLevelReader.maxDoc());
    List<RandomDoc> matchingDocs = randomValueDocs.get(queryValue);
    if (matchingDocs == null) {
      return new FixedBitSet(topLevelReader.maxDoc());
    }

    for (RandomDoc matchingDoc : matchingDocs) {
      for (String linkValue : matchingDoc.linkValues) {
        List<RandomDoc> otherMatchingDocs = linkValueDocuments.get(linkValue);
        if (otherMatchingDocs == null) {
          continue;
        }

        for (RandomDoc otherSideDoc : otherMatchingDocs) {
          DocsEnum docsEnum = MultiFields.getTermDocsEnum(topLevelReader, MultiFields.getLiveDocs(topLevelReader), "id", new BytesRef(otherSideDoc.id), 0);
          assert docsEnum != null;
          int doc = docsEnum.nextDoc();
          expectedResult.set(doc);
        }
      }
    }
    return expectedResult;
  }

  private static class IndexIterationContext {

    String[] randomUniqueValues;
    boolean[] randomFrom;
    Map<String, List<RandomDoc>> fromDocuments = new HashMap<>();
    Map<String, List<RandomDoc>> toDocuments = new HashMap<>();
    Map<String, List<RandomDoc>> randomValueFromDocs = new HashMap<>();
    Map<String, List<RandomDoc>> randomValueToDocs = new HashMap<>();

    Map<String, Map<Integer, JoinScore>> fromHitsToJoinScore = new HashMap<>();
    Map<String, Map<Integer, JoinScore>> toHitsToJoinScore = new HashMap<>();

  }

  private static class RandomDoc {

    final String id;
    final List<String> linkValues;
    final String value;
    final boolean from;

    private RandomDoc(String id, int numberOfLinkValues, String value, boolean from) {
      this.id = id;
      this.from = from;
      linkValues = new ArrayList<>(numberOfLinkValues);
      this.value = value;
    }
  }

  private static class JoinScore {

    float maxScore;
    float total;
    int count;

    void addScore(float score) {
      total += score;
      if (score > maxScore) {
        maxScore = score;
      }
      count++;
    }

    float score(ScoreMode mode) {
      switch (mode) {
        case None:
          return 1.0f;
        case Total:
          return total;
        case Avg:
          return total / count;
        case Max:
          return maxScore;
      }
      throw new IllegalArgumentException("Unsupported ScoreMode: " + mode);
    }

  }

}
