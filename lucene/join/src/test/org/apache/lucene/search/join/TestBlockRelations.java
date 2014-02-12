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
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.*;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TestBlockRelations extends LuceneTestCase {

  static class RelationQuery extends Query {
    private final Query child;
    private final String relationField;
    
    RelationQuery(Query child, String nextP) {
      this.child = child;
      this.relationField = nextP;
    }
    
    @Override
    public String toString(String field) {
      return "join "+child+" by relation from "+relationField;
    }
    
    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
      final Weight childWeight = child.createWeight(searcher);
      return new Weight() {
        
        @Override
        public Scorer scorer(final AtomicReaderContext context, boolean scoreDocsInOrder,
            boolean topScorer, final Bits acceptDocs) throws IOException {
          final Scorer childScorer = childWeight.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
          return new Scorer(this){
            
            final TermsEnum parentTerms; 
            final Bits lives = acceptDocs;
            private int docId = -1;
            {
              parentTerms = context.reader().terms(relationField).iterator(null);
              
            }

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
              return docId;
            }

            @Override
            public int nextDoc() throws IOException {
              
              int childDoc = childScorer.nextDoc();
              return docId  = leapFrogParentChild(childScorer, childDoc);
            }

            private int leapFrogParentChild(final Scorer childScorer,
                int childDoc) throws IOException {
              while(childDoc != NO_MORE_DOCS){
                final int parent = getParentOfChild(childDoc);
                if(lives==null || lives.get(parent)){
                  return parent;
                }else{
                  childDoc = childScorer.advance(parent);
                  assert childDoc>parent : "should be orthogonal but "+childDoc+" "+parent;
                  continue;
                }
              }
              assert childDoc == NO_MORE_DOCS: "however "+childDoc;
              return NO_MORE_DOCS;
            }

            private int getParentOfChild(final int childDoc)
                throws IOException {
              final BytesRef text = new BytesRef();
              NumericUtils.intToPrefixCodedBytes(childDoc, 0, text);
              final SeekStatus seekRes = parentTerms.seekCeil(text);
              assert seekRes == SeekStatus.NOT_FOUND : "parent enum should be orthogonal and enclosing for children, however "+seekRes+" on "+childDoc;
              // TODO parent might be nuked by deletion 
              final int parent = NumericUtils.prefixCodedToInt(parentTerms.term());
              assert parent>childDoc : parent+" ? "+childDoc;
              return parent;
            }

            @Override
            public int advance(int target) throws IOException {
              if(target==NO_MORE_DOCS){ // see explanation in ConjunctionScorer.doNext(int)
                return docId=NO_MORE_DOCS;
              }
              // find live parent
              int parent;
              while(true){
                final BytesRef bytes = new BytesRef();
                NumericUtils.intToPrefixCoded(target, 0, bytes);
                final SeekStatus seekRes = parentTerms.seekCeil(bytes);
                assert seekRes != SeekStatus.END : "parent is the last doc in the index anyway: "
                    +seekRes+" when seeking to "+target;
                parent = NumericUtils.prefixCodedToInt(parentTerms.term());
                assert target<=parent : "advance("+target+")=="+parent ;
                if(lives==null || lives.get(parent)){
                  break;
                }else{
                  target = parent+1;
                  continue;
                }
              }
              // jump to prev parent
              final DocsEnum docs = parentTerms.docs(lives, null);
              final int prevParent = docs.nextDoc();
              assert prevParent != NO_MORE_DOCS :" however "+prevParent;
              // don't worry, I just exhaust it for fun
              assert docs.nextDoc() == NO_MORE_DOCS:"should have only one 'prev' parent, however "+docs.docID();
              
              final int childDoc = childScorer.advance(prevParent);
              
              if(childDoc == NO_MORE_DOCS){
                return docId=NO_MORE_DOCS;
              }
              assert childDoc!=parent:"however "+childDoc+" == "+parent;
              // check if there is child match before parent
              if(childDoc > parent){
                leapFrogParentChild(childScorer, childDoc);
              }
              return docId=parent;
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
          return RelationQuery.this;
        }
        
        @Override
        public Explanation explain(AtomicReaderContext context, int doc)
            throws IOException {
          return null;
        }
      };
    }
    
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      final Query rewrite = child.rewrite(reader);
      if(child!=rewrite){
        return new RelationQuery(rewrite, relationField);
      }
      return this;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((child == null) ? 0 : child.hashCode());
      result = prime * result
          + ((relationField == null) ? 0 : relationField.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      RelationQuery other = (RelationQuery) obj;
      if (child == null) {
        if (other.child != null) return false;
      } else if (!child.equals(other.child)) return false;
      if (relationField == null) {
        if (other.relationField != null) return false;
      } else if (!relationField.equals(other.relationField)) return false;
      return true;
    }
  }

  private static final String nextParent = "nextParent";

  // One resume...
  private Document makeResume(String name, String country) {
    Document resume = new Document();
    resume.add(newStringField("docType", "resume", Field.Store.NO));
    resume.add(newStringField("name", name, Field.Store.YES));
    resume.add(newStringField("country", country, Field.Store.NO));
    return resume;
  }

  // ... has multiple jobs
  private Document makeJob(String skill, int year) {
    Document job = new Document();
    job.add(newStringField("skill", skill, Field.Store.YES));
    job.add(new IntField("year", year, Field.Store.NO));
    job.add(new StoredField("year", year));
    return job;
  }

  static void indexRelations(RandomIndexWriter w, Iterator<List<Document>> blocks) throws IOException{
    
    int docNum=0; 
    for(PeekingIterator<List<Document>> lookAhead = Iterators.peekingIterator(blocks);
        lookAhead.hasNext();){
      
      if(docNum==0){
        List<Document> block = lookAhead.peek();
        final Document starter = createStarterDoc(block);
        w.addDocument(starter);
        //System.out.println("docNum:"+docNum+" prefix coded:"+bytes+" nextParent:"+i);
      }
      List<Document> blockDocs = lookAhead.next();
      if(lookAhead.hasNext()){// need to refer to next block
        final Document parent = blockDocs.get(blockDocs.size()-1);
        
        final Field i = createNextParentRef(docNum, blockDocs.size(),
            lookAhead.peek().size());
        parent.add(i);
        //System.out.println("docNum:"+(docNum+blockDocs.size())+" prefix coded:"+bytes+" nextParent:"+i);
      }
      w.addDocuments(blockDocs);
      /**for(Document d: blockDocs){
        System.out.println(d);
      }*/
      docNum+=blockDocs.size();
      //System.out.println(docNum);
    }
  }

  private static Field createNextParentRef(int docNum,
      final int currentBlockSize, final int nextBlockSize) {
    BytesRef bytes = new BytesRef();
    NumericUtils.intToPrefixCoded(docNum + currentBlockSize+
        nextBlockSize, 0, bytes);
  
    final Field i = newStringField(nextParent,
        bytes.utf8ToString(), Store.NO);
    return i;
  }

  private static Document createStarterDoc(List<Document> block) {
    // write an implying starter doc
    final Document starter = new Document();
    final BytesRef bytes = new BytesRef();
    
    NumericUtils.intToPrefixCoded(block.size(), 0, bytes);
    final Field i =
    newStringField(nextParent, bytes.utf8ToString(), Field.Store.NO);
    starter.add(i);
    return starter;
  }
  
  /** search for all parents one by one, 
   * @throws IOException on search */
  private void assertNextParents(IndexSearcher s) throws IOException {
    final TopFieldDocs parents = s.search(new TermQuery(new Term("docType", "resume")), 100, Sort.INDEXORDER);
    assertTrue(parents.scoreDocs.length>0);
    int lastParent = 0; //starter
    for(ScoreDoc sd: parents.scoreDocs){
      BytesRef bytes = new BytesRef();
      NumericUtils.intToPrefixCoded(sd.doc, 0, bytes);
      final TopFieldDocs parent = s.search(new TermQuery(new Term(nextParent, bytes)), 10,
          new Sort(new SortField(nextParent,Type.INT))); // requesting indexed field values via sorting
      
      assertEquals("every parent doc# term has single posting associated" ,1, parent.totalHits);
      assertEquals("indexed term is equal to parent doc# (via IntParser)", sd.doc, ((FieldDoc) parent.scoreDocs[0]).fields[0]);
      assertEquals("every parent doc# term refers to the previous partent", lastParent, parent.scoreDocs[0].doc);
      // NumberUtils.SortableStr2int(parentTerms.term().toString(),0,3)
      lastParent = sd.doc;
    }
    
  }
  
  public void testSimple() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    List<List<Document>> blocks = Arrays.<List<Document>>asList( new ArrayList<Document>(){{
      add(makeJob("java", 2007));
      add(makeJob("python", 2010));
      Collections.shuffle(this);
      add(makeResume("Lisa", "United Kingdom"));
    }},
     new ArrayList<Document>(){{
      add(makeJob("ruby", 2005));
      add(makeJob("java", 2006));
      Collections.shuffle(this);
      add(makeResume("Frank", "United States"));
     }});
    Collections.shuffle(blocks);
    indexRelations(w, blocks.iterator());
    
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    
    assertNextParents(s);

    final Query child = new TermQuery(new Term("skill", "java"));
    assertResumes(s, s.search(new RelationQuery(child, nextParent ), 10), 
                  Arrays.asList("Lisa","Frank"));
    
    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery childQuery = new BooleanQuery();
    childQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childQuery.add(new BooleanClause(NumericRangeQuery.newIntRange("year", 2006, 2011, true, true), Occur.MUST));

    // Define parent document criteria (find a resident in the UK)
    final Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    final Query childJoinQuery = new RelationQuery(childQuery, nextParent);

    // Combine the parent and nested child queries into a single query for a candidate
    assertResumes(s, s.search(new BooleanQuery(){{
                                    add(new BooleanClause(parentQuery, Occur.MUST));
                                    add(new BooleanClause(childJoinQuery, Occur.MUST));
                                    }}, 10), 
        Arrays.asList("Lisa"));
    
    assertResumes(s, s.search(childJoinQuery, // combine'em by filtering, same stuff
                                  new QueryWrapperFilter(parentQuery), 10), 
        Arrays.asList("Lisa"));

    assertResumes(s, s.search(childJoinQuery,
        new QueryWrapperFilter(new BooleanQuery(){{
          add( parentQuery, Occur.SHOULD); // adding any child doc in filter does nothing
          add( new TermQuery(new Term("skill", "java")), Occur.SHOULD);
        }}), 10), 
      Arrays.asList("Lisa"));
    
    assertResumes(s, s.search(childJoinQuery, // intersecting with children yields empty
        new QueryWrapperFilter( new TermQuery(new Term("skill", "java"))), 10), 
      Arrays.<String>asList());
    
    r.close();
    dir.close();
  }

  private void assertResumes(IndexSearcher s, TopDocs result,
      final List<String> devNames) throws IOException {
    final TopDocs javers = result;
    assert javers.totalHits ==devNames.size() : ""+Arrays.toString(javers.scoreDocs);
    Set<String> actJavers=new HashSet<String>();
    for(ScoreDoc sd: javers.scoreDocs){
      final boolean mute = actJavers.add(s.doc(sd.doc).get("name"));
      assert mute:actJavers+" "+sd+" "+s.doc(sd.doc);
    }
    assert actJavers.equals(new HashSet<String>(devNames));
  }
}
