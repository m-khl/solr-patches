package org.apache.solr.search.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.ExternalFileField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.function.FileFloatSource.Data;
import org.junit.BeforeClass;

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

public class TestExternalFileFieldReload extends SolrTestCaseJ4 {
 
  /*
  * * reload test plan:
 *   same fields twice in single Q. expect no race and versions in one q the same
 *   reload not yet unloaded 
 *     field isn't loaded, trigger reload. check field is loaded
  * */
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-functionquery.xml","schema11.xml");
  }
  
  /*
   *  *   vanilla:
 *     two fields 
 *     check they both are cached
 *     update both files 
 *     trigger reload first one
 *     check one field is the same, one is reloded
 *     trigger reload second
 *     check that cached field is unchanged, and reload is happened 
   * */
  public void testSingleFieldReload() throws IOException, Exception{
    float[] ids = {100,-4,0,10,25,5,77,23,55,-78,-45,-24,63,78,94,22,34,54321,261,-627};

    assertU(delQ("*:*"));
    TestFunctionQuery.createIndex(null,ids);
   
    SolrQueryRequest req = req("*:*");
      
    TestFunctionQuery.makeExternalFile("foo_extf", "54321=1\n0=2\n25=3","UTF-8");
    TestFunctionQuery.makeExternalFile("bar_extf", "54321=10\n0=20\n25=30","UTF-8");
    
    try{
    Object fooKey = FileFloatSource.getFieldValueSourceKey(req, "foo_extf");
    Object barKey = FileFloatSource.getFieldValueSourceKey(req, "bar_extf");

    HitTracker tracker = new HitTracker(req); 
    
    TestFunctionQuery.singleTest("foo_extf", "\0", 54321, 1, 0,2, 25,3);
    tracker.assertNoHit();
    TestFunctionQuery.singleTest("bar_extf", "\0", 54321, 10, 0,20, 25,30);
    tracker.assertNoHit();
    
    Object fooFloats = FileFloatSource.getCachedValue(req, fooKey);
    Object barFloats = FileFloatSource.getCachedValue(req, barKey);
    
    TestFunctionQuery.makeExternalFile("foo_extf", "54321=4\n0=5\n25=6","UTF-8");
    TestFunctionQuery.makeExternalFile("bar_extf", "54321=40\n0=50\n25=60","UTF-8");
    
    assertSame("despite the file is written same data is expected",
            fooFloats, FileFloatSource.getCachedValue(req, fooKey));
    assertSame("despite the file is written same data is expected",
            barFloats, FileFloatSource.getCachedValue(req, barKey));
    
    boolean isFooReload; 
    boolean isBarReload;
    if(usually()){
      isFooReload = random().nextBoolean();
      isBarReload = !isFooReload;
      reload(isFooReload ? "foo_extf": "bar_extf");
    }else{
      isFooReload = true;
      isBarReload = true;
      assertU(h.query("/reloadCache", lrf.makeRequest("*:*")));
    }
    
    TestFunctionQuery.singleTest("foo_extf", "\0", isFooReload ? f(54321, 4, 0,5, 25,6) : f(54321, 1, 0,2, 25,3));
    if(isFooReload)
      tracker.assertNoHit();
    else
      tracker.assertHit();
    
    TestFunctionQuery.singleTest("bar_extf", "\0", isBarReload ? f(54321, 40, 0,50, 25,60) : f(54321, 10, 0,20, 25,30));
    if(isBarReload)
      tracker.assertNoHit();
    else
      tracker.assertHit();
    
    assertEquals(isFooReload, fooFloats != FileFloatSource.getCachedValue(req, fooKey) );
    assertEquals(isBarReload, barFloats != FileFloatSource.getCachedValue(req, barKey) );
    }finally{
      req.close();
    }
  }

  private static float[] f(float ... sugar){
    return sugar;
  }
  
  private static class HitTracker {
    private long lookups;
    private long hits;
    private SolrQueryRequest req;

    HitTracker(SolrQueryRequest req){
      this.req = req;
      NamedList statistics = stats(req);
      lookups = (Long) statistics.get("lookups");
      hits = (Long) statistics.get("hits");
    }

    private NamedList stats(SolrQueryRequest req) {
      SolrInfoMBean cache = req.getCore().getInfoRegistry().get("queryResultCache");
      NamedList statistics = cache.getStatistics();
      return statistics;
    }
    
    void assertHit(){
      assertIncs(1,  1);
    }

    void assertNoHit(){
      assertIncs(1,  0);
    }
    
    private void assertIncs(long lookupsInc, long hitsInc) {
      NamedList statistics = stats(req);
      long newLookups = (Long) statistics.get("lookups");
      long newHits = (Long) statistics.get("hits");
      assertEquals("expects lookups are "+(lookupsInc==0 ? "the same" : "increased on "+lookupsInc),  lookups+lookupsInc, newLookups);
      assertEquals("expects hits are "+(hitsInc==0 ? "the same" : "increased on "+hitsInc),  hits+hitsInc , newHits);
      lookups = newLookups;
      hits = newHits;
    }
  }
  
  
  public void testReloadDuringSingleQueryParsing() throws ParseException, IOException{
    
    assertU(delQ("*:*"));
    TestFunctionQuery.createIndex(null,0,1,2,3,4,5,6,7,8,9);
    TestFunctionQuery.makeExternalFile("baz_extf", "0=0\n1=1\n2=2\n3=3\n4=4\n5=5\n6=6\n7=7\n8=8\n9=9","UTF-8");
    
    SolrQueryRequest req = req("*:*");
    try{
      Query beforeReload = QParser.getParser("_query_:\"{!func}baz_extf\" *:* _query_:\"{!func}baz_extf\"", null, req).getQuery();
      req.close(); req = req("*:*");
      TestFunctionQuery.makeExternalFile("baz_extf", "0=0\n1=10\n2=20\n3=30\n4=40\n5=50\n6=60\n7=70\n8=80\n9=90","UTF-8");
      
      Query duringReload = QParser.getParser("_query_:\"{!func}baz_extf\" _query_:\"{!testtrap}baz_extf\" _query_:\"{!func}baz_extf\"", null, req).getQuery();
      req.close(); req = req("*:*");
      
      Query afterReload = QParser.getParser("_query_:\"{!func}baz_extf\" *:* _query_:\"{!func}baz_extf\"", null, req).getQuery();
      req.close(); req = req("*:*");
      // check low level ptrs
      assertSame("floats across a single request are not aware of reload", getFloats(duringReload, 0), getFloats(duringReload, 2));
      assertNotSame("floats across requests are aware of reload", getFloats(duringReload, 0), getFloats(afterReload, 2));
      
      assertEquals("reloading file doesn't cause the diff", beforeReload, duringReload);
      assertEquals("same for hashes", beforeReload.hashCode(), duringReload.hashCode());
      
      assertFalse("reloading file causes the diff", duringReload.equals(afterReload));
      assertFalse("reloading file causes the diff", beforeReload.equals(afterReload));
      // sorry, ain't gonna test meaningful scores here, because score is hard to compute due to weighting
    }finally{
      req.close();
    }
  }

  public float[] getFloats(Query duringReload, int numClause) {
    return ((FileFloatSource)((FunctionQuery)((BooleanQuery)duringReload).getClauses()[numClause].getQuery()).getValueSource()).values.target;
  }
  
  public void testReloadColdCahce() throws IOException, Exception {
    assertU(delQ("*:*"));
    TestFunctionQuery.createIndex(null,11,12,13,14,15);
    TestFunctionQuery.makeExternalFile("foo_extf", "11=1\n12=2\n13=3\n14=4\n15=5","UTF-8");
    
    SolrQueryRequest req = req("*:*");
    try{
      Object fooKey = FileFloatSource.getFieldValueSourceKey(req, "foo_extf");
      try{
        FileFloatSource.getCachedValue(req, fooKey).toString();
        fail();
      }catch (Exception e) { 
        assertTrue("expect empty cache before any hit", e instanceof NullPointerException);
      }
      if(random().nextBoolean()){
        reload("foo_extf");
      }else{
        parse(req, "foo_extf");
      }
      // take array
      float[] cachedValue = FileFloatSource.getCachedValue(req, FileFloatSource.getFieldValueSourceKey(req, "foo_extf"));
      // spoil the file
      TestFunctionQuery.makeExternalFile("foo_extf", "11=66\n12=66\n13=66\n14=66\n15=66","UTF-8");
      // hit value cached before
      Object  valueBefore = FileFloatSource.onlyForTesting;
      TestFunctionQuery.singleTest("foo_extf", "\0", 13, 3);
      assertSame("hitting q doesn't cause reload",FileFloatSource.onlyForTesting, valueBefore);
      
      req.close(); req = req("*:*");
      // look up it from the cache again
      float[] cachedValue2 = FileFloatSource.getCachedValue(req, FileFloatSource.getFieldValueSourceKey(req, "foo_extf"));
      assertSame("no matter how it's loaded, core data is the same",cachedValue, cachedValue2);
      
    }finally{
      req.close();
    }
  }
  
  /** single q mention two fields that doesn't cause req.ctx cache hit
   * @throws ParseException */
  public void testTwoFieldsAcrossCommits() throws ParseException{
    assertU(delQ("*:*"));
    TestFunctionQuery.createIndex(null,101,101,102,103,104,105,106,107,108,109);
    
    SolrQueryRequest req = req("*:*");
    try{
      String contents = "0=0\n101=1\n102=2\n103=3\n104=4\n105=5\n106=6\n107=7\n108=8\n109=9";
      TestFunctionQuery.makeExternalFile("foo_extf", contents,"UTF-8");
      TestFunctionQuery.makeExternalFile("bar_extf", contents,"UTF-8");
      assertEquals("same field is equal", parse(req, "foo_extf"),parse(req, "foo_extf"));
      Query barQ;
      Query fooQ;
      assertFalse("different fields are not equal",(fooQ=parse(req, "foo_extf")).equals(barQ=parse(req, "bar_extf")));
      req.close(); req = req("*:*");
      assertEquals("equality hase place across requests", parse(req, "foo_extf"),fooQ);
      assertEquals("equality hase place across requests", parse(req, "bar_extf"),barQ);
      req.close();
      assertU(adoc("id","666"));
      assertU(commit());
      req = req("*:*");
      assertFalse("but not across the commits",fooQ.equals(parse(req, "foo_extf")));
      assertFalse("but not across the commits",barQ.equals(parse(req, "bar_extf")));
      
      assertEquals("check essential again", parse(req, "foo_extf"),parse(req, "foo_extf"));
      assertFalse("and again",parse(req, "foo_extf").equals(parse(req, "bar_extf")));
    }finally{
      req.close();
    }
 
  } 
  /**let's see how it will warm cached query
   * @throws ParseException 
   * @throws IOException */
  public void testWarming() throws ParseException, IOException{
    assertU(delQ("*:*"));
    TestFunctionQuery.createIndex(null,11,12,13,14,15);
    TestFunctionQuery.makeExternalFile("foo_extf", "11=1\n12=2\n13=3\n14=4\n15=5","UTF-8");
    SolrQueryRequest req = req("*:*");
    try{
      Query fooQ = parse(req, "foo_extf");
      assertEquals(3, 
      req.getSearcher().search(fooQ,
          new QueryWrapperFilter(QParser.getParser("id:13", null, req).getQuery()), 100).scoreDocs[0].score, 0.01);
      req.close();
      
      assertU(delQ("id:11"));
      assertU(commit());
      
      TestFunctionQuery.makeExternalFile("foo_extf", "11=1\n12=2\n13=3000\n14=4\n15=5","UTF-8");
      
      req = req("*:*");
      assertEquals(3000, 
          req.getSearcher().search(fooQ,
              new QueryWrapperFilter(QParser.getParser("id:13", null, req).getQuery()), 100).scoreDocs[0].score,0.01);
    }finally{
      req.close();
    }
  }
  
  private Query parse(SolrQueryRequest req, String field) throws ParseException{
    return QParser.getParser("{!func}"+field, null, req).getQuery();
  }
  
  public static class ReloadFieldQParserPlugin extends QParserPlugin{

    @Override
    public void init(NamedList args) {}

    @Override
    public QParser createParser(String qstr, SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      return new QParser(qstr,localParams, params, req) {
        @Override
        public Query parse() throws ParseException {
          try {
            if(random().nextBoolean())
              TestExternalFileFieldReload.reload(qstr);
            else
              TestExternalFileFieldReload.reloadInThread(qstr);
              
          } catch (Exception e) {
            throw new RuntimeException();
          }
          return new MatchAllDocsQuery();
        }
      };
    }
    
  }

  protected static void reload(String fieldName) throws IOException, Exception {
    assertU(h.query("/reloadField", lrf.makeRequest("field", fieldName)));
  }

  protected static void reloadInThread(final String qstr) throws Exception {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          reload(qstr);
        } catch (IOException e) {
          throw new RuntimeException();
        } catch (Exception e) {
          throw new RuntimeException();
        }
      }
    });
    thread.start();
    thread.join();
  }
 
}
