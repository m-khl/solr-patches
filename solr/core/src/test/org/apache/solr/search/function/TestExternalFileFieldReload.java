package org.apache.solr.search.function;

import java.io.IOException;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.ExternalFileFieldReloader;
import org.apache.solr.search.*;
import static org.apache.solr.search.function.TestFunctionQuery.*;
import org.apache.solr.search.SolrIndexSearcher.QueryCommand;
import org.apache.solr.search.SolrIndexSearcher.QueryResult;
import org.junit.Before;
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
 
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-functionquery.xml","schema11.xml");
  }
  
  @Before
  public void clearCacheAndindex(){
    SolrQueryRequest req = req("*:*");
    ((SolrCache) req.getCore().getInfoRegistry().get("queryResultCache")).clear();
    assertU(delQ("*:*"));
    req.close();
  }
  
  public void testSingleFieldReload() throws IOException, Exception{
    float[] ids = {100,-4,0,10,25,5,77,23,55,-78,-45,-24,63,78,94,22,34,54321,261,-627};

    makeExternalFile("foo_extf", "54321=1\n0=2\n25=3","UTF-8");
    makeExternalFile("bar_extf", "54321=10\n0=20\n25=30","UTF-8");

    createIndex(null,ids);
   
    SolrQueryRequest req = req("*:*");
    try{

    Object fooKey = FileFloatSource.getFieldValueSourceKey(req, "foo_extf");
    Object barKey = FileFloatSource.getFieldValueSourceKey(req, "bar_extf");

    HitTracker tracker = new HitTracker(req); 
    
    singleTest("foo_extf", "\0", 54321, 1, 0,2, 25,3);
    tracker.assertNoHit();
    singleTest("bar_extf", "\0", 54321, 10, 0,20, 25,30);
    tracker.assertNoHit();
    
    Object fooFloats = FileFloatSource.getCachedValue(req, fooKey);
    Object barFloats = FileFloatSource.getCachedValue(req, barKey);
    
    makeExternalFile("foo_extf", "54321=4\n0=5\n25=6","UTF-8");
    makeExternalFile("bar_extf", "54321=40\n0=50\n25=60","UTF-8");
    
    singleTest("foo_extf", "\0", 54321, 1, 0,2, 25,3);
    tracker.assertHit();
    assertSame("despite the file is written same data is expected",
            fooFloats, FileFloatSource.getCachedValue(req, fooKey));
    singleTest("bar_extf", "\0", 54321, 10, 0,20, 25,30);
    tracker.assertHit();
    assertSame("despite the file is written same data is expected",
            barFloats, FileFloatSource.getCachedValue(req, barKey));
    
    boolean isFooReload; 
    boolean isBarReload;
    if(random().nextBoolean()){
      isFooReload = random().nextBoolean();
      isBarReload = !isFooReload;
      reload(isFooReload ? "foo_extf": "bar_extf");
    }else{// legacy reloads
      if(random().nextBoolean()){
        assertU(h.query("/reloadCache", lrf.makeRequest("*:*")));
        req.close();//  it commits!
        req = req("*:*");
      }else{
        ExternalFileFieldReloader reloader = new ExternalFileFieldReloader(req.getCore());
        reloader.init(new NamedList());
        assertU(commit());
        SolrQueryRequest newReq = req("*:*");
        reloader.newSearcher(newReq.getSearcher(), req.getSearcher());
        req.close();
        req=newReq;
      }
      tracker = new HitTracker(req); 
      isFooReload = true;
      isBarReload = true;
    }
    
    singleTest("foo_extf", "\0", isFooReload ? f(54321, 4, 0,5, 25,6) : f(54321, 1, 0,2, 25,3));
    if(isFooReload)
      tracker.assertNoHit();
    else
      tracker.assertHit();
    
    singleTest("bar_extf", "\0", isBarReload ? f(54321, 40, 0,50, 25,60) : f(54321, 10, 0,20, 25,30));
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
  
  
  /**let's see how it will warm cached query */
  public void testWarming() throws ParseException, IOException{
    createIndex(null,11,12,13,14,15);

    SolrQueryRequest req = req("*:*");
    try{
      makeExternalFile("foo_extf", "11=1\n12=2\n13=3\n14=4\n15=5","UTF-8");
      Query fooQ = parse(req, "foo_extf");
      Query thirteenthDoc = QParser.getParser("id:13", null, req).getQuery();
      boolean fillQueryResultCache = random().nextBoolean();
      float thirteenthDocScore = fillQueryResultCache 
          ? searchBySolr(req, fooQ, thirteenthDoc)
              :searchByLucene(req, fooQ, thirteenthDoc);
      assertEquals("whether we fill cache or not, score is the same", 3, thirteenthDocScore, 0.01);
      req.close();
      // it should be captured during warming
      makeExternalFile("foo_extf", "11=1\n12=2\n13=100\n14=4\n15=5","UTF-8");
      
      assertU(delQ("id:11"));
      assertU(commit()); // here warming should happen
      // lazy reading should capture this value
      makeExternalFile("foo_extf", "11=1\n12=2\n13=3000\n14=4\n15=5","UTF-8");
      
      req = req("*:*");
      HitTracker tracker = new HitTracker(req);
      float scoreBySolr = searchBySolr(req, fooQ, thirteenthDoc);
      if (fillQueryResultCache){
        assertEquals("warming rereads file early right after commit", 100 , scoreBySolr,0.01);
        tracker.assertHit();
      }else{
        assertEquals("no warming - file is readed lately", 3000., scoreBySolr,0.01);
        tracker.assertNoHit();
      }
      // funny thing is that new parsed query should give no hit - because it has new version 
      // but it should not re read the file!
      makeExternalFile("foo_extf", "11=1\n12=2\n13=666\n14=4\n15=5","UTF-8");
      singleTest("foo_extf", "\0", 13, fillQueryResultCache ? 100 : 3000);
      tracker.assertNoHit();
      singleTest("foo_extf", "\0", 13, fillQueryResultCache ? 100 : 3000);
      tracker.assertHit();
    }finally{
      req.close();
    }
  }

  private float searchByLucene(SolrQueryRequest req, Query fooQ,
      Query filter) throws IOException {
    return req.getSearcher().search(fooQ,
        new QueryWrapperFilter(filter), 100).scoreDocs[0].score;
  }
  
  private float searchBySolr(SolrQueryRequest req, Query fooQ,
      Query thirteenthDocFilter) throws IOException {
    QueryCommand qc = new QueryCommand();
        qc.setQuery(fooQ)
          .setFilterList(thirteenthDocFilter)
          .setSort(Sort.RELEVANCE)
          .setOffset(0)
          .setLen(10)
          .setFlags(SolrIndexSearcher.GET_SCORES);
        QueryResult qr = new QueryResult();
        req.getSearcher().search(qr,qc);
    DocList docList = qr.getDocList(); 
    DocIterator iterator = docList.iterator();
    iterator.next();
    return iterator.score();
  }
  
  private Query parse(SolrQueryRequest req, String field) throws ParseException{
    return QParser.getParser("{!func}"+field, null, req).getQuery();
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
}
