package org.apache.solr;

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

import java.util.Random;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.ScoreJoinQParserPlugin;
import org.apache.solr.search.SolrCache;
import org.junit.BeforeClass;

public class TestScoreJoinQPScore extends SolrTestCaseJ4 {

  private static final String idField = "id";
  private static final String toField = "movieId_s";

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    // TODO badass
    QParserPlugin.standardPlugins[QParserPlugin.standardPlugins.length-2]=ScoreJoinQParserPlugin.NAME;
    QParserPlugin.standardPlugins[QParserPlugin.standardPlugins.length-1]=ScoreJoinQParserPlugin.class;
    initCore("solrconfig.xml","schema12.xml");
  }

  public void testSimple() throws Exception {
    final String idField = "id";
    final String toField = "productId_s";

    clearIndex();

    // 0
    assertU(add(doc("t_description", "random text",
        "name", "name1",
        idField, "1")));

// 1
    
    assertU(add(doc("price_s", "10.0",
        idField, "2",
        toField, "1")));
// 2
    assertU(add(doc("price_s", "20.0",
        idField, "3", 
        toField, "1")));
// 3
    assertU(add(doc("t_description", "more random text",
        "name", "name2",
        idField, "4")));
// 4
    assertU(add(doc("price_s", "10.0",
        idField, "5", 
        toField, "4")));
// 5
    assertU(add(doc("price_s", "20.0",
        idField, "6",
        toField, "4")));

    assertU(commit());
    
    // Search for product
    assertJQ(req("q","{!scorejoin from="+idField+" to="+toField+" score=None}name:name2", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'5'},{'id':'6'}]}" );
    
    /*Query joinQuery =
        JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);

    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(4, result.scoreDocs[0].doc);
    assertEquals(5, result.scoreDocs[1].doc);
    */
     assertJQ(req("q","{!scorejoin from="+idField+" to="+toField+" score=None}name:name1", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'2'},{'id':'3'}]}" );

    /*joinQuery = JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name1")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(1, result.scoreDocs[0].doc);
    assertEquals(2, result.scoreDocs[1].doc);*/

    // Search for offer
     assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=None}id:5", "fl","id")
         ,"/response=={'numFound':1,'start':0,'docs':[{'id':'4'}]}" );
    /*joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "5")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);

    indexSearcher.getIndexReader().close();
    dir.close();*/
  }

  public void testSimpleWithScoring() throws Exception {
    indexDataForScorring();

    // Search for movie via subtitle
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:random", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'1'},{'id':'4'}]}" );
    //dump(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:random", "fl","id,score", "debug", "true"));
    /*
    Query joinQuery =
        JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "random")), indexSearcher, ScoreMode.Max);
    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(0, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);*/

    
    // Score mode max.
    //dump(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:movie", "fl","id,score", "debug", "true"));
    
   // dump(req("q","title:movie", "fl","id,score", "debug", "true"));
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'4'},{'id':'1'}]}" );
    
    /*joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Max);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);
    assertEquals(0, result.scoreDocs[1].doc);*/
    
    // Score mode total
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Total}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'1'},{'id':'4'}]}" );
  /*  joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Total);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(0, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);
*/
    //Score mode avg
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Avg}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'4'},{'id':'1'}]}" );
    
  /*  joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Avg);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);
    assertEquals(0, result.scoreDocs[1].doc);*/
  }

  public void testCacheHit() throws Exception {
    indexDataForScorring();
    
    SolrCache cache = (SolrCache) h.getCore().getInfoRegistry()
        .get("queryResultCache");
    {
    final NamedList statPre = cache.getStatistics();

    assertJQ(req("q","{!scorejoin from=movieId_s to=id score=Avg}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'4'},{'id':'1'}]}" );
    
    assertEquals("it lookups", 1,
        delta("lookups", cache.getStatistics(), statPre));
    final long mayHit = delta("hits", cache.getStatistics(), statPre);
    assertTrue("it may hit", 0==mayHit || 1==mayHit);
    assertEquals("or insert on cold", 1,
        delta("inserts", cache.getStatistics(), statPre)+mayHit);
    }
    
    {
      final NamedList statPre = cache.getStatistics();

      assertJQ(req("q","{!scorejoin from=movieId_s to=id score=Avg}title:movie", "fl","id")
          ,"/response=={'numFound':2,'start':0,'docs':[{'id':'4'},{'id':'1'}]}" );
      
      assertEquals("it lookups", 1,
          delta("lookups", cache.getStatistics(), statPre));
      assertEquals("it hits",1, delta("hits", cache.getStatistics(), statPre));
      assertEquals("it doesn't insert", 0,
          delta("inserts", cache.getStatistics(), statPre));
    }
    
    {
      final NamedList statPre = cache.getStatistics();

      Random r = random();
      boolean changed = false;
      boolean x =false ;
      String from = (x=r.nextBoolean()) ? "id":"movieId_s";
      changed |= x;
      String to = (x=r.nextBoolean()) ? "movieId_s" : "id";
      changed |= x;
      String score = (x=r.nextBoolean()) ? new ScoreMode[]{ScoreMode.Max,ScoreMode.None, ScoreMode.Total}[r.nextInt(3)].name():"Avg";
      changed |= x;
      String q = changed?"title:movie":"title:random";
      h.query(req("q","{!scorejoin from="+from+" to="+to+" score="+score+"}"+q, "fl","id")
          );
      
      assertEquals("it lookups", 1,
          delta("lookups", cache.getStatistics(), statPre));
      assertEquals("it doesn't hit",0, delta("hits", cache.getStatistics(), statPre));
      assertEquals("it inserts", 1,
          delta("inserts", cache.getStatistics(), statPre));
    }
  }
  
  private long delta(String key, NamedList a, NamedList b) {
    return (Long) a.get(key) - (Long) b.get(key);
  }
  
  private void indexDataForScorring() {
    clearIndex();
// 0
    assertU(add(doc("t_description", "A random movie",
    "name", "Movie 1",
    idField, "1")));
// 1
    
    assertU(add(doc("title", "The first subtitle of this movie", 
    idField, "2",
    toField, "1")));
    

// 2
    
    assertU(add(doc("title", "random subtitle; random event movie", 
    idField, "3", 
    toField, "1"))); 

// 3
    
    assertU(add(doc("t_description", "A second random movie", 
    "name", "Movie 2", 
    idField, "4")));
// 4
    
    assertU(add(doc("title", "a very random event happened during christmas night",
    idField, "5",
    toField, "4")));
    

// 5
    
    assertU(add(doc("title", "movie end movie test 123 test 123 random", 
    idField, "6", 
    toField, "4"))); 
    

    assertU(commit());
  }
  
  private SolrQueryRequest dump(SolrQueryRequest req) throws Exception {
    System.out.println(req);
    System.out.println(h.query(req));
    return req;
  }
}
