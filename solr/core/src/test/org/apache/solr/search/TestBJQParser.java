package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestBJQParser extends SolrTestCaseJ4 {

    private static boolean cachedMode;

    @BeforeClass
    public static void beforeClass() throws Exception {
        String oldCacheNamePropValue = 
                System.getProperty("blockJoinParentFilterCache");
        System.setProperty("blockJoinParentFilterCache", 
                (cachedMode = random.nextBoolean()) ? "blockJoinParentFilterCache" : "don't cache");
        
      initCore("solrconfig-bjqparser.xml", "schema.xml");

      if(oldCacheNamePropValue!=null){
          System.setProperty("blockJoinParentFilterCache", oldCacheNamePropValue);
      }
      createIndex();
    }

    
    public static void createIndex() {
        assertU(delQ("*:*"));
        assertU(optimize());
        
        List<List<String[]>> blocks = new ArrayList<List<String[]>>();
        for(String parent:new String[]{"a","b","c","d", "e","f"}){
            List<String[]> block = new ArrayList<String[]>();
            for(String child:new String[]{"k","l","m"}){
                block.add(
                        new String[]{"child_s",child, "parentchild_s",parent+child})
                ;
            }
            Collections.shuffle(block, random);
            addGrandChildren(block);
            block.add(new String[]{"parent_s",parent});
            blocks.add(block);
        }
        Collections.shuffle(blocks, random);
        int i=0;
        for(List<String[]> block: blocks){
            for(String[] doc: block){
                assertU(add(doc(doc), "overwrite", "false"));
                i++;
            }
            if(random.nextBoolean()){
                assertU(commit());
                // force empty segment
                if(random.nextBoolean()){
                    assertU(commit());
                }
            }
        }
        assertU(commit());
        assertQ(req("q","*:*"),"//*[@numFound='"+i+"']");
    }


    private static void addGrandChildren(List<String[]> block) {
        List<String> grandChildren = new ArrayList<String>(Arrays.asList("x","y","z"));
        for(ListIterator<String[]> iter = block.listIterator(); iter.hasNext();){
            String[] child = iter.next();
            String child_s = child[1];
            String parentchild_s = child[3];
            int grandChildPos=0;
            while( !grandChildren.isEmpty() && ( 
                    (grandChildPos = random.nextInt(grandChildren.size()*2)) < grandChildren.size() || 
                        (!iter.hasNext() && !grandChildren.isEmpty()))){
                grandChildPos = grandChildPos >=grandChildren.size() ? 0 : grandChildPos; 
               iter.add(new String[]{
                    "grand_s", grandChildren.remove(grandChildPos) , 
                        "grand_child_s",child_s, "grand_parentchild_s",parentchild_s
               });
            }
        }
        Collections.reverse(block);
    }
    
    @Test
    public void testFull() throws IOException, Exception{
        String childb = "{!parent filter=\"parent_s:[* TO *]\"}child_s:l";
        assertQ(req("q",childb), sixParents
              );
    }

    private static final String sixParents [] = new String[]{
        "//*[@numFound='6']"
        ,"//doc/arr[@name=\"parent_s\"]/str='a'",
        "//doc/arr[@name=\"parent_s\"]/str='b'",
        "//doc/arr[@name=\"parent_s\"]/str='c'",
        "//doc/arr[@name=\"parent_s\"]/str='d'",
        "//doc/arr[@name=\"parent_s\"]/str='e'",
    "//doc/arr[@name=\"parent_s\"]/str='f'"};
    
    @Test
    public void testJustParentsFilter() throws IOException {
        assertQ(req("q", "{!parent filter=\"parent_s:[* TO *]\"}"), 
                sixParents
              );
    }
    
    private final static String beParents []=  new String[]{
            "//*[@numFound='2']",
            "//doc/arr[@name=\"parent_s\"]/str='b'",
    "//doc/arr[@name=\"parent_s\"]/str='e'"}
    ;
    @Test
    public void testIntersectBqBjq() {
        
        assertQ(req("q","+parent_s:(e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"), 
                beParents 
              );
        assertQ(req("fq","{!parent filter=$pq v=$chq}\"",
                "q","parent_s:(e b)",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"), 
                beParents
              );
        
        assertQ(req("q","*:*",
                "fq","{!parent filter=$pq v=$chq}\"",
                "fq","parent_s:(e b)",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"), 
                beParents
              );
    }
    
    @Ignore("until BJQ supports filtered search")
    @Test
    public void testFq() {
        assertQ(req("q","{!parent filter=$pq v=$chq}",
                "fq","parent_s:(e b)",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"//,"debugQuery","on"
                ),
                beParents
              );

        boolean qfq = random.nextBoolean();
        assertQ(req(qfq ? "q":"fq","parent_s:(a e b)",
                  (!qfq)? "q":"fq", "{!parent filter=$pq v=$chq}",
                "chq","parentchild_s:(bm ek cl)",
                "pq","parent_s:[* TO *]"), 
                beParents
        );
        
    }

    @Test
    public void testIntersectParentBqChildBq() throws IOException {
        
        assertQ(req("q","+parent_s:(a e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","parentchild_s:(bm ek cl)",
                "pq","parent_s:[* TO *]"), 
                beParents
        );
    }
    
    @Test
    public void testGrandChildren() throws IOException {
        
    }
    
    @Test
    public void testCacheHit() throws IOException {
        
        SolrCache parentFilterCache = (SolrCache) h.getCore().getInfoRegistry().get("blockJoinParentFilterCache");
        
        SolrCache filterCache = (SolrCache) h.getCore().getInfoRegistry().get("filterCache");
        
        NamedList parentsBefore = parentFilterCache.getStatistics();
        
        NamedList filtersBefore =  filterCache.getStatistics();
        
        // it should be weird enough to be uniq
        String parentFilter = "parent_s:([a TO c] [d TO f])";
        
        assertQ("search by parent filter",req("q", "{!parent filter=\""+parentFilter +"\"}"), 
                "//*[@numFound='6']");
        
        assertQ("filter by parent filter", req("q","*:*",
                "fq", "{!parent filter=\""+parentFilter +"\"}"), 
                 "//*[@numFound='6']");
        
        assertEquals("didn't hit fqCache yet ", 0L, delta("lookups",filterCache.getStatistics(), filtersBefore ));
        
        assertQ("filter by join", req("q","*:*",
                "fq", "{!parent filter=\""+parentFilter +"\"}child_s:l"), 
                 "//*[@numFound='6']");
        
        if(cachedMode){
            assertEquals("in cache mode every request lookups", 3, delta("lookups", 
                                                                    parentFilterCache.getStatistics(),  parentsBefore));
            assertEquals("last two lookups causes hits", 2,  delta("hits", 
                                                                    parentFilterCache.getStatistics(),  parentsBefore));
            assertEquals("the first lookup gets insert", 1,  delta("inserts", 
                                                                    parentFilterCache.getStatistics(),  parentsBefore));
        }else{
            assertEquals("no one look into cache", 0L ,delta("lookups", 
                    parentFilterCache.getStatistics(),  parentsBefore));
            assertEquals("no one hit cache", 0L ,delta("hits", 
                    parentFilterCache.getStatistics(),  parentsBefore));
            assertEquals("no one insert into cache", 0L ,delta("inserts", 
                    parentFilterCache.getStatistics(),  parentsBefore));
        }
        
        assertEquals("true join query is cached in fqCache", 1L, delta("lookups",filterCache.getStatistics(), filtersBefore ));
    }
    
    private long delta(String key, NamedList a, NamedList b){
        return (Long) a.get(key) - (Long) b.get(key);
    }
    
    @Test
    public void emptyListInit() throws ParseException{ 
        
        SolrQueryRequest req = req("q", "*:*");
        try{
        
        assertEquals("empty init args works well",
                QParser.getParser("{!parent filter=\"parent_s:[* TO *]\"}child_s:l", "", req).getQuery(),
                QParser.getParser("{!parentemptyinit filter=\"parent_s:[* TO *]\"}child_s:l", "", req).getQuery());
        
        assertEquals("empty init args works well for parent filter mode too",
                QParser.getParser("{!parent filter=\"parent_s:[* TO *]\"}", "", req).getQuery(),
                QParser.getParser("{!parentemptyinit filter=\"parent_s:[* TO *]\"}", "", req).getQuery());
        }finally{
            req.close();
        }
    }
    
    @Test
    public void nullInit(){
        new BlockJoinParentQParserPlugin().init(null);
    }
}
