package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestBJQParser extends SolrTestCaseJ4 {


    @BeforeClass
    public static void beforeClass() throws Exception {
        String oldCacheNamePropValue = 
                System.getProperty("blockJoinParentFilterCache");
        System.setProperty("blockJoinParentFilterCache", 
                random.nextBoolean() ? "blockJoinParentFilterCache" : "don't cache");
        
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
    
    @Test
    public void testFull() throws IOException, Exception{
        String childb = "{!parent filter=\"parent_s:[* TO *]\"}child_s:l";
        assertQ(req("q",childb), 
                "//*[@numFound='6']"
              ,"//doc/arr[@name=\"parent_s\"]/str='a'",
              "//doc/arr[@name=\"parent_s\"]/str='b'",
              "//doc/arr[@name=\"parent_s\"]/str='c'",
              "//doc/arr[@name=\"parent_s\"]/str='d'",
              "//doc/arr[@name=\"parent_s\"]/str='e'",
              "//doc/arr[@name=\"parent_s\"]/str='f'"
              );
    }
    
    @Test
    public void testJustParentsFilter() throws IOException {
        assertQ(req("q", "{!parent filter=\"parent_s:[* TO *]\"}"), 
                "//*[@numFound='6']"
              ,"//doc/arr[@name=\"parent_s\"]/str='a'",
              "//doc/arr[@name=\"parent_s\"]/str='b'",
              "//doc/arr[@name=\"parent_s\"]/str='c'",
              "//doc/arr[@name=\"parent_s\"]/str='d'",
              "//doc/arr[@name=\"parent_s\"]/str='e'",
              "//doc/arr[@name=\"parent_s\"]/str='f'"
              );
    }
    
    @Test
    public void testIntersectBqBjq() {
        assertQ(req("q","+parent_s:(e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
              );
        assertQ(req("fq","{!parent filter=$pq v=$chq}\"",
                "q","parent_s:(e b)",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
              );
        
        assertQ(req("q","*:*",
                "fq","{!parent filter=$pq v=$chq}\"",
                "fq","parent_s:(e b)",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
              );
    }
    
    @Ignore("until BJQ supports filtered search")
    @Test
    public void testFq() {
        assertQ(req("q","{!parent filter=$pq v=$chq}",
                "fq","parent_s:(e b)",
                "chq","child_s:l",    "pq","parent_s:[* TO *]"//,"debugQuery","on"
                ),
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
              );

        boolean qfq = random.nextBoolean();
        assertQ(req(qfq ? "q":"fq","parent_s:(a e b)",
                  (!qfq)? "q":"fq", "{!parent filter=$pq v=$chq}",
                "chq","parentchild_s:(bm ek cl)",
                "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
        );
        
    }

    @Test
    public void testIntersectParentBqChildBq() throws IOException {
        
        assertQ(req("q","+parent_s:(a e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","parentchild_s:(bm ek cl)",
                "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
        );
    }
    
}
