package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBJQParser extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
      initCore("solrconfig-bjqparser.xml", "schema.xml");
      createIndex();
    }

    public static void createIndex() {
        assertU(delQ("*:*"));
        assertU(optimize());
        
        List<List<String[]>> blocks = new ArrayList<List<String[]>>();
        for(String parent:new String[]{"a","b","c","d", "e","f"}){
            List<String[]> block = new ArrayList<String[]>();
            for(String child:new String[]{"a","b","c"}){
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
//                System.out.println("write "+Arrays.toString(doc) );
                assertU(add(doc(doc), "overwrite", "false"));
                i++;
            }
            if(random.nextBoolean()){
                //System.out.println("---<commit>---");
                assertU(commit());
                // force empty segment
                if(random.nextBoolean()){
                  //  System.out.println("---<second commit>---");
                    assertU(commit());
                }
            }
        }
//        System.out.println("---<tail commit>---");
        assertU(commit());
        assertQ(req("q","*:*"),"//*[@numFound='"+i+"']");
    }
    
    @Test
    public void testFull() throws IOException, Exception{
        String childb = "{!parent filter=\"parent_s:[* TO *]\"}child_s:b";
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
    public void testIntersectBqBjq() {
        assertQ(req("q","+parent_s:(e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","child_s:b",    "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']","//doc/arr[@name=\"parent_s\"]/str='b'","//doc/arr[@name=\"parent_s\"]/str='e'"
              );
        assertQ(req("q","{!parent filter=$pq v=$chq}",
                "fq","parent_s:(e b)",
                "chq","child_s:b",    "pq","parent_s:[* TO *]"//,"debugQuery","on"
                ),
                "//*[@numFound='2']","//doc/arr[@name=\"parent_s\"]/str='b'","//doc/arr[@name=\"parent_s\"]/str='e'"
              );
        assertQ(req("fq","{!parent filter=$pq v=$chq}\"",
                "q","parent_s:(e b)",
                "chq","child_s:b",    "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']","//doc/arr[@name=\"parent_s\"]/str='b'","//doc/arr[@name=\"parent_s\"]/str='e'"
              );
        assertQ(req("q","*:*",
                "fq","{!parent filter=$pq v=$chq}\"",
                "fq","parent_s:(e b)",
                "chq","child_s:b",    "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']","//doc/arr[@name=\"parent_s\"]/str='b'","//doc/arr[@name=\"parent_s\"]/str='e'"
              );
    }

    //ant test -Dtestcase=TestBJQParser -Dtestmethod=testIntersectParentBqChildBq -Dtests.seed=-3d90f64a8203aba1:43618ac49ba9ef16:377f9fde6d390767 -Dargs="-Dfile.encoding=UTF-8"
    @Test
    public void testIntersectParentBqChildBq() throws IOException, Exception {
        System.out.println(h.query(req("q","parent_s:(a e b)","wt","csv","sort","_docid_ asc",
                "fl","parent_s,child_s,parentchild_s")));
        System.out.println(h.query(req("q","parentchild_s:(bc ea cb)","wt","csv","sort","_docid_ asc",
                "fl","parent_s,child_s,parentchild_s")));
        assertQ(req("q","+parent_s:(a e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","parentchild_s:(bc ea cb)",
                "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
        );
    }
}
