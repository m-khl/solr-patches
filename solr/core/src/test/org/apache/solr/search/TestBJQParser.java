package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

//NOTE: reproduce with: ant test -Dtestcase=TestBJQParser -Dtestmethod=testBJQSearch -Dtests.seed=55474f6aa7a4414d:-6c6a11617e8f7570:-7bff551291d09884 -Dargs="-Dfile.encoding=UTF-8"
//  ant test -Dtestcase=TestBJQParser -Dtestmethod=testBJQSearch -Dtests.seed=-6cef409da642331e:550998f804a3558e:462b90f3158b3a0b -Dargs="-Dfile.encoding=UTF-8"
//ant test -Dtestcase=TestBJQParser -Dtestmethod=testIntersectBqBjq -Dtests.seed=3ef098603c177b37:b8cc54ff0f4f4a2:-5a2fedb31cb5d205 -Dargs="-Dfile.encoding=UTF-8"
public class TestBJQParser extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
      initCore("solrconfig-bjqparser.xml", "schema.xml");
      createIndex();
    }

    public static void createIndex() {
        assertU(delQ("*:*"));
        assertU(optimize());
        
        List<List<String>> blocks = new ArrayList<List<String>>();
        for(String parent:new String[]{"a","b","c","d", "e","f"}){
            List<String> block = new ArrayList<String>();
            for(String child:new String[]{"a","b","c"}){
                block.add(
                        add(doc("child_s",child, "parentchild_s",parent+child), "overwrite", "false")
                );
            }
            Collections.shuffle(block, random);
            block.add(
                    add(doc("parent_s",parent), "overwrite", "false"));
            blocks.add(block);
        }
        Collections.shuffle(blocks, random);
        for(List<String> block: blocks){
            for(String doc: block){
                assertU(doc);
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
    }
    
    @Test
    public void testFull() throws IOException, Exception{
        
        assertQ(req("q","{!parent filter=\"parent_s:[* TO *]\"}child_s:b"), 
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
                "chq","child_s:b",
                "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
              "//doc/arr[@name=\"parent_s\"]/str='b'",
              "//doc/arr[@name=\"parent_s\"]/str='e'"
              );
    }

    @Test
    public void testIntersectParentBqChildBq() throws IOException, Exception {
        System.out.println(h.query(req("parent_s:(a e b)")));
        System.out.println(h.query(req("parentchild_s:(bc ea cb)")));
        assertQ(req("q","+parent_s:(a e b) +_query_:\"{!parent filter=$pq v=$chq}\"",
                "chq","parentchild_s:(bc ea cb)",
                "pq","parent_s:[* TO *]"), 
                "//*[@numFound='2']",
                "//doc/arr[@name=\"parent_s\"]/str='b'",
                "//doc/arr[@name=\"parent_s\"]/str='e'"
        );
    }
}
