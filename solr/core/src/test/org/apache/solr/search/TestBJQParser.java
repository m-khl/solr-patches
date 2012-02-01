package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
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
                assertU(add(doc(doc), "overwrite", "false"));
                i++;
            }
            if(random.nextBoolean()){
                assertU(commit());
                assertQ(req("q","*:*"),"//*[@numFound='"+i+"']");
                String childb = "{!parent filter=\"parent_s:[* TO *]\"}child_s:"+(new String[]{"a","b","c"})[random.nextInt(3)];
                assertQ(req("q", childb),"//doc/arr[@name=\"parent_s\"]/str='"+
                        block.get(block.size()-1)[1]
                        +"'");
                // force empty segment
                if(random.nextBoolean()){
                    assertU(commit());
                    assertQ(req("q","*:*"),"//*[@numFound='"+i+"']");
                    assertQ(req("q", childb),"//doc/arr[@name=\"parent_s\"]/str='"+
                            block.get(block.size()-1)[1]
                            +"'");
                }
            }
        }
        assertU(commit());
        assertQ(req("q","*:*"),"//*[@numFound='"+i+"']");
    }
    
    @Test
    public void testFull() throws IOException, Exception{
        //NOTE: reproduce with: ant test -Dtestcase=TestBJQParser -Dtestmethod=testFull -Dtests.seed=7350714397afc6c6:633a39acc51ae8e2:-76de92440be16af2 -Dargs="-Dfile.encoding=UTF-8"
        System.out.println(h.query(req("q","*:*",
                "sort","_docid_ asc",
                "fl","parent_s,child_s,parentchild_s",
                "rows","100",
                "wt","csv")));
        
        assertQ(req("q","parent_s:a"),"//*[@numFound='1']");
        assertQ(req("q","+child_s:b +parentchild_s:ab"),"//*[@numFound='1']");
        String childb = "{!parent filter=\"parent_s:[* TO *]\"}child_s:b";
        System.out.println(h.query(req("q",childb, 
                "sort","_docid_ asc",
                "fl","parent_s,child_s,parentchild_s",
                "rows","100",
                "wt","csv")));
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
