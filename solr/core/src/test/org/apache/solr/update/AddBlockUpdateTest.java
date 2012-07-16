package org.apache.solr.update;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

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

public class AddBlockUpdateTest extends SolrTestCaseJ4 {

  private static final String child = "child_s";
  private static final String parent = "parent_s";
  private static ExecutorService exe;
  private static AtomicInteger counter = new AtomicInteger();
  
  
  private RefCounted<SolrIndexSearcher> searcherRef;
  private SolrIndexSearcher _searcher;
    
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    exe = //Executors.newSingleThreadExecutor();
        rarely() ? Executors.newFixedThreadPool(atLeast(2)) :
            Executors.newCachedThreadPool()
        ; 
  }
  
  @Before
  public void prepare(){
    //assertU("<rollback/>");
    assertU(delQ("*:*"));
    assertU(commit("expungeDeletes","true"));
    
  }
  
  private SolrIndexSearcher getSearcher(){
    if(_searcher==null){
      searcherRef = h.getCore().getSearcher();
      _searcher = searcherRef.get();
    }
    return _searcher;
  }
  
  @After
  public void cleanup(){
    if(searcherRef!=null || _searcher!=null){
      searcherRef.decref();
      searcherRef = null;
      _searcher=null;
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    exe.shutdownNow();
  }
  
  @Test
  public void testBasics() throws Exception {
    List<String> blocks = new ArrayList<String>(
        Arrays.asList(
            block("abcD") , 
            rarely() ? commit(): "",
            block("efgH"),
            merge(block("ijkL"),block("mnoP")),
            merge(block("qrsT"),block("uvwX")),
            (rarely() ? commit(): ""),
            block("Y"),
            block("Z"),
            block(""),
            rarely() ? commit(): ""
            )
        ); 
    Collections.shuffle(blocks);
    
    log.trace("{}",blocks);
    
    for(Future<Void> f : exe.invokeAll(callables(blocks))){
      f.get(); // exceptions? 
    }  
    
    assertU(commit());
    
    final SolrIndexSearcher searcher = getSearcher();
      //final String resp = h.query(req("q","*:*", "sort","_docid_ asc", "rows", "10000"));
      //log.trace(resp);
      final int parentsNum = "DHLPTXYZ".length();
      assertQ(req(parent+":[* TO *]"),"//*[@numFound='" + parentsNum + "']");
      assertQ(req(child+":[* TO *]"),"//*[@numFound='" + (('z'-'a'+1)-parentsNum) + "']");
      assertQ(req("*:*"),"//*[@numFound='"+('z'-'a'+1)+"']");
      assertSingleParentOf(searcher, one("abc"), "D");
      assertSingleParentOf(searcher, one("efg"), "H");
      assertSingleParentOf(searcher, one("ijk"), "L");
      assertSingleParentOf(searcher, one("mno"), "P");
      assertSingleParentOf(searcher, one("qrs"), "T");
      assertSingleParentOf(searcher, one("uvw"), "X");
  }

  @Test 
  public void testSmallBlockDirect() throws Exception {
    final AddBlockCommand cmd = new AddBlockCommand(req("*:*"));
    final List<SolrInputDocument> docs = Arrays.asList(new SolrInputDocument(){{
      addField("id", id());
      addField(child, "a");
    }},
    new SolrInputDocument(){{
      addField("id", id());
      addField(parent, "B");
    }});
    cmd.setDocs(docs);
    assertEquals(2, h.getCore().getUpdateHandler().addBlock(cmd));
    assertU(commit());
    
    final SolrIndexSearcher searcher = getSearcher();
      assertQ(req("*:*"),"//*[@numFound='2']");
      assertSingleParentOf(searcher, one("a"), "B");
  }
  
  @Test 
  public void testEmptyDirect() throws Exception {
    final AddBlockCommand cmd = new AddBlockCommand(req("*:*"));
    // let's add empty one
    cmd.setDocs(Collections.<SolrInputDocument>emptyList());
    assertEquals(0, h.getCore().getUpdateHandler().addBlock(cmd));
    assertU(commit());
    
      assertQ(req("*:*"),"//*[@numFound='0']");
  }
  
  @Test
  public void testExeptionThrown() throws Exception {
    final String abcD = block("abcD");
    log.trace(abcD);
    assertBlockU(abcD);
     
     assertFailedBlockU("<add>"+
         rawDoc(
             "id",id(), 
             parent,"Y",
             "sample_i","notanumber",
             "subdocs",
                  ""+doc("id",id(), child,"x"))+
         rawDoc(
             "id",id(),
             parent,"W")
     +"</add>");
     assertBlockU(block("efgH"));
     assertBlockU(commit());
     
     final SolrIndexSearcher searcher = getSearcher();
     assertQ(req("*:*"),"//*[@numFound='"+"abcDefgHx".length()+"']");
     assertSingleParentOf(searcher, one("abc"), "D");
     assertSingleParentOf(searcher, one("efg"), "H");
     // i believe I'll find child x; but not Y W,
     // but it's not really consistent behaviour!! 
     assertQ(req(child+":x"),"//*[@numFound='1']");
     
     assertQ(req(parent+":Y"),"//*[@numFound='0']");
     assertQ(req(parent+":W"),"//*[@numFound='0']");
     
  }
  
  @Test
  public void testSolrJXML() throws IOException{
    UpdateRequest req = new UpdateRequest();
    
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(Arrays.asList(
      new SolrInputDocument(){{
      addField("id", id());
      addField("parent_s", "X");
      ArrayList<SolrInputDocument> ch1 = new ArrayList<SolrInputDocument>(Arrays.asList(
        new SolrInputDocument(){{
          addField("id",id());
          addField("child_s","y");
        }},
        new SolrInputDocument(){{
          addField("id",id());
          addField("child_s","z");
        }}
      ));
      Collections.shuffle(ch1, random());
      addField("children",  ch1);
     }},
     new SolrInputDocument(){{
       addField("id", id());
       addField("parent_s", "A");
       addField("children",  new ArrayList<SolrInputDocument>(Arrays.asList(
           new SolrInputDocument(){{
             addField("id",id());
             addField("child_s","b");
           }}
         )));
       addField("children2",  
           new SolrInputDocument(){{
             addField("id",id());
             addField("child_s","c");
           }}
         );
      }}));
    Collections.shuffle(docs, random());
    req.add(docs);
    
    RequestWriter requestWriter = new RequestWriter();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    requestWriter.write(req, os);
    assertBlockU(os.toString());
    assertU(commit());
    
    final SolrIndexSearcher searcher = getSearcher();
    assertSingleParentOf(searcher, one("yz"), "X");
    assertSingleParentOf(searcher, one("bc"), "A");
  }
  
  private String merge(String block, String block2) {
    return (block+block2).replace("</add><add>", "");
  }

  private static String id() {
    return ""+counter.incrementAndGet();
  }

  private String one(String string) {
    return ""+string.charAt(random().nextInt(string.length()));
  }

  protected void assertSingleParentOf(final SolrIndexSearcher searcher,
      final String childTerm, String parentExp) throws IOException {
    final TopDocs docs = searcher.search(join(childTerm), 10);
    assertEquals(1, docs.totalHits);
    final String pAct = searcher.doc(docs.scoreDocs[0].doc).get(parent);
    assertEquals(parentExp , pAct);
  }

  protected ToParentBlockJoinQuery join(final String childTerm) {
    return new ToParentBlockJoinQuery(new TermQuery(new Term(child,childTerm)),
        new TermRangeFilter(parent, null, null, false, false), ScoreMode.None);
  }

  private Collection<? extends Callable<Void>> callables(List<String> blocks) {
    final List<Callable<Void>> rez = new ArrayList<Callable<Void>>();
    for(String updXml:blocks ){
      final String msg = updXml;
      if(msg.length()>0){
        rez.add(new Callable<Void>() {
          @Override
          public Void call() {
              assertBlockU(msg);
            return null;
          }

          
        });
      }
    }
    return rez;
  }

  private static void assertBlockU(final String msg) {
    assertBlockU(msg, "0");
  }
  
  private static void assertFailedBlockU(final String msg) {
    try {
      assertBlockU(msg, "1");
      fail("expecting fail");
    }catch(Exception e){
      // gotcha
    }
  }

  private static void assertBlockU(final String msg, String expected) {
    try {
         String res = h.checkUpdateStatus(msg, expected, "/updateFlatten");
         if (res != null) {
           fail("update was not successful: " + res+" expected: "+expected);
         }
    } catch (SAXException e) {
      throw new RuntimeException("Invalid XML", e);
    }
  }
  
  /**
   *   on the given abcD it generates one parent doc, taking D from the tail 
   *   and two subdocs relaitons ab and c 
   *   uniq ids are supplied also 
   *   
   *       // how to keep tags in javadoc? 
   *       
   *   <add>
   *     <doc>
   *       <field name="parent_s">D</field>
   *       <field name="1th-subdocs">
   *         <doc> <field name="child_s">a</field> </doc>
   *         <doc>  <field name="child_s">b</field>  </doc>
   *       </field>
   *       <field name="2th-subdocs">
   *         <doc> <field name="child_s">c</field> </doc>
   *       </field>
   *     </doc>
   *   </add>
   * */
  private String block(String string) {
    final String tag = "add";
    final StringBuilder sb = new StringBuilder();
    sb.append("<"+tag+">");
    
    if(string.length()>0){
      List<String> args= new ArrayList<String>(Arrays.asList(
              parent,""+string.charAt(string.length()-1),
              "id", id())
      );
      // add fields with subdocs
      int ord=0;
      for(int i=0; i<string.length()-1; i+=2){
        args.add(""+(++ord)+"th-subdocs");
        final String relation = string.substring(i, Math.min(i+2,string.length()-1));
        args.add(toSubDocs(relation));
      }
      sb.append(rawDoc(args.toArray(new String[]{})));
    }
    sb.append("</"+tag+">");
    return sb.toString();
  }

  private String toSubDocs(final String relation) {
    StringBuilder subb = new StringBuilder();
    for(int j=0;j<relation.length();j++){
      subb.append(doc(child,""+relation.charAt(j),
        "id", id()));
    }
    return subb.toString();
  }
  /**
   * kinda doc(String...) but does not escape xml, allows to insert <doc/> 
   * as field vales   
   * @return <doc>
   * */
  private String rawDoc(String ... fieldsAndValues) {
    final StringBuffer ssb = new StringBuffer();
    ssb.append("<doc>");
    for (int i = 0; i < fieldsAndValues .length; i+=2) {
      ssb.append("<field name=\""); ssb.append(fieldsAndValues[i]);ssb.append("\">");
        ssb.append(fieldsAndValues[i+1]);
      ssb.append("</field>");
    }
    ssb.append("</doc>");
    return ssb.toString();
  }
}
