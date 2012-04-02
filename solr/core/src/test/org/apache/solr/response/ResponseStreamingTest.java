package org.apache.solr.response;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResponseStreamingTest extends SolrJettyTestBase {
    
    private static int twos = 0;
    private static int oneSeven = 0;

    private static final class CounterCallback extends StreamingResponseCallback {
      private int count;
      private int lastId=-1;
      
      @Override
      public void streamSolrDocument(SolrDocument doc) {
        Integer id = Integer.valueOf(doc.getFieldValue("id").toString());
        assertTrue(id>lastId);
        lastId = id;
        count++;
      }
      
      @Override
      public void streamDocListInfo(long numFound, long start, Float maxScore) {
      }
      
      public int getCount() {
        return count;
      }
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
       createJetty(new File(ExternalPaths.EXAMPLE_HOME, "../../core/src/test-files/solr").getAbsolutePath(), 
               "solrconfig-response-streaming.xml", null);
       
       String url = "http://localhost:"+port+context;
       CommonsHttpSolrServer gserver = new CommonsHttpSolrServer( url );
       
       gserver.deleteByQuery( "*:*" ); // delete everything!
       
       ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
       for(int i=1; i<100;i++){
           SolrInputDocument doc = new SolrInputDocument();
           doc.addField("id", i );
           final int j = i;
           final List<Integer> digits = new Digits(j).dump();
           
           if(digits.contains(2)){
             twos ++;
           }
           
           if(digits.contains(1) && digits.contains(7)){
             oneSeven++;
           }
           
           doc.addField("cat_s", digits);
           docs.add(doc);
           
           if(!docs.isEmpty() && (i % 10 == 0 || !(i+1<100))){
               gserver.add(docs);
               docs.clear();
           }
       }
       if(!docs.isEmpty()){
         gserver.add(docs);
         docs.clear();
       }

       gserver.commit();
    }
    
    @Test
    public void testTwosStreaming() throws SolrServerException, IOException{
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("qt","response-streaming");
        params.add("response-streaming","true");
        params.add("fl","id");
        
        params.set("q","cat_s:2");
        CounterCallback callback = new CounterCallback();
        QueryResponse rsp = getSolrServer().queryAndStreamResponse(params, callback);
        
        assertEquals(twos, callback.getCount());
        assertNull(rsp.getResults());
    }
    
    @Test
    public void testTwos() throws SolrServerException, IOException{
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("fl","id");
        params.set("q","cat_s:2");
        QueryResponse rsp = getSolrServer().query(params);
        assertEquals(twos, rsp.getResults().getNumFound());
    }
    
    @Test
    public void testOneSevenStreaming() throws SolrServerException, IOException{
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("qt","response-streaming");
        params.add("response-streaming","true");
        params.add("fl","id");
        
        params.set("q","+cat_s:1 +cat_s:7");
        CounterCallback callback = new CounterCallback();
        final QueryResponse rsp = getSolrServer().queryAndStreamResponse(params, callback);
        
        assertEquals(oneSeven, callback.getCount());
        assertNull(rsp.getResults());
    }
    
    @Test
    public void ttestOneSeven() throws SolrServerException, IOException{
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("fl","id");
        params.set("q","+cat_s:1 +cat_s:7");
        QueryResponse rsp = getSolrServer().query(params);
        assertEquals(oneSeven, rsp.getResults().getNumFound());
    }
}
