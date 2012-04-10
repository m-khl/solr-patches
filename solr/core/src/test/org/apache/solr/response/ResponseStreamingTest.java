package org.apache.solr.response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.LargeVolumeTestBase.DocThread;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResponseStreamingTest extends SolrJettyTestBase {
    
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

    static final class Digits implements Iterable<Integer> {
        
        private int i;

        Digits(int j) {
            this.i = j;
        }

        @Override
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>(){
                final int j = i; 
                int ord = 10;
                
                @Override
                public boolean hasNext() {
                    return j==0 ? ord/(1+j)<=10: ord/j<=10;
                }
                @Override
                public Integer next() {
                    int result = j % ord / (ord/10);
                    ord*=10;
                    return result;
                }
                @Override
                public void remove() {
                }
                
            };
        }
        
        List<Integer> dump(){
            List<Integer> l = new ArrayList<Integer>();
            for(Integer i: this){
                l.add(i);
            }
            return l; 
        }
        
        public void set(int k){
          i = k;
        }
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
       createJetty(new File(ExternalPaths.EXAMPLE_HOME, "../../core/src/test-files/solr").getAbsolutePath(), 
               "solrconfig-response-streaming.xml", null);
    }
    
    @Test
    public void testSample() throws SolrServerException, IOException{
        SolrServer gserver = this.getSolrServer();
        gserver.deleteByQuery( "*:*" ); // delete everything!
        
        int twos = 0;
        int oneSeven = 0;
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
        
        // some of the commits could have failed because maxWarmingSearchers exceeded,
        // so do a final commit to make sure everything is visible.
        gserver.commit();
        
        CommonsHttpSolrServer solr = new CommonsHttpSolrServer("http://localhost:"+port+"/solr");
        
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("qt","response-streaming");
        //params.add("q","*:*");
        params.add("response-streaming","true");
        params.add("fl","id");
        
        //System.out.println(solr.query( params ).getResponse());
        params.set("q","cat_s:2");
        CounterCallback callback = new CounterCallback();
        QueryResponse rsp = solr.queryAndStreamResponse(params, callback);
        
        assertEquals(twos, callback.getCount());
        assertNull(rsp.getResults());
        
        params.set("q","+cat_s:1 +cat_s:7");
        callback = new CounterCallback();
        solr.queryAndStreamResponse(params, callback);
        
        assertEquals(oneSeven, callback.getCount());
        assertNull(rsp.getResults());
    }
    
    @Test
    public void testDigits(){
        assertEquals(Arrays.asList(1,4,3),  new Digits(341).dump());
        assertEquals(Arrays.asList(0),  new Digits(0).dump());
        assertEquals(Arrays.asList(1),  new Digits(1).dump());
        assertEquals(Arrays.asList(2),  new Digits(2).dump());

        assertEquals(Arrays.asList(0,1),  new Digits(10).dump());
        assertEquals(Arrays.asList(0,2),  new Digits(20).dump());        
        assertEquals(Arrays.asList(2,2),  new Digits(22).dump());
        assertEquals(Arrays.asList(1,0,1),  new Digits(101).dump());
        assertEquals(Arrays.asList(0,0,0,1),  new Digits(1000).dump());
        assertEquals(Arrays.asList(0,1,0,1),  new Digits(1010).dump());

    }

}
