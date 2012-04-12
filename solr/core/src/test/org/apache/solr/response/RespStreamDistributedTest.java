package org.apache.solr.response;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.ResponseStreamingTest.CounterCallback;

public class RespStreamDistributedTest extends BaseDistributedSearchTestCase {
  
  @Override
  public void doTest() throws Exception {
    del( "*:*" ); // delete everything!
    
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    Digits digits = new Digits(0);
    int twos=0;
    for(int i=10; i<100;i++){
      digits.set(i);
      index("id", i , "cat_s", digits.dump());
      for(int d : digits){
        if(d==2){
          twos++;
          break; // skip second 2 in 22
        }
      }
    }
    
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    CounterCallback callback = new CounterCallback();
    params.add("q", "cat_s:2");
    params.add("qt","response-streaming");
    params.add("shards.qt","response-streaming");
    params.add("response-streaming","true");
    params.add("sort","_docid_ asc");
    params.add("fl","id");
    QueryResponse rsp = controlClient.queryAndStreamResponse(params, callback);
    assertEquals(twos, callback.getCount());
    assertNull(rsp.getResults());
    
    setDistributedParams(params);
    
 // query a random server
    int which = r.nextInt(clients.size());
    SolrServer client = clients.get(which);
    final CounterCallback callback2 = new CounterCallback();
    QueryResponse shardsRsp = client.queryAndStreamResponse(params, callback2);
    assertEquals(twos, callback2.getCount());
    assertNull(shardsRsp.getResults());
    
    /*query("q","cat_s:2", 
        "qt","response-streaming",
        "response-streaming","true",
        "sort","_docid_ asc",
        "fl","id");*/
  }
  
  public JettySolrRunner createJetty(File baseDir, String dataDir) throws Exception {
    return createJetty(baseDir, dataDir, null, "solrconfig-response-streaming.xml");
  }
}
