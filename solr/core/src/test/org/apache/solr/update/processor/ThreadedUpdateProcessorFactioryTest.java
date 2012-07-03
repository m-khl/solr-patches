package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.HashMap;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class ThreadedUpdateProcessorFactioryTest extends SolrTestCaseJ4 {
  
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-update-processor-chains.xml", "schema12.xml");
  }
  
  @Before
  public void cleanup() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
  }
  
  @Test(timeout=10000)
  public void testBasic() throws Exception {
    
    boolean inboundCommit = random().nextBoolean();
    StringBuilder sb = new StringBuilder("<update><add>");
    final int hundred = 100;
    final int badDoc = random().nextBoolean() ? -1 : random().nextInt(hundred);
    for(int id=0;id<hundred;id++){
      if(badDoc==id){
        if(random().nextBoolean()){ // field format is violated
          sb.append(doc("id",""+(id+hundred*hundred), "name_i", "bad"));
        }else{// PK is ommitted
          sb.append(doc("idbad","bad", "name", "bad"));
        }
      }
      sb.append(doc("id",""+(id), "name_s", ""+(id+1)));
      
    }
   sb.append("</add> " +
   		(inboundCommit ? "<commit/>" : "" )+
   		"</update>");
   final String chainName = new String[]{"smallBufferThreads", "smallThreads", "normalThreads" }[random().nextInt(3)];
    MapSolrParams params = new MapSolrParams(new HashMap<String,String>(){{
        put("update.chain", chainName);
        put("backing.chain",random().nextBoolean()?"log-and-run":"log-delay-run");
      }});
    ((ThreadedUpdateProcessorFactory)h.getCore().getUpdateProcessingChain(chainName).getFactories()[0])
        .offerTimeout=new int[]{0,1,10}[random().nextInt(3)];
    String xml = sb.toString();
    if(badDoc==-1){
      assertU("sending many docs",xml,params);
      if(!inboundCommit){
        assertU(commit());
      }
      assertQ(req("*:*"), "//*[@numFound='"+hundred+"']");
      assertQ(req("name_s:1"), "//*[@numFound='1']");
      assertQ(req("name_s:"+hundred), "//*[@numFound='1']");
      assertQ(req("name_s:"+(random().nextInt(hundred-2)+2)), "//*[@numFound='1']");
      assertQ(req("name_s:bad"), "//*[@numFound='0']");
    }else{ // failed doc isn't indexed, exception is propagated, some of remain docs are written
      // it partially mimics single thread behavior when docs before wrong one are written, 
      // and later are ignored. exactly such behavior can't be provided for multiple threads
      assertFailedU("sending many docs with bad guy",xml,params);
    }
  }
  
  @Test
  public void testThreadsBehindThreads() throws Exception {
    assertFailedU("chaining threads is illegal, i don't know why", adoc("id","8990"),
        new MapSolrParams(new HashMap<String,String>(){{
          put("update.chain", "normalThreads");
          put("backing.chain","smallThreads");
        }})
    );
  }
  
  @Test
  public void testPreventsRegularChaining() throws Exception {
    assertFailedU("use backing.chain instead", adoc("id","8990"),
        new MapSolrParams(new HashMap<String,String>(){{
          put("update.chain", "wrongThreads");
        }})
    );
  }
  
  
  
  @Test(expected=IllegalStateException.class)
  public void testMisThreading() throws IOException, InterruptedException{
    MapSolrParams params = new MapSolrParams(new HashMap<String,String>(){{
      put("update.chain", "normalThreads");
      put("backing.chain","log-and-run");
    }});
    final SolrQueryRequest req = req(params);
    final UpdateRequestProcessorChain chain = h.getCore().getUpdateProcessingChain("normalThreads");
    final UpdateRequestProcessor proc[] = new UpdateRequestProcessor[1];
    Thread thread = new Thread(){
      public void run() {
        proc[0] = chain.createProcessor(req, new SolrQueryResponse());
      };
    };
    thread.start();
    thread.join();
    switch(random().nextInt(5)){
      case 0:
        proc[0].processAdd(new AddUpdateCommand(req));
      break;
      case 1:
        proc[0].processCommit(new CommitUpdateCommand(req, false));
      break;
      case 2:
        proc[0].processDelete(new DeleteUpdateCommand(req));
      break;
      case 3:
        proc[0].processMergeIndexes(new MergeIndexesCommand(null, req));
      break;
      case 4:
        proc[0].processRollback(new RollbackUpdateCommand(req));
      break;
    }
  }
  
  
  public static class DelayProcessorFactory extends UpdateRequestProcessorFactory {
    
    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req,
        SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new UpdateRequestProcessor( next) {
        @Override
        public void processAdd(AddUpdateCommand cmd) throws IOException {
          try {
            Thread.sleep(random().nextInt(90)+10);
          } catch (InterruptedException e) {
            throw new IOException("wrapper",e);
          }
          super.processAdd(cmd);
        }
      };
    }
    
  }

}


