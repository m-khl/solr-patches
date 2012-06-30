package org.apache.solr.update.processor;

import java.util.HashMap;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.XML;
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
  
  @Test
  public void testBasic() throws Exception {
    
    boolean inboundCommit = random().nextBoolean();
    StringBuilder sb = new StringBuilder("<update><add>");
    final int hundred = 100;
    final int badDoc = random().nextBoolean() ? -1 : random().nextInt(hundred);
    for(int id=0;id<hundred;id++){
      if(badDoc==id){
        sb.append(doc("idead_s",""+(id), "name_s", "bad"));
      }
      sb.append(doc("id",""+(id), "name_s", ""+(id+1)));
      
    }
   sb.append("</add> " +
   		(inboundCommit ? "<commit/>" : "" )+
   		"</update>");
    MapSolrParams params = new MapSolrParams(new HashMap<String,String>(){{
        put("update.chain", new String[]{"smallBufferThreads", "smallThreads", "normalThreads" }[random().nextInt(3)]);
        put("backing.chain","log-and-run");
      }});
    
    String xml = sb.toString();
    if(badDoc==-1){
      assertU("sending many docs",xml,params);
    }else{ // failed doc won't indexed, exception is propagated, but remain docs are written
      // it partially mimics single thread behavior - docs before wrong one are written, 
      // and later are ignored. such behavior can't be provided for multiple threads
      assertFailedU("sending many docs with bad guy",xml,params);
    }
    if(!inboundCommit){
      assertU(commit());
    }
    assertQ(req("*:*"), "//*[@numFound='"+hundred+"']");
    assertQ(req("name_s:1"), "//*[@numFound='1']");
    assertQ(req("name_s:"+hundred), "//*[@numFound='1']");
    assertQ(req("name_s:"+(random().nextInt(hundred-2)+2)), "//*[@numFound='1']");
    assertQ(req("name_s:bad"), "//*[@numFound='0']");
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
}


