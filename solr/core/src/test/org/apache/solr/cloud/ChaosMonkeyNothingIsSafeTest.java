package org.apache.solr.cloud;

/*
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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.HttpClient;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.update.SolrCmdDistributor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

@Slow
@SuppressSSL
@ThreadLeakLingering(linger = 60000)
public class ChaosMonkeyNothingIsSafeTest extends AbstractFullDistribZkTestBase {
  private static final int FAIL_TOLERANCE = 20;

  public static Logger log = LoggerFactory.getLogger(ChaosMonkeyNothingIsSafeTest.class);
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
    SolrCmdDistributor.testing_errorHook = new Diagnostics.Callable() {
      @Override
      public void call(Object... data) {
        Exception e = (Exception) data[0];
        if (e == null) return;
        if (e.getMessage().contains("Timeout")) {
          Diagnostics.logThreadDumps("REQUESTING THREAD DUMP DUE TO TIMEOUT: " + e.getMessage());
        }
      }
    };
  }
  
  @AfterClass
  public static void afterSuperClass() {
    SolrCmdDistributor.testing_errorHook = null;
  }
  
  protected static final String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  protected static final RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};
  
  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // can help to hide this when testing and looking at logs
    //ignoreException("shard update error");
    System.setProperty("numShards", Integer.toString(sliceCount));
    useFactory("solr.StandardDirectoryFactory");
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("numShards");
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public ChaosMonkeyNothingIsSafeTest() {
    super();
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "2"));
    shardCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.shardcount", "7"));
  }
  
  @Override
  public void doTest() throws Exception {
    boolean testsSuccesful = false;
    try {
      handle.clear();
      handle.put("QTime", SKIPVAL);
      handle.put("timestamp", SKIPVAL);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      // make sure we have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
      }      // make sure we again have leaders for each shard
      
      waitForRecoveriesToFinish(false);
      
      // we cannot do delete by query
      // as it's not supported for recovery
       del("*:*");
      
      List<StopableThread> threads = new ArrayList<>();
      int threadCount = TEST_NIGHTLY ? 3 : 1;
      int i = 0;
      for (i = 0; i < threadCount; i++) {
        StopableIndexingThread indexThread = new StopableIndexingThread(controlClient, cloudClient, Integer.toString(i), true);
        threads.add(indexThread);
        indexThread.start();
      }
      
      threadCount = 1;
      i = 0;
      for (i = 0; i < threadCount; i++) {
        StopableSearchThread searchThread = new StopableSearchThread();
        threads.add(searchThread);
        searchThread.start();
      }
      
      // TODO: we only do this sometimes so that we can sometimes compare against control,
      // it's currently hard to know what requests failed when using ConcurrentSolrUpdateServer
      boolean runFullThrottle = random().nextBoolean();
      if (runFullThrottle) {
        FullThrottleStopableIndexingThread ftIndexThread = new FullThrottleStopableIndexingThread(
            clients, "ft1", true);
        threads.add(ftIndexThread);
        ftIndexThread.start();
      }
      
      chaosMonkey.startTheMonkey(true, 10000);
      try {
        long runLength;
        if (RUN_LENGTH != -1) {
          runLength = RUN_LENGTH;
        } else {
          int[] runTimes;
          if (TEST_NIGHTLY) {
            runTimes = new int[] {5000, 6000, 10000, 15000, 25000, 30000,
                30000, 45000, 90000, 120000};
          } else {
            runTimes = new int[] {5000, 7000, 15000};
          }
          runLength = runTimes[random().nextInt(runTimes.length - 1)];
        }
        
        Thread.sleep(runLength);
      } finally {
        chaosMonkey.stopTheMonkey();
      }
      
      for (StopableThread indexThread : threads) {
        indexThread.safeStop();
      }
      
      // start any downed jetties to be sure we still will end up with a leader per shard...
      
      // wait for stop...
      for (StopableThread indexThread : threads) {
        indexThread.join();
      }
      
      // try and wait for any replications and what not to finish...
      
      Thread.sleep(2000);
      
      // wait until there are no recoveries...
      waitForThingsToLevelOut(Integer.MAX_VALUE);//Math.round((runLength / 1000.0f / 3.0f)));
      
      // make sure we again have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 30000);
      }
      
      commit();
      
      // TODO: assert we didnt kill everyone
      
      zkStateReader.updateClusterState(true);
      assertTrue(zkStateReader.getClusterState().getLiveNodes().size() > 0);
      
      
      // we expect full throttle fails, but cloud client should not easily fail
      for (StopableThread indexThread : threads) {
        if (indexThread instanceof StopableIndexingThread && !(indexThread instanceof FullThrottleStopableIndexingThread)) {
          assertFalse("There were too many update fails - we expect it can happen, but shouldn't easily", ((StopableIndexingThread) indexThread).getFailCount() > FAIL_TOLERANCE);
        }
      }
      
      // full throttle thread can
      // have request fails 
      checkShardConsistency(!runFullThrottle, true);
      
      long ctrlDocs = controlClient.query(new SolrQuery("*:*")).getResults()
      .getNumFound(); 
      
      // ensure we have added more than 0 docs
      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*"))
          .getResults().getNumFound();
      
      assertTrue("Found " + ctrlDocs + " control docs", cloudClientDocs > 0);
      
      if (VERBOSE) System.out.println("control docs:"
          + controlClient.query(new SolrQuery("*:*")).getResults()
              .getNumFound() + "\n\n");
      
      // try and make a collection to make sure the overseer has survived the expiration and session loss

      // sometimes we restart zookeeper as well
      if (random().nextBoolean()) {
        zkServer.shutdown();
        zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
        zkServer.run();
      }
      
      CloudSolrServer client = createCloudClient("collection1");
      try {
          createCollection(null, "testcollection",
              1, 1, 1, client, null, "conf1");

      } finally {
        client.shutdown();
      }
      List<Integer> numShardsNumReplicas = new ArrayList<>(2);
      numShardsNumReplicas.add(1);
      numShardsNumReplicas.add(1);
      checkForCollection("testcollection",numShardsNumReplicas, null);
      
      testsSuccesful = true;
    } finally {
      if (!testsSuccesful) {
        printLayout();
      }
    }
  }

  class FullThrottleStopableIndexingThread extends StopableIndexingThread {
    private HttpClient httpClient = HttpClientUtil.createClient(null);
    private volatile boolean stop = false;
    int clientIndex = 0;
    private ConcurrentUpdateSolrServer suss;
    private List<SolrServer> clients;  
    private AtomicInteger fails = new AtomicInteger();
    
    public FullThrottleStopableIndexingThread(List<SolrServer> clients,
        String id, boolean doDeletes) {
      super(controlClient, cloudClient, id, doDeletes);
      setName("FullThrottleStopableIndexingThread");
      setDaemon(true);
      this.clients = clients;
      HttpClientUtil.setConnectionTimeout(httpClient, 15000);
      HttpClientUtil.setSoTimeout(httpClient, 15000);
      suss = new ConcurrentUpdateSolrServer(
          ((HttpSolrServer) clients.get(0)).getBaseURL(), httpClient, 8,
          2) {
        @Override
        public void handleError(Throwable ex) {
          log.warn("suss error", ex);
        }
      };
    }
    
    @Override
    public void run() {
      int i = 0;
      int numDeletes = 0;
      int numAdds = 0;

      while (true && !stop) {
        String id = this.id + "-" + i;
        ++i;
        
        if (doDeletes && random().nextBoolean() && deletes.size() > 0) {
          String delete = deletes.remove(0);
          try {
            numDeletes++;
            suss.deleteById(delete);
          } catch (Exception e) {
            changeUrlOnError(e);
            //System.err.println("REQUEST FAILED:");
            //e.printStackTrace();
            fails.incrementAndGet();
          }
        }
        
        try {
          numAdds++;
          if (numAdds > (TEST_NIGHTLY ? 4002 : 197))
            continue;
          SolrInputDocument doc = getDoc(
              "id",
              id,
              i1,
              50,
              t1,
              "Saxon heptarchies that used to rip around so in old times and raise Cain.  My, you ought to seen old Henry the Eight when he was in bloom.  He WAS a blossom.  He used to marry a new wife every day, and chop off her head next morning.  And he would do it just as indifferent as if ");
          suss.add(doc);
        } catch (Exception e) {
          changeUrlOnError(e);
          //System.err.println("REQUEST FAILED:");
          //e.printStackTrace();
          fails.incrementAndGet();
        }
        
        if (doDeletes && random().nextBoolean()) {
          deletes.add(id);
        }
        
      }
      
      System.err.println("FT added docs:" + numAdds + " with " + fails + " fails" + " deletes:" + numDeletes);
    }

    private void changeUrlOnError(Exception e) {
      if (e instanceof ConnectException) {
        clientIndex++;
        if (clientIndex > clients.size() - 1) {
          clientIndex = 0;
        }
        suss.shutdownNow();
        suss = new ConcurrentUpdateSolrServer(
            ((HttpSolrServer) clients.get(clientIndex)).getBaseURL(),
            httpClient, 30, 3) {
          @Override
          public void handleError(Throwable ex) {
            log.warn("suss error", ex);
          }
        };
      }
    }
    
    @Override
    public void safeStop() {
      stop = true;
      suss.blockUntilFinished();
      suss.shutdownNow();
      httpClient.getConnectionManager().shutdown();
    }

    @Override
    public int getFailCount() {
      return fails.get();
    }
    
    @Override
    public Set<String> getAddFails() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Set<String> getDeleteFails() {
      throw new UnsupportedOperationException();
    }
    
  };
  
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
  
}
