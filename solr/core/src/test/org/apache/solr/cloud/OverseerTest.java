package org.apache.solr.cloud;

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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.CoreState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OverseerTest extends SolrTestCaseJ4 {

  static final int TIMEOUT = 10000;
  private static final boolean DEBUG = false;

  
  public static class MockZKController{
    
    private final SolrZkClient zkClient;
    private final ZkStateReader zkStateReader;
    private final String nodeName;
    private final String collection;
    private final LeaderElector elector;
    private final Map<String, CoreState> coreStates = Collections.synchronizedMap(new HashMap<String, CoreState>());
    private final Map<String, ElectionContext> electionContext = Collections.synchronizedMap(new HashMap<String, ElectionContext>());
    
    public MockZKController(String zkAddress, String nodeName, String collection) throws InterruptedException, TimeoutException, IOException, KeeperException {
      this.nodeName = nodeName;
      this.collection = collection;
      zkClient = new SolrZkClient(zkAddress, TIMEOUT);
      zkStateReader = new ZkStateReader(zkClient);
      zkStateReader.createClusterStateWatchersAndUpdate();
      Overseer.createClientNodes(zkClient, nodeName);
      
      // live node
      final String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
      zkClient.makePath(nodePath, CreateMode.EPHEMERAL, true);
      elector = new LeaderElector(zkClient);
    }

    private void deleteNode(final String path) {
      try {
        Stat stat = zkClient.exists(path, null, false);
        if (stat != null) {
          zkClient.delete(path, stat.getVersion(), false);
        }
      } catch (KeeperException e) {
        fail("Unexpected KeeperException!" + e);
      } catch (InterruptedException e) {
        fail("Unexpected InterruptedException!" + e);
      }
    }

    public void close(){
      try {
        deleteNode(ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName);
        zkClient.close();
      } catch (InterruptedException e) {
        //e.printStackTrace();
      }
    }
    
    public void publishState(String coreName, String stateName, int numShards)
        throws KeeperException, InterruptedException, IOException {
      if (stateName == null) {
        coreStates.remove(coreName);
        ElectionContext ec = electionContext.remove(coreName);
        if (ec != null) {
          ec.cancelElection();
        }
      } else {
        HashMap<String,String> coreProps = new HashMap<String,String>();
        coreProps.put(ZkStateReader.STATE_PROP, stateName);
        coreProps.put(ZkStateReader.NODE_NAME_PROP, nodeName);
        coreProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
        coreProps.put(ZkStateReader.COLLECTION_PROP, collection);
        coreProps.put(ZkStateReader.BASE_URL_PROP, "http://" + nodeName
            + "/solr/");
        CoreState state = new CoreState(coreName, collection, coreProps,
            numShards);
        coreStates.remove(coreName);
        coreStates.put(coreName, state);
      }
      final String statePath = Overseer.STATES_NODE + "/" + nodeName;
      zkClient.setData(
          statePath,
          ZkStateReader.toJSON(coreStates.values().toArray(
              new CoreState[coreStates.size()])), true);
      
      for (int i = 0; i < 30; i++) {
        String shardId = getShardId(coreName);
        if (shardId != null) {
          try {
            zkClient.makePath("/collections/" + collection + "/leader_elect/"
                + shardId + "/election", true);
          } catch (NodeExistsException nee) {}
          ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
              "http://" + nodeName + "/solr/", ZkStateReader.NODE_NAME_PROP,
              nodeName, ZkStateReader.CORE_NAME_PROP, coreName,
              ZkStateReader.SHARD_ID_PROP, shardId,
              ZkStateReader.COLLECTION_PROP, collection);
          ShardLeaderElectionContextBase ctx = new ShardLeaderElectionContextBase(
              elector, shardId, collection, nodeName + "_" + coreName, props,
              zkStateReader);
          elector.joinElection(ctx);
          break;
        }
        Thread.sleep(200);
      }
    }
    
    private String getShardId(final String coreName) {
      Map<String,Slice> slices = zkStateReader.getCloudState().getSlices(
          collection);
      if (slices != null) {
        for (Slice slice : slices.values()) {
          if (slice.getShards().containsKey(nodeName + "_" + coreName)) {
            return slice.getName();
          }
        }
      }
      return null;
    }
  }    
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solrcloud.skip.autorecovery", "true");
    initCore();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solrcloud.skip.autorecovery");
    initCore();
  }

  @Test
  public void testShardAssignment() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);

    ZkController zkController = null;
    SolrZkClient zkClient = null;
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      zkClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "3");

      zkController = new ZkController(null, server.getZkAddress(), TIMEOUT, 10000,
          "localhost", "8983", "solr", new CurrentCoreDescriptorProvider() {

            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      System.setProperty("bootstrap_confdir", getFile("solr/conf")
          .getAbsolutePath());

      final int numShards=6;
      final String[] ids = new String[numShards];
      
      for (int i = 0; i < numShards; i++) {
        CloudDescriptor collection1Desc = new CloudDescriptor();
        collection1Desc.setCollectionName("collection1");
        CoreDescriptor desc1 = new CoreDescriptor(null, "core" + (i + 1), "");
        desc1.setCloudDescriptor(collection1Desc);
        zkController.preRegister(desc1);
        ids[i] = zkController.register("core" + (i + 1), desc1);
      }
      
      assertEquals("shard1", ids[0]);
      assertEquals("shard2", ids[1]);
      assertEquals("shard3", ids[2]);
      assertEquals("shard1", ids[3]);
      assertEquals("shard2", ids[4]);
      assertEquals("shard3", ids[5]);

      waitForCollections(reader, "collection1");
      
      //make sure leaders are in cloud state
      assertNotNull(reader.getLeaderUrl("collection1", "shard1", 15000));
      assertNotNull(reader.getLeaderUrl("collection1", "shard2", 15000));
      assertNotNull(reader.getLeaderUrl("collection1", "shard3", 15000));
      
    } finally {
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
      System.clearProperty("bootstrap_confdir");
      if (DEBUG) {
        if (zkController != null) {
          zkClient.printLayoutToStdOut();
        }
      }
      if (zkClient != null) {
        zkClient.close();
      }
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }
  }

  @Test
  public void testShardAssignmentBigger() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    final int nodeCount = random().nextInt(50)+50;   //how many simulated nodes (num of threads)
    final int coreCount = random().nextInt(100)+100;  //how many cores to register
    final int sliceCount = random().nextInt(20)+1;  //how many slices
    
    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    final ZkController[] controllers = new ZkController[nodeCount];
    final ExecutorService[] nodeExecutors = new ExecutorService[nodeCount];
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      zkClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, Integer.valueOf(sliceCount).toString());

      for (int i = 0; i < nodeCount; i++) {
      
      controllers[i] = new ZkController(null, server.getZkAddress(), TIMEOUT, 10000,
          "localhost", "898" + i, "solr", new CurrentCoreDescriptorProvider() {

            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });
      }

      System.setProperty("bootstrap_confdir", getFile("solr/conf")
          .getAbsolutePath());

      
      for (int i = 0; i < nodeCount; i++) {
        nodeExecutors[i] = Executors.newFixedThreadPool(1);
      }
      
      final String[] ids = new String[coreCount];
      //register total of coreCount cores
      for (int i = 0; i < coreCount; i++) {
        final int slot = i;
        Runnable coreStarter = new Runnable() {
          @Override
          public void run() {
            final CloudDescriptor collection1Desc = new CloudDescriptor();
            collection1Desc.setCollectionName("collection1");

            final String coreName = "core" + slot;
            
            final CoreDescriptor desc = new CoreDescriptor(null, coreName, "");
            desc.setCloudDescriptor(collection1Desc);
            try {
              controllers[slot % nodeCount].preRegister(desc);
              ids[slot] = controllers[slot % nodeCount]
                  .register(coreName, desc);
            } catch (Throwable e) {
              e.printStackTrace();
              fail("register threw exception:" + e.getClass());
            }
          }
        };
        
        nodeExecutors[i % nodeCount].submit(coreStarter);
      }
      
      for (int i = 0; i < nodeCount; i++) {
        nodeExecutors[i].shutdown();
      }

      for (int i = 0; i < nodeCount; i++) {
        while (!nodeExecutors[i].awaitTermination(100, TimeUnit.MILLISECONDS));
      }
      
      // make sure all cores have been assigned a id in cloudstate
      for (int i = 0; i < 40; i++) {
        reader.updateCloudState(true);
        CloudState state = reader.getCloudState();
        Map<String,Slice> slices = state.getSlices("collection1");
        int count = 0;
        for (String name : slices.keySet()) {
          count += slices.get(name).getShards().size();
        }
        if (coreCount == count) break;
        Thread.sleep(200);
      }

      // make sure all cores have been returned a id
      for (int i = 0; i < 90; i++) {
        int assignedCount = 0;
        for (int j = 0; j < coreCount; j++) {
          if (ids[j] != null) {
            assignedCount++;
          }
        }
        if (coreCount == assignedCount) {
          break;
        }
        Thread.sleep(500);
      }
      
      final HashMap<String, AtomicInteger> counters = new HashMap<String,AtomicInteger>();
      for (int i = 1; i < sliceCount+1; i++) {
        counters.put("shard" + i, new AtomicInteger());
      }
      
      for (int i = 0; i < coreCount; i++) {
        final AtomicInteger ai = counters.get(ids[i]);
        assertNotNull("could not find counter for shard:" + ids[i], ai);
        ai.incrementAndGet();
      }

      for (String counter: counters.keySet()) {
        int count = counters.get(counter).intValue();
        int expectedCount = coreCount / sliceCount;
        int min = expectedCount - 1;
        int max = expectedCount + 1;
        if (count < min || count > max) {
          fail("Unevenly assigned shard ids, " + counter + " had " + count
              + ", expected: " + min + "-" + max);
        }
      }
      
      //make sure leaders are in cloud state
      for (int i = 0; i < sliceCount; i++) {
        assertNotNull(reader.getLeaderUrl("collection1", "shard" + (i + 1)), 15000);
      }

    } finally {
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
      System.clearProperty("bootstrap_confdir");
      if (DEBUG) {
        if (controllers[0] != null) {
          zkClient.printLayoutToStdOut();
        }
      }
      if (zkClient != null) {
        zkClient.close();
      }
      if (reader != null) {
        reader.close();
      }
      for (int i = 0; i < controllers.length; i++)
        if (controllers[i] != null) {
          controllers[i].close();
        }
      server.shutdown();
      for (int i = 0; i < nodeCount; i++) {
        nodeExecutors[i].shutdownNow();
      }
    }
  }

  //wait until collections are available
  private void waitForCollections(ZkStateReader stateReader, String... collections) throws InterruptedException, KeeperException {
    int maxIterations = 100;
    while (0 < maxIterations--) {
      stateReader.updateCloudState(true);
      final CloudState state = stateReader.getCloudState();
      Set<String> availableCollections = state.getCollections();
      int availableCount = 0;
      for(String requiredCollection: collections) {
        if(availableCollections.contains(requiredCollection)) {
          availableCount++;
        }
        if(availableCount == collections.length) return;
        Thread.sleep(50);
      }
    }
    log.warn("Timeout waiting for collections: " + Arrays.asList(collections) + " state:" + stateReader.getCloudState());
  }
  
  @Test
  public void testStateChange() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    SolrZkClient overseerClient = null;
    
    try {
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      zkClient.makePath("/live_nodes", true);

      //live node
      String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + "node1";
      zkClient.makePath(nodePath,CreateMode.EPHEMERAL, true);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      Overseer.createClientNodes(zkClient, "node1");
      
      overseerClient = electNewOverseer(server.getZkAddress());

      HashMap<String, String> coreProps = new HashMap<String,String>();
      coreProps.put(ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr");
      coreProps.put(ZkStateReader.NODE_NAME_PROP, "node1");
      coreProps.put(ZkStateReader.CORE_NAME_PROP, "core1");
      coreProps.put(ZkStateReader.ROLES_PROP, "");
      coreProps.put(ZkStateReader.STATE_PROP, ZkStateReader.RECOVERING);
      CoreState state = new CoreState("core1", "collection1", coreProps, 2);
      
      nodePath = "/node_states/node1";

      try {
        zkClient.makePath(nodePath, CreateMode.EPHEMERAL, true);
      } catch (KeeperException ke) {
        if(ke.code()!=Code.NODEEXISTS) {
          throw ke;
        }
      }
      
      zkClient.setData(nodePath, ZkStateReader.toJSON(new CoreState[]{state}), true);
      waitForCollections(reader, "collection1");

      assertEquals(reader.getCloudState().toString(), ZkStateReader.RECOVERING,
          reader.getCloudState().getSlice("collection1", "shard1").getShards()
              .get("node1_core1").get(ZkStateReader.STATE_PROP));

      //publish node state (active)
      coreProps.put(ZkStateReader.STATE_PROP, ZkStateReader.ACTIVE);
      
      coreProps.put(ZkStateReader.SHARD_ID_PROP, "shard1");
      state = new CoreState("core1", "collection1", coreProps, 2);

      zkClient.setData(nodePath, ZkStateReader.toJSON(new CoreState[]{state}), true);

      verifyStatus(reader, ZkStateReader.ACTIVE);

    } finally {

      if (zkClient != null) {
        zkClient.close();
      }
      if (overseerClient != null) {
        overseerClient.close();
      }

      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
  }

  private void verifyStatus(ZkStateReader reader, String expectedState) throws InterruptedException {
    int maxIterations = 100;
    String coreState = null;
    while(maxIterations-->0) {
      Slice slice = reader.getCloudState().getSlice("collection1", "shard1");
      if(slice!=null) {
        coreState = slice.getShards().get("node1_core1").get(ZkStateReader.STATE_PROP);
        if(coreState.equals(expectedState)) {
          return;
        }
      }
      Thread.sleep(50);
    }
    fail("Illegal state, was:" + coreState + " expected:" + expectedState + "cloudState:" + reader.getCloudState());
  }
  
  private void verifyShardLeader(ZkStateReader reader, String collection, String shard, String expectedCore) throws InterruptedException, KeeperException {
    int maxIterations = 100;
    while(maxIterations-->0) {
      ZkNodeProps props =  reader.getCloudState().getLeader(collection, shard);
      if(props!=null) {
        if(expectedCore.equals(props.get(ZkStateReader.CORE_NAME_PROP))) {
          return;
        }
      }
      Thread.sleep(100);
    }
    
    assertEquals("Unexpected shard leader coll:" + collection + " shard:" + shard, expectedCore, (reader.getCloudState().getLeader(collection, shard)!=null)?reader.getCloudState().getLeader(collection, shard).get(ZkStateReader.CORE_NAME_PROP):null);
  }

  @Test
  public void testOverseerFailure() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient controllerClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      controllerClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);
      
      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1", "collection1");
      
      overseerClient = electNewOverseer(server.getZkAddress());

      Thread.sleep(1000);
      mockController.publishState("core1", ZkStateReader.RECOVERING, 1);

      waitForCollections(reader, "collection1");
      verifyStatus(reader, ZkStateReader.RECOVERING);

      int version = getCloudStateVersion(controllerClient);
      
      mockController.publishState("core1", ZkStateReader.ACTIVE, 1);
      
      while(version == getCloudStateVersion(controllerClient));

      verifyStatus(reader, ZkStateReader.ACTIVE);
      version = getCloudStateVersion(controllerClient);
      overseerClient.close();
      Thread.sleep(1000); //wait for overseer to get killed

      mockController.publishState("core1", ZkStateReader.RECOVERING, 1);
      version = getCloudStateVersion(controllerClient);
      
      overseerClient = electNewOverseer(server.getZkAddress());

      while(version == getCloudStateVersion(controllerClient));

      verifyStatus(reader, ZkStateReader.RECOVERING);
      
      assertEquals("Live nodes count does not match", 1, reader.getCloudState()
          .getLiveNodes().size());
      assertEquals("Shard count does not match", 1, reader.getCloudState()
          .getSlice("collection1", "shard1").getShards().size());      
      version = getCloudStateVersion(controllerClient);
      mockController.publishState("core1", null,1);
      while(version == getCloudStateVersion(controllerClient));
      Thread.sleep(100);
      assertEquals("Shard count does not match", 0, reader.getCloudState()
          .getSlice("collection1", "shard1").getShards().size());
    } finally {
      
      if (mockController != null) {
        mockController.close();
      }
      
      if (overseerClient != null) {
       overseerClient.close();
      }
      if (controllerClient != null) {
        controllerClient.close();
      }
      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
  }

  private class OverseerRestarter implements Runnable{
    SolrZkClient overseerClient = null;
    public volatile boolean run = true;
    private final String zkAddress;

    public OverseerRestarter(String zkAddress) {
      this.zkAddress = zkAddress;
    }
    
    @Override
    public void run() {
      try {
        overseerClient = electNewOverseer(zkAddress);
      } catch (Throwable t) {
        //t.printStackTrace();
      }
      Random rnd = random();
      while (run) {
        if(rnd.nextInt(20)==1){
          try {
            overseerClient.close();
            overseerClient = electNewOverseer(zkAddress);
          } catch (Throwable e) {
            //e.printStackTrace();
          }
        }
        try {
          Thread.sleep(100);
        } catch (Throwable e) {
          //e.printStackTrace();
        }
      }
      try {
        overseerClient.close();
      } catch (Throwable e) {
        //e.printStackTrace();
      }
    }
  }

  
  @Test
  public void testShardLeaderChange() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    final ZkTestServer server = new ZkTestServer(zkDir);
    SolrZkClient controllerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    MockZKController mockController2 = null;
    OverseerRestarter killer = null;
    Thread killerThread = null;
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      controllerClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);

      killer = new OverseerRestarter(server.getZkAddress());
      killerThread = new Thread(killer);
      killerThread.start();

      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      for (int i = 0; i < atLeast(4); i++) {
        mockController = new MockZKController(server.getZkAddress(), "node1", "collection1");
        mockController.publishState("core1", "state1",1);
        if(mockController2!=null) {
          mockController2.close();
          mockController2 = null;
        }
        mockController.publishState("core1", "state2",1);
        mockController2 = new MockZKController(server.getZkAddress(), "node2", "collection1");
        mockController.publishState("core1", "state1",1);
        verifyShardLeader(reader, "collection1", "shard1", "core1");
        mockController2.publishState("core4", "state2" ,1);
        mockController.close();
        mockController = null;
        verifyShardLeader(reader, "collection1", "shard1", "core4");
      }
    } finally {
      if (killer != null) {
        killer.run = false;
        if (killerThread != null) {
          killerThread.join();
        }
      }
      if (mockController != null) {
        mockController.close();
      }
      if (mockController2 != null) {
        mockController2.close();
      }
      if (controllerClient != null) {
        controllerClient.close();
      }
      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
  }

  @Test
  public void testDoubleAssignment() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient controllerClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      controllerClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);
      
      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1", "collection1");
      
      overseerClient = electNewOverseer(server.getZkAddress());

      mockController.publishState("core1", ZkStateReader.RECOVERING, 1);

      waitForCollections(reader, "collection1");
      
      verifyStatus(reader, ZkStateReader.RECOVERING);

      mockController.close();

      int version = getCloudStateVersion(controllerClient);
      
      mockController = new MockZKController(server.getZkAddress(), "node1", "collection1");
      mockController.publishState("core1", ZkStateReader.RECOVERING, 1);

      while (version == getCloudStateVersion(controllerClient));
      
      reader.updateCloudState(true);
      CloudState state = reader.getCloudState();
      
      int numFound = 0;
      for (Map<String,Slice> collection : state.getCollectionStates().values()) {
        for (Slice slice : collection.values()) {
          if (slice.getShards().get("node1_core1") != null) {
            numFound++;
          }
        }
      }
      assertEquals("Shard was found in more than 1 times in CloudState", 1,
          numFound);
    } finally {
      if (overseerClient != null) {
       overseerClient.close();
      }
      if (mockController != null) {
        mockController.close();
      }

      if (controllerClient != null) {
        controllerClient.close();
      }
      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
  }

  @Test
  public void testPlaceholders() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient controllerClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      controllerClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);
      
      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1", "collection1");
      
      overseerClient = electNewOverseer(server.getZkAddress());

      mockController.publishState("core1", ZkStateReader.RECOVERING, 12);

      waitForCollections(reader, "collection1");
      
      assertEquals("Slicecount does not match", 12, reader.getCloudState().getSlices("collection1").size());
      
    } finally {
      if (overseerClient != null) {
       overseerClient.close();
      }
      if (mockController != null) {
        mockController.close();
      }

      if (controllerClient != null) {
        controllerClient.close();
      }
      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
  }

  private int getCloudStateVersion(SolrZkClient controllerClient)
      throws KeeperException, InterruptedException {
    return controllerClient.exists(ZkStateReader.CLUSTER_STATE, null, false).getVersion();
  }


  private SolrZkClient electNewOverseer(String address) throws InterruptedException,
      TimeoutException, IOException, KeeperException {
    SolrZkClient zkClient  = new SolrZkClient(address, TIMEOUT);
    ZkStateReader reader = new ZkStateReader(zkClient);
    LeaderElector overseerElector = new LeaderElector(zkClient);
    ElectionContext ec = new OverseerElectionContext(address.replaceAll("/", "_"), zkClient, reader);
    overseerElector.setup(ec);
    overseerElector.joinElection(ec);
    return zkClient;
  }
}