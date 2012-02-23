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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.update.PeerSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncStrategy {
  protected final Logger log = LoggerFactory.getLogger(getClass());

  private HttpShardHandlerFactory shardHandlerFactory;

  private ShardHandler shardHandler;
  
  private static MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
  private static HttpClient client = new HttpClient(mgr);
  static {
    mgr.getParams().setDefaultMaxConnectionsPerHost(20);
    mgr.getParams().setMaxTotalConnections(10000);
    mgr.getParams().setConnectionTimeout(30000);
    mgr.getParams().setSoTimeout(30000);

    // prevent retries  (note: this didn't work when set on mgr.. needed to be set on client)
    DefaultHttpMethodRetryHandler retryhandler = new DefaultHttpMethodRetryHandler(0, false);
    client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, retryhandler);
  }
  
  public SyncStrategy() {
    shardHandlerFactory = new HttpShardHandlerFactory();
    shardHandler = shardHandlerFactory.getShardHandler(client);
  }
  
  private static class SyncShardRequest extends ShardRequest {
    String coreName;
  }
  
  public boolean sync(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps) {
    // TODO: look at our state usage of sync
    // zkController.publish(core, ZkStateReader.SYNC);
    
    // solrcloud_debug
    // System.out.println("SYNC UP");
    if (core.getUpdateHandler().getUpdateLog() == null) {
      log.error("No UpdateLog found - cannot sync");
      return false;
    }
    boolean success = syncReplicas(zkController, core, leaderProps);
    return success;
  }
  
  private boolean syncReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps) {
    boolean success = false;
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();

    // first sync ourselves - we are the potential leader after all
    try {
      success = syncWithReplicas(zkController, core, leaderProps, collection,
          shardId);
    } catch (Exception e) {
      SolrException.log(log, "Sync Failed", e);
    }
    try {
      // if !success but no one else is in active mode,
      // we are the leader anyway
      // TODO: should we also be leader if there is only one other active?
      // if we couldn't sync with it, it shouldn't be able to sync with us
      if (!success
          && !areAnyOtherReplicasActive(zkController, leaderProps, collection,
              shardId)) {
//        System.out
//            .println("wasnt a success but no on else i active! I am the leader");
        
        success = true;
      }
      
      if (success) {
        // solrcloud_debug
        // System.out.println("Sync success");
        // we are the leader - tell all of our replias to sync with us
        
        syncToMe(zkController, collection, shardId, leaderProps);
        
      } else {
        
        // solrcloud_debug
        // System.out.println("Sync failure");
      }
      
    } catch (Exception e) {
      SolrException.log(log, "Sync Failed", e);
    }
    
    return success;
  }
  
  private boolean areAnyOtherReplicasActive(ZkController zkController,
      ZkNodeProps leaderProps, String collection, String shardId) {
    CloudState cloudState = zkController.getZkStateReader().getCloudState();
    Map<String,Slice> slices = cloudState.getSlices(collection);
    Slice slice = slices.get(shardId);
    Map<String,ZkNodeProps> shards = slice.getShards();
    for (Map.Entry<String,ZkNodeProps> shard : shards.entrySet()) {
      String state = shard.getValue().get(ZkStateReader.STATE_PROP);
//      System.out.println("state:"
//          + state
//          + shard.getValue().get(ZkStateReader.NODE_NAME_PROP)
//          + " live: "
//          + cloudState.liveNodesContain(shard.getValue().get(
//              ZkStateReader.NODE_NAME_PROP)));
      if ((state.equals(ZkStateReader.ACTIVE))
          && cloudState.liveNodesContain(shard.getValue().get(
              ZkStateReader.NODE_NAME_PROP))
          && !new ZkCoreNodeProps(shard.getValue()).getCoreUrl().equals(
              new ZkCoreNodeProps(leaderProps).getCoreUrl())) {
        return true;
      }
    }
    
    return false;
  }
  
  private boolean syncWithReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps props, String collection, String shardId)
      throws MalformedURLException, SolrServerException, IOException {
    List<ZkCoreNodeProps> nodes = zkController.getZkStateReader()
        .getReplicaProps(collection, shardId,
            props.get(ZkStateReader.NODE_NAME_PROP),
            props.get(ZkStateReader.CORE_NAME_PROP), ZkStateReader.ACTIVE); // TODO:
    // should
    // there
    // be a
    // state
    // filter?
    
    if (nodes == null) {
      // I have no replicas
      return true;
    }
    
    List<String> syncWith = new ArrayList<String>();
    for (ZkCoreNodeProps node : nodes) {
      // if we see a leader, must be stale state, and this is the guy that went down
      if (!node.getNodeProps().keySet().contains(ZkStateReader.LEADER_PROP)) {
        syncWith.add(node.getCoreUrl());
      }
    }
    
 
    PeerSync peerSync = new PeerSync(core, syncWith, core.getUpdateHandler().getUpdateLog().numRecordsToKeep);
    return peerSync.sync();
  }
  
  private void syncToMe(ZkController zkController, String collection,
      String shardId, ZkNodeProps leaderProps) throws MalformedURLException,
      SolrServerException, IOException {
    
    // sync everyone else
    // TODO: we should do this in parallel at least
    List<ZkCoreNodeProps> nodes = zkController
        .getZkStateReader()
        .getReplicaProps(collection, shardId,
            leaderProps.get(ZkStateReader.NODE_NAME_PROP),
            leaderProps.get(ZkStateReader.CORE_NAME_PROP), ZkStateReader.ACTIVE);
    if (nodes == null) {
      // System.out.println("I have no replicas");
      // I have no replicas
      return;
    }
    //System.out.println("tell my replicas to sync");
    ZkCoreNodeProps zkLeader = new ZkCoreNodeProps(leaderProps);
    for (ZkCoreNodeProps node : nodes) {
      try {
//         System.out
//             .println("try and ask " + node.getCoreUrl() + " to sync");
        log.info("try and ask " + node.getCoreUrl() + " to sync");
        requestSync(zkLeader.getCoreUrl(), node.getCoreName());

      } catch (Exception e) {
        SolrException.log(log, "Error syncing replica to leader", e);
      }
    }
    
    
    for(;;) {
      ShardResponse srsp = shardHandler.takeCompletedOrError();
      if (srsp == null) break;
      boolean success = handleResponse(srsp);
      //System.out.println("got response:" + success);
      if (!success) {
         try {
           log.info("Sync failed - asking replica to recover.");
           //System.out.println("Sync failed - asking replica to recover.");
           RequestRecovery recoverRequestCmd = new RequestRecovery();
           recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
           recoverRequestCmd.setCoreName(((SyncShardRequest)srsp.getShardRequest()).coreName);
           
           CommonsHttpSolrServer server = new CommonsHttpSolrServer(zkLeader.getBaseUrl());
           server.request(recoverRequestCmd);
         } catch (Exception e) {
           log.info("Could not tell a replica to recover", e);
         }
         shardHandler.cancelAll();
        break;
      }
    }
  }
  
  private boolean handleResponse(ShardResponse srsp) {
    NamedList<Object> response = srsp.getSolrResponse().getResponse();
    // TODO: why does this return null sometimes?
    if (response == null) {
      return false;
    }
    boolean success = (Boolean) response.get("sync");
    
    return success;
  }

  private void requestSync(String replica, String coreName) {
    SyncShardRequest sreq = new SyncShardRequest();
    sreq.coreName = coreName;
    sreq.purpose = 1;
    // TODO: this sucks
    if (replica.startsWith("http://"))
      replica = replica.substring(7);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set("distrib",false);
    sreq.params.set("getVersions",Integer.toString(100));
    sreq.params.set("sync",replica);
    
    shardHandler.submit(sreq, replica, sreq.params);
  }
  
  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }
}
