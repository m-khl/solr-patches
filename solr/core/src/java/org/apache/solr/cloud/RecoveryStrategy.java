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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SafeStopThread;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.RequestHandlers.LazyRequestHandlerWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryStrategy extends Thread implements SafeStopThread {
  private static final int MAX_RETRIES = 500;
  private static final int INTERRUPTED = MAX_RETRIES + 1;
  private static final int START_TIMEOUT = 100;
  
  private static final String REPLICATION_HANDLER = "/replication";

  private static Logger log = LoggerFactory.getLogger(RecoveryStrategy.class);

  private volatile boolean close = false;

  private ZkController zkController;
  private String baseUrl;
  private String coreZkNodeName;
  private ZkStateReader zkStateReader;
  private volatile String coreName;
  private int retries;
  private boolean recoveringAfterStartup;
  private CoreContainer cc;
  
  public RecoveryStrategy(CoreContainer cc, String name) {
    this.cc = cc;
    this.coreName = name;
    setName("RecoveryThread");
    zkController = cc.getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();
    coreZkNodeName = zkController.getNodeName() + "_" + coreName;
  }

  public void setRecoveringAfterStartup(boolean recoveringAfterStartup) {
    this.recoveringAfterStartup = recoveringAfterStartup;
  }

  // make sure any threads stop retrying
  public void close() {
    close = true;
  }

  
  private void recoveryFailed(final SolrCore core,
      final ZkController zkController, final String baseUrl,
      final String shardZkNodeName, final CoreDescriptor cd) {
    SolrException.log(log, "Recovery failed - I give up.");
    zkController.publishAsRecoveryFailed(baseUrl, cd,
        shardZkNodeName, core.getName());
    close = true;
  }
  
  private void replicate(String nodeName, SolrCore core, ZkNodeProps leaderprops, String baseUrl)
      throws SolrServerException, IOException {
   
    String leaderBaseUrl = leaderprops.get(ZkStateReader.BASE_URL_PROP);
    ZkCoreNodeProps leaderCNodeProps = new ZkCoreNodeProps(leaderprops);
    String leaderUrl = leaderCNodeProps.getCoreUrl();
    
    log.info("Attempting to replicate from " + leaderUrl);
    
    // if we are the leader, either we are trying to recover faster
    // then our ephemeral timed out or we are the only node
    if (!leaderBaseUrl.equals(baseUrl)) {
      // send commit
      commitOnLeader(leaderUrl);
      
      // use rep handler directly, so we can do this sync rather than async
      SolrRequestHandler handler = core.getRequestHandler(REPLICATION_HANDLER);
      if (handler instanceof LazyRequestHandlerWrapper) {
        handler = ((LazyRequestHandlerWrapper)handler).getWrappedHandler();
      }
      ReplicationHandler replicationHandler = (ReplicationHandler) handler;
      
      if (replicationHandler == null) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
            "Skipping recovery, no " + REPLICATION_HANDLER + " handler found");
      }
      
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl + "replication");
      
      if (close) retries = INTERRUPTED; 
      boolean success = replicationHandler.doFetch(solrParams, true); // TODO: look into making sure force=true does not download files we already have

      if (!success) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replication for recovery failed.");
      }
      
      // solrcloud_debug
//      try {
//        RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
//        SolrIndexSearcher searcher = searchHolder.get();
//        try {
//          System.out.println(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName() + " replicated "
//              + searcher.search(new MatchAllDocsQuery(), 1).totalHits + " from " + leaderUrl + " gen:" + core.getDeletionPolicy().getLatestCommit().getGeneration() + " data:" + core.getDataDir());
//        } finally {
//          searchHolder.decref();
//        }
//      } catch (Exception e) {
//        
//      }
    }
  }

  private void commitOnLeader(String leaderUrl) throws MalformedURLException,
      SolrServerException, IOException {
    CommonsHttpSolrServer server = new CommonsHttpSolrServer(leaderUrl);
    server.setConnectionTimeout(30000);
    server.setSoTimeout(30000);
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams());
    ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true).process(
        server);
    server.commit();
    server.shutdown();
  }

  private void sendPrepRecoveryCmd(String leaderBaseUrl,
      String leaderCoreName) throws MalformedURLException, SolrServerException,
      IOException {
    CommonsHttpSolrServer server = new CommonsHttpSolrServer(leaderBaseUrl);
    server.setConnectionTimeout(45000);
    server.setSoTimeout(45000);
    WaitForState prepCmd = new WaitForState();
    prepCmd.setCoreName(leaderCoreName);
    prepCmd.setNodeName(zkController.getNodeName());
    prepCmd.setCoreNodeName(coreZkNodeName);
    prepCmd.setState(ZkStateReader.RECOVERING);
    prepCmd.setCheckLive(true);
    prepCmd.setPauseFor(6000);
    
    server.request(prepCmd);
    server.shutdown();
  }

  @Override
  public void run() {
    SolrCore core = cc.getCore(coreName);
    if (core == null) {
      SolrException.log(log, "SolrCore not found - cannot recover:" + coreName);
      return;
    }

    // set request info for logging
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
    
    try {
      doRecovery(core);
    } finally {
      SolrRequestInfo.clearRequestInfo();
    }
  }
  
  public void doRecovery(SolrCore core) {
    boolean replayed = false;
    boolean successfulRecovery = false;

    UpdateLog ulog;
    try {
      ulog = core.getUpdateHandler().getUpdateLog();
      if (ulog == null) {
        SolrException.log(log, "No UpdateLog found - cannot recover");
        recoveryFailed(core, zkController, baseUrl, coreZkNodeName,
            core.getCoreDescriptor());
        return;
      }
    } finally {
      core.close();
    }

    List<Long> startingRecentVersions;
    UpdateLog.RecentUpdates startingRecentUpdates = ulog.getRecentUpdates();
    try {
      startingRecentVersions = startingRecentUpdates.getVersions(ulog.numRecordsToKeep);
    } finally {
      startingRecentUpdates.close();
    }

    List<Long> reallyStartingVersions = ulog.getStartingVersions();


    if (reallyStartingVersions != null && recoveringAfterStartup) {
      int oldIdx = 0;  // index of the start of the old list in the current list
      long firstStartingVersion = reallyStartingVersions.size() > 0 ? reallyStartingVersions.get(0) : 0;

      for (; oldIdx<startingRecentVersions.size(); oldIdx++) {
        if (startingRecentVersions.get(oldIdx) == firstStartingVersion) break;
      }

      if (oldIdx > 0) {
        log.info("####### Found new versions added after startup: num=" + oldIdx);
      }

      // TODO: only log at debug level in the future (or move to oldIdx > 0 block)
      log.info("###### startupVersions=" + reallyStartingVersions);
      log.info("###### currentVersions=" + startingRecentVersions);
    }
    
    if (recoveringAfterStartup) {
      // if we're recovering after startup (i.e. we have been down), then we need to know what the last versions were
      // when we went down.
      startingRecentVersions = reallyStartingVersions;
    }

    boolean firstTime = true;

    while (!successfulRecovery && !close && !isInterrupted()) { // don't use interruption or it will close channels though
      core = cc.getCore(coreName);
      if (core == null) {
        SolrException.log(log, "SolrCore not found - cannot recover:" + coreName);
        return;
      }
      try {
        // first thing we just try to sync
        zkController.publish(core.getCoreDescriptor(), ZkStateReader.RECOVERING);
 
        CloudDescriptor cloudDesc = core.getCoreDescriptor()
            .getCloudDescriptor();
        ZkNodeProps leaderprops = zkStateReader.getLeaderProps(
            cloudDesc.getCollectionName(), cloudDesc.getShardId());
        
        String leaderBaseUrl = leaderprops.get(ZkStateReader.BASE_URL_PROP);
        String leaderCoreName = leaderprops.get(ZkStateReader.CORE_NAME_PROP);
        
        String leaderUrl = ZkCoreNodeProps.getCoreUrl(leaderBaseUrl, leaderCoreName); 
        
        sendPrepRecoveryCmd(leaderBaseUrl, leaderCoreName);
        
        
        // first thing we just try to sync
        if (firstTime) {
          firstTime = false; // only try sync the first time through the loop
          log.info("Attempting to PeerSync from " + leaderUrl + " recoveringAfterStartup="+recoveringAfterStartup);
          // System.out.println("Attempting to PeerSync from " + leaderUrl
          // + " i am:" + zkController.getNodeName());
          PeerSync peerSync = new PeerSync(core,
              Collections.singletonList(leaderUrl), ulog.numRecordsToKeep);
          peerSync.setStartingVersions(startingRecentVersions);
          boolean syncSuccess = peerSync.sync();
          if (syncSuccess) {
            SolrQueryRequest req = new LocalSolrQueryRequest(core,
                new ModifiableSolrParams());
            core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
            log.info("Sync Recovery was successful - registering as Active");
            // System.out
            // .println("Sync Recovery was successful - registering as Active "
            // + zkController.getNodeName());
            
            // solrcloud_debug
            // try {
            // RefCounted<SolrIndexSearcher> searchHolder =
            // core.getNewestSearcher(false);
            // SolrIndexSearcher searcher = searchHolder.get();
            // try {
            // System.out.println(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName()
            // + " synched "
            // + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
            // } finally {
            // searchHolder.decref();
            // }
            // } catch (Exception e) {
            //
            // }
            
            // sync success - register as active and return
            zkController.publishAsActive(baseUrl, core.getCoreDescriptor(),
                coreZkNodeName, coreName);
            successfulRecovery = true;
            close = true;
            return;
          }
          
          log.info("Sync Recovery was not successful - trying replication");
        }
        //System.out.println("Sync Recovery was not successful - trying replication");
        
        log.info("Begin buffering updates");
        ulog.bufferUpdates();
        replayed = false;
        
        try {
          
          replicate(zkController.getNodeName(), core,
              leaderprops, leaderUrl);
          
          replay(ulog);
          replayed = true;
          
          log.info("Recovery was successful - registering as Active");
          // if there are pending recovery requests, don't advert as active
          zkController.publishAsActive(baseUrl, core.getCoreDescriptor(),
              coreZkNodeName, coreName);
          close = true;
          successfulRecovery = true;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Recovery was interrupted", e);
          retries = INTERRUPTED;
        } catch (Throwable t) {
          SolrException.log(log, "Error while trying to recover", t);
        } finally {
          if (!replayed) {
            try {
              ulog.dropBufferedUpdates();
            } catch (Throwable t) {
              SolrException.log(log, "", t);
            }
          }

        }
        
      } catch (Throwable t) {
        SolrException.log(log, "Error while trying to recover", t);
      } finally {
        if (core != null) {
          core.close();
        }
      }
      
      if (!successfulRecovery) {
        // lets pause for a moment and we need to try again...
        // TODO: we don't want to retry for some problems?
        // Or do a fall off retry...
        try {
          
          SolrException.log(log, "Recovery failed - trying again...");
          retries++;
          if (retries >= MAX_RETRIES) {
            if (retries == INTERRUPTED) {
              
            } else {
              // TODO: for now, give up after X tries - should we do more?
              core = cc.getCore(coreName);
              try {
                recoveryFailed(core, zkController, baseUrl, coreZkNodeName,
                    core.getCoreDescriptor());
              } finally {
                if (core != null) {
                  core.close();
                }
              }
            }
            break;
          }
          
        } catch (Exception e) {
          SolrException.log(log, "", e);
        }
        
        try {
          Thread.sleep(Math.min(START_TIMEOUT * retries, 60000));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Recovery was interrupted", e);
          retries = INTERRUPTED;
        }
      }
    
      
      log.info("Finished recovery process");
      
    }
  }

  private Future<RecoveryInfo> replay(UpdateLog ulog)
      throws InterruptedException, ExecutionException, TimeoutException {
    Future<RecoveryInfo> future = ulog.applyBufferedUpdates();
    if (future == null) {
      // no replay needed\
      log.info("No replay needed");
    } else {
      log.info("Replaying buffered documents");
      // wait for replay
      future.get();
    }
    
    // solrcloud_debug
//    try {
//      RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
//      SolrIndexSearcher searcher = searchHolder.get();
//      try {
//        System.out.println(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName() + " replayed "
//            + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
//      } finally {
//        searchHolder.decref();
//      }
//    } catch (Exception e) {
//      
//    }
    
    return future;
  }

  public boolean isClosed() {
    return close;
  }

}
