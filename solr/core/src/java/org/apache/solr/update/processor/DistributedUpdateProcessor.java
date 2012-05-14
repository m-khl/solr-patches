package org.apache.solr.update.processor;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionBucket;
import org.apache.solr.update.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// NOT mt-safe... create a new processor for each add thread
// TODO: we really should not wait for distrib after local? unless a certain replication factor is asked for
public class DistributedUpdateProcessor extends UpdateRequestProcessor {
  public final static Logger log = LoggerFactory.getLogger(DistributedUpdateProcessor.class);

  public static final String SEEN_LEADER = "leader";
  public static final String COMMIT_END_POINT = "commit_end_point";
  public static final String DELETE_BY_QUERY_LEVEL = "dbq_level";

  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final UpdateRequestProcessor next;

  public static final String VERSION_FIELD = "_version_";

  private final UpdateHandler updateHandler;
  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private boolean returnVersions = true; // todo: default to false and make configurable

  private NamedList addsResponse = null;
  private NamedList deleteResponse = null;
  private NamedList deleteByQueryResponse = null;
  private CharsRef scratch;
  
  private final SchemaField idField;
  
  private final SolrCmdDistributor cmdDistrib;

  private boolean zkEnabled = false;

  private CloudDescriptor cloudDesc;
  private String collection;
  private ZkController zkController;
  
  // these are setup at the start of each request processing
  // method in this update processor
  private boolean isLeader = true;
  private boolean forwardToLeader = false;
  private List<Node> nodes;

  
  public DistributedUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    this.rsp = rsp;
    this.next = next;
    this.idField = req.getSchema().getUniqueKeyField();
    // version init

    this.updateHandler = req.getCore().getUpdateHandler();
    this.ulog = updateHandler.getUpdateLog();
    this.vinfo = ulog == null ? null : ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;
    returnVersions = req.getParams().getBool(UpdateParams.VERSIONS ,false);

    // TODO: better way to get the response, or pass back info to it?
    SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    this.req = req;
    
    CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
    
    this.zkEnabled  = coreDesc.getCoreContainer().isZooKeeperAware();
    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;

    
    zkController = req.getCore().getCoreDescriptor().getCoreContainer().getZkController();
    
    cloudDesc = coreDesc.getCloudDescriptor();
    
    if (cloudDesc != null) {
      collection = cloudDesc.getCollectionName();
    }
    
    cmdDistrib = new SolrCmdDistributor();
  }

  private List<Node> setupRequest(int hash) {
    List<Node> nodes = null;

    // if we are in zk mode...
    if (zkEnabled) {
      // the leader is...
      // TODO: if there is no leader, wait and look again
      // TODO: we are reading the leader from zk every time - we should cache
      // this and watch for changes?? Just pull it from ZkController cluster state probably?
      String shardId = getShard(hash, collection, zkController.getCloudState()); // get the right shard based on the hash...

      try {
        // TODO: if we find out we cannot talk to zk anymore, we should probably realize we are not
        // a leader anymore - we shouldn't accept updates at all??
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(zkController.getZkStateReader().getLeaderProps(
            collection, shardId));
        
        String leaderNodeName = leaderProps.getCoreNodeName();
        String coreName = req.getCore().getName();
        String coreNodeName = zkController.getNodeName() + "_" + coreName;
        isLeader = coreNodeName.equals(leaderNodeName);
        
        if (req.getParams().getBool(SEEN_LEADER, false)) {
          // we are coming from the leader, just go local - add no urls
          forwardToLeader = false;
        } else if (isLeader) {
          // that means I want to forward onto my replicas...
          // so get the replicas...
          forwardToLeader = false;
          List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
              .getReplicaProps(collection, shardId, zkController.getNodeName(),
                  coreName, null, ZkStateReader.DOWN);
          if (replicaProps != null) {
            nodes = new ArrayList<Node>(replicaProps.size());
            for (ZkCoreNodeProps props : replicaProps) {
              nodes.add(new StdNode(props));
            }
          }
          
        } else {
          // I need to forward onto the leader...
          nodes = new ArrayList<Node>(1);
          nodes.add(new RetryNode(leaderProps, zkController.getZkStateReader(), collection, shardId));
          forwardToLeader = true;
        }
        
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }

    return nodes;
  }


  private String getShard(int hash, String collection, CloudState cloudState) {
    // ranges should be part of the cloud state and eventually gotten from zk

    // get the shard names
    return cloudState.getShard(hash, collection);
  }

  // used for deleteByQuery to get the list of nodes this leader should forward to
  private List<Node> setupRequest() {
    List<Node> nodes = null;
    String shardId = cloudDesc.getShardId();

    try {

      ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(zkController.getZkStateReader().getLeaderProps(
          collection, shardId));

      String leaderNodeName = leaderProps.getCoreNodeName();
      String coreName = req.getCore().getName();
      String coreNodeName = zkController.getNodeName() + "_" + coreName;
      isLeader = coreNodeName.equals(leaderNodeName);

      // TODO: what if we are no longer the leader?

      forwardToLeader = false;
      List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
          .getReplicaProps(collection, shardId, zkController.getNodeName(),
              coreName);
      if (replicaProps != null) {
        nodes = new ArrayList<Node>(replicaProps.size());
        for (ZkCoreNodeProps props : replicaProps) {
          nodes.add(new StdNode(props));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
          e);
    }

    return nodes;
  }


  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    // TODO: check for id field?
    int hash = 0;
    if (zkEnabled) {
      zkCheck();
      hash = hash(cmd);
      nodes = setupRequest(hash);
    } else {
      // even in non zk mode, tests simulate updates from a leader
      isLeader = !req.getParams().getBool(SEEN_LEADER, false);
    }
    
    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd = versionAdd(cmd);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }
    
    ModifiableSolrParams params = null;
    if (nodes != null) {
      params = new ModifiableSolrParams(req.getParams());
      if (isLeader) {
        params.set(SEEN_LEADER, true);
      }
      params.remove("commit"); // this will be distributed from the local commit
      cmdDistrib.distribAdd(cmd, nodes, params);
    }
    
    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && idField != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<String>();
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }
    
    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.
  }
 
  // TODO: optionally fail if n replicas are not reached...
  private void doFinish() {
    // TODO: if not a forward and replication req is not specified, we could
    // send in a background thread

    cmdDistrib.finish();
    Response response = cmdDistrib.getResponse();
    // TODO - we may need to tell about more than one error...
    
    // if its a forward, any fail is a problem - 
    // otherwise we assume things are fine if we got it locally
    // until we start allowing min replication param
    if (response.errors.size() > 0) {
      // if one node is a RetryNode, this was a forward request
      if (response.errors.get(0).node instanceof RetryNode) {
        rsp.setException(response.errors.get(0).e);
      }
      // else
      // for now we don't error - we assume if it was added locally, we
      // succeeded 
    }
   
    
    // if it is not a forward request, for each fail, try to tell them to
    // recover - the doc was already added locally, so it should have been
    // legit

    // TODO: we should do this in the background it would seem
    for (SolrCmdDistributor.Error error : response.errors) {
      if (error.node instanceof RetryNode) {
        // we don't try to force a leader to recover
        // when we cannot forward to it
        continue;
      }
      // TODO: we should force their state to recovering ??
      // TODO: could be sent in parallel
      // TODO: do retries??
      // TODO: what if its is already recovering? Right now recoveries queue up -
      // should they?
      String recoveryUrl = error.node.getBaseUrl();
      HttpSolrServer server;
      log.info("try and ask " + recoveryUrl + " to recover");
      try {
        server = new HttpSolrServer(recoveryUrl);
        server.setSoTimeout(5000);
        server.setConnectionTimeout(5000);
        
        RequestRecovery recoverRequestCmd = new RequestRecovery();
        recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
        recoverRequestCmd.setCoreName(error.node.getCoreName());
        
        server.request(recoverRequestCmd);
      } catch (Exception e) {
        log.info("Could not tell a replica to recover", e);
      }
      
    }
  }

 
  // must be synchronized by bucket
  private void doLocalAdd(AddUpdateCommand cmd) throws IOException {
    super.processAdd(cmd);
  }

  // must be synchronized by bucket
  private void doLocalDelete(DeleteUpdateCommand cmd) throws IOException {
    super.processDelete(cmd);
  }

  /**
   * @param cmd
   * @return whether or not to drop this cmd
   * @throws IOException
   */
  private boolean versionAdd(AddUpdateCommand cmd) throws IOException {
    BytesRef idBytes = cmd.getIndexedId();

    if (vinfo == null || idBytes == null) {
      super.processAdd(cmd);
      return false;
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash here)
    int bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    // TODO: don't reuse update commands any more!
    long versionOnUpdate = cmd.getVersion();

    if (versionOnUpdate == 0) {
      SolrInputField versionField = cmd.getSolrInputDocument().getField(VersionInfo.VERSION_FIELD);
      if (versionField != null) {
        Object o = versionField.getValue();
        versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
      } else {
        // Find the version
        String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
        versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
      }
    }

    boolean isReplay = (cmd.getFlags() & UpdateCommand.REPLAY) != 0;
    boolean leaderLogic = isLeader && !isReplay;


    VersionBucket bucket = vinfo.bucket(bucketHash);

    vinfo.lockForUpdate();
    try {
      synchronized (bucket) {
        // we obtain the version when synchronized and then do the add so we can ensure that
        // if version1 < version2 then version1 is actually added before version2.

        // even if we don't store the version field, synchronizing on the bucket
        // will enable us to know what version happened first, and thus enable
        // realtime-get to work reliably.
        // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
        // there may be other reasons in the future for a version on the commands
        if (versionsStored) {

          long bucketVersion = bucket.highest;

          if (leaderLogic) {

            boolean updated = getUpdatedDocument(cmd);
            if (updated && versionOnUpdate == -1) {
              versionOnUpdate = 1;  // implied "doc must exist" for now...
            }

            if (versionOnUpdate != 0) {
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              long foundVersion = lastVersion == null ? -1 : lastVersion;
              if ( versionOnUpdate == foundVersion || (versionOnUpdate < 0 && foundVersion < 0) || (versionOnUpdate==1 && foundVersion > 0) ) {
                // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
                // specified it must exist (versionOnUpdate==1) and it does.
              } else {
                throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getPrintableId() + " expected=" + versionOnUpdate + " actual=" + foundVersion);
              }
            }


            long version = vinfo.getNewClock();
            cmd.setVersion(version);
            cmd.getSolrInputDocument().setField(VersionInfo.VERSION_FIELD, version);
            bucket.updateHighest(version);
          } else {
            // The leader forwarded us this update.
            cmd.setVersion(versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.add(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occured.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update.  we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered.  We need to drop this update.
                return true;
              }
            }
          }
        }

        doLocalAdd(cmd);
      }  // end synchronized (bucket)
    } finally {
      vinfo.unlockForUpdate();
    }
    return false;
  }


  // TODO: may want to switch to using optimistic locking in the future for better concurrency
  // that's why this code is here... need to retry in a loop closely around/in versionAdd
  boolean getUpdatedDocument(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    boolean update = false;
    for (SolrInputField sif : sdoc.values()) {
      if (sif.getValue() instanceof Map) {
        update = true;
        break;
      }
    }

    if (!update) return false;

    BytesRef id = cmd.getIndexedId();
    SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(cmd.getReq().getCore(), id);

    if (oldDoc == null) {
      // not found... allow this in the future (depending on the details of the update, or if the user explicitly sets it).
      // could also just not change anything here and let the optimistic locking throw the error
      throw new SolrException(ErrorCode.CONFLICT, "Document not found for update.  id=" + cmd.getPrintableId());
    }

    oldDoc.remove(VERSION_FIELD);

    for (SolrInputField sif : sdoc.values()) {
      Object val = sif.getValue();
      if (val instanceof Map) {
        for (Entry<String,Object> entry : ((Map<String,Object>) val).entrySet()) {
          String key = entry.getKey();
          Object fieldVal = entry.getValue();
          if ("add".equals(key)) {
            oldDoc.addField( sif.getName(), fieldVal, sif.getBoost());
          } else if ("set".equals(key)) {
            oldDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
          } else if ("inc".equals(key)) {
            SolrInputField numericField = oldDoc.get(sif.getName());
            if (numericField == null) {
              oldDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
            } else {
              // TODO: fieldtype needs externalToObject?
              String oldValS = numericField.getFirstValue().toString();
              SchemaField sf = cmd.getReq().getSchema().getField(sif.getName());
              BytesRef term = new BytesRef();
              sf.getType().readableToIndexed(oldValS, term);
              Object oldVal = sf.getType().toObject(sf, term);

              String fieldValS = fieldVal.toString();
              Number result;
              if (oldVal instanceof Long) {
                result = ((Long) oldVal).longValue() + Long.parseLong(fieldValS);
              } else if (oldVal instanceof Float) {
                result = ((Float) oldVal).floatValue() + Float.parseFloat(fieldValS);
              } else if (oldVal instanceof Double) {
                result = ((Double) oldVal).doubleValue() + Double.parseDouble(fieldValS);
              } else {
                // int, short, byte
                result = ((Integer) oldVal).intValue() + Integer.parseInt(fieldValS);
              }

              oldDoc.setField(sif.getName(),  result, sif.getBoost());
            }

          }
        }
      } else {
        // normal fields are treated as a "set"
        oldDoc.put(sif.getName(), sif);
      }

    }

    cmd.solrDoc = oldDoc;
    return true;
  }



  
  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    if (!cmd.isDeleteById()) {
      doDeleteByQuery(cmd);
      return;
    }

    int hash = 0;
    if (zkEnabled) {
      zkCheck();
      hash = hash(cmd);
      nodes = setupRequest(hash);
    } else {
      // even in non zk mode, tests simulate updates from a leader
      isLeader = !req.getParams().getBool(SEEN_LEADER, false);
    }
    
    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd  = versionDelete(cmd);
    }
    
    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    ModifiableSolrParams params = null;
    if (nodes != null) {
      params = new ModifiableSolrParams(req.getParams());
      if (isLeader) {
        params.set(SEEN_LEADER, true);
      }
      params.remove("commit"); // we already will have forwarded this from our local commit
      cmdDistrib.distribDelete(cmd, nodes, params);
    }

    // cmd.getIndexId == null when delete by query
    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && cmd.getIndexedId() != null && idField != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<String>();
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  public void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // even in non zk mode, tests simulate updates from a leader
    if(!zkEnabled) {
      isLeader = !req.getParams().getBool(SEEN_LEADER, false);
    } else {
      zkCheck();
    }

    // Lev1: we are the first to receive this deleteByQuery, it must be forwarded to the leader of every shard
    // Lev2: we are a leader receiving a forwarded deleteByQuery... we must:
    //       - block all updates (use VersionInfo)
    //       - flush *all* updates going to our replicas
    //       - forward the DBQ to our replicas and wait for the response
    //       - log + execute the local DBQ
    // Lev3: we are a replica receiving a DBQ from our leader
    //       - log + execute the local DBQ

    int dbqlevel = req.getParams().getInt(DELETE_BY_QUERY_LEVEL, 1);

    if (zkEnabled && dbqlevel == 1) {
      boolean leaderForAnyShard = false;  // start off by assuming we are not a leader for any shard

      Map<String,Slice> slices = zkController.getCloudState().getSlices(collection);
      if (slices == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Cannot find collection:" + collection + " in "
                + zkController.getCloudState().getCollections());
      }

      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(DELETE_BY_QUERY_LEVEL, 2);

      List<Node> leaders =  new ArrayList<Node>(slices.size());
      for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
        String sliceName = sliceEntry.getKey();
        ZkNodeProps leaderProps;
        try {
          leaderProps = zkController.getZkStateReader().getLeaderProps(collection, sliceName);
        } catch (InterruptedException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Exception finding leader for shard " + sliceName, e);
        }

        // TODO: What if leaders changed in the meantime?
        // should we send out slice-at-a-time and if a node returns "hey, I'm not a leader" (or we get an error because it went down) then look up the new leader?

        // Am I the leader for this slice?
        ZkCoreNodeProps coreLeaderProps = new ZkCoreNodeProps(leaderProps);
        String leaderNodeName = coreLeaderProps.getCoreNodeName();
        String coreName = req.getCore().getName();
        String coreNodeName = zkController.getNodeName() + "_" + coreName;
        isLeader = coreNodeName.equals(leaderNodeName);

        if (isLeader) {
          // don't forward to ourself
          leaderForAnyShard = true;
        } else {
          leaders.add(new StdNode(coreLeaderProps));
        }
      }

      params.remove("commit"); // this will be distributed from the local commit
      cmdDistrib.distribDelete(cmd, leaders, params);

      if (!leaderForAnyShard) {
        return;
      }

      // change the level to 2 so we look up and forward to our own replicas (if any)
      dbqlevel = 2;
    }

    List<Node> replicas = null;

    if (zkEnabled && dbqlevel == 2) {
      // This core should be a leader
      replicas = setupRequest();
    }

    if (vinfo == null) {
      super.processDelete(cmd);
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    versionOnUpdate = Math.abs(versionOnUpdate);  // normalize to positive version

    boolean isReplay = (cmd.getFlags() & UpdateCommand.REPLAY) != 0;
    boolean leaderLogic = isLeader && !isReplay;

    if (!leaderLogic && versionOnUpdate==0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    vinfo.blockUpdates();
    try {

      if (versionsStored) {
        if (leaderLogic) {
          long version = vinfo.getNewClock();
          cmd.setVersion(-version);
          // TODO update versions in all buckets

          doLocalDelete(cmd);

        } else {
          cmd.setVersion(-versionOnUpdate);

          if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.deleteByQuery(cmd);
            return;
          }

          doLocalDelete(cmd);
        }
      }

      // since we don't know which documents were deleted, the easiest thing to do is to invalidate
      // all real-time caches (i.e. UpdateLog) which involves also getting a new version of the IndexReader
      // (so cache misses will see up-to-date data)

    } finally {
      vinfo.unblockUpdates();
    }



    // TODO: need to handle reorders to replicas somehow
    // forward to all replicas
    if (leaderLogic && replicas != null) {
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set(DELETE_BY_QUERY_LEVEL, 3);
      params.set(VERSION_FIELD, Long.toString(cmd.getVersion()));
      params.set(SEEN_LEADER, "true");
      cmdDistrib.distribDelete(cmd, replicas, params);
      cmdDistrib.finish();
    }


    if (returnVersions && rsp != null) {
      if (deleteByQueryResponse == null) {
        deleteByQueryResponse = new NamedList<String>();
        rsp.add("deleteByQuery",deleteByQueryResponse);
      }
      deleteByQueryResponse.add(cmd.getQuery(), cmd.getVersion());
    }
  }


  private void zkCheck() {
    int retries = 10;
    while (!zkController.isConnected()) {
      
      if (retries-- == 0) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Cannot talk to ZooKeeper - Updates are disabled.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    
  }

  private boolean versionDelete(DeleteUpdateCommand cmd) throws IOException {

    BytesRef idBytes = cmd.getIndexedId();

    if (vinfo == null || idBytes == null) {
      super.processDelete(cmd);
      return false;
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash here)
    int bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    long signedVersionOnUpdate = versionOnUpdate;
    versionOnUpdate = Math.abs(versionOnUpdate);  // normalize to positive version

    boolean isReplay = (cmd.getFlags() & UpdateCommand.REPLAY) != 0;
    boolean leaderLogic = isLeader && !isReplay;

    if (!leaderLogic && versionOnUpdate==0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    VersionBucket bucket = vinfo.bucket(bucketHash);

    vinfo.lockForUpdate();
    try {

      synchronized (bucket) {
        if (versionsStored) {
          long bucketVersion = bucket.highest;

          if (leaderLogic) {

            if (signedVersionOnUpdate != 0) {
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              long foundVersion = lastVersion == null ? -1 : lastVersion;
              if ( (signedVersionOnUpdate == foundVersion) || (signedVersionOnUpdate < 0 && foundVersion < 0) || (signedVersionOnUpdate == 1 && foundVersion > 0) ) {
                // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
                // specified it must exist (versionOnUpdate==1) and it does.
              } else {
                throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getId() + " expected=" + signedVersionOnUpdate + " actual=" + foundVersion);
              }
            }

            long version = vinfo.getNewClock();
            cmd.setVersion(-version);
            bucket.updateHighest(version);
          } else {
            cmd.setVersion(-versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.delete(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occured.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update.  we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered.  We need to drop this update.
                return true;
              }
            }
          }
        }

        doLocalDelete(cmd);
        return false;
      }  // end synchronized (bucket)

    } finally {
      vinfo.unlockForUpdate();
    }
  }


  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    if (zkEnabled) {
      zkCheck();
    }
    
    if (vinfo != null) {
      vinfo.lockForUpdate();
    }
    try {

      if (ulog == null || ulog.getState() == UpdateLog.State.ACTIVE || (cmd.getFlags() & UpdateCommand.REPLAY) != 0) {
        super.processCommit(cmd);
      } else {
        log.info("Ignoring commit while not ACTIVE - state: " + ulog.getState() + " replay:" + (cmd.getFlags() & UpdateCommand.REPLAY));
      }

    } finally {
      if (vinfo != null) {
        vinfo.unlockForUpdate();
      }
    }
    // TODO: we should consider this? commit everyone in the current collection

    if (zkEnabled) {
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      if (!params.getBool(COMMIT_END_POINT, false)) {
        params.set(COMMIT_END_POINT, true);

        String nodeName = req.getCore().getCoreDescriptor().getCoreContainer()
            .getZkController().getNodeName();
        String shardZkNodeName = nodeName + "_" + req.getCore().getName();
        List<Node> nodes = getCollectionUrls(req, req.getCore().getCoreDescriptor()
            .getCloudDescriptor().getCollectionName(), shardZkNodeName);

        if (nodes != null) {
          cmdDistrib.distribCommit(cmd, nodes, params);
          finish();
        }
      }
    }
  }
  
  @Override
  public void finish() throws IOException {
    doFinish();
    
    if (next != null && nodes == null) next.finish();
  }
 

  
  private List<Node> getCollectionUrls(SolrQueryRequest req, String collection, String shardZkNodeName) {
    CloudState cloudState = req.getCore().getCoreDescriptor()
        .getCoreContainer().getZkController().getCloudState();
    List<Node> urls = new ArrayList<Node>();
    Map<String,Slice> slices = cloudState.getSlices(collection);
    if (slices == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
          "Could not find collection in zk: " + cloudState);
    }
    for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
      Slice replicas = slices.get(sliceEntry.getKey());

      Map<String,ZkNodeProps> shardMap = replicas.getShards();
      
      for (Entry<String,ZkNodeProps> entry : shardMap.entrySet()) {
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
        if (cloudState.liveNodesContain(nodeProps.getNodeName()) && !entry.getKey().equals(shardZkNodeName)) {
          urls.add(new StdNode(nodeProps));
        }
      }
    }
    if (urls.size() == 0) {
      return null;
    }
    return urls;
  }
  
  // TODO: move this to AddUpdateCommand/DeleteUpdateCommand and cache it? And
  // make the hash pluggable of course.
  // The hash also needs to be pluggable
  private int hash(AddUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
  
  private int hash(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
  
  // RetryNodes are used in the case of 'forward to leader' where we want
  // to try the latest leader on a fail in the case the leader just went down.
  public static class RetryNode extends StdNode {
    
    private ZkStateReader zkStateReader;
    private String collection;
    private String shardId;
    
    public RetryNode(ZkCoreNodeProps nodeProps, ZkStateReader zkStateReader, String collection, String shardId) {
      super(nodeProps);
      this.zkStateReader = zkStateReader;
      this.collection = collection;
      this.shardId = shardId;
    }
    
    @Override
    public String toString() {
      return url;
    }

    @Override
    public boolean checkRetry() {
      ZkCoreNodeProps leaderProps;
      try {
        leaderProps = new ZkCoreNodeProps(zkStateReader.getLeaderProps(
            collection, shardId));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      
      this.url = leaderProps.getCoreUrl();

      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      RetryNode other = (RetryNode) obj;
      if (url == null) {
        if (other.url != null) return false;
      } else if (!url.equals(other.url)) return false;

      return true;
    }
  }
  
}
