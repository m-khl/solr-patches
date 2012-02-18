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

package org.apache.solr.handler.admin;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @since solr 1.3
 */
public class CoreAdminHandler extends RequestHandlerBase {
  protected static Logger log = LoggerFactory.getLogger(CoreAdminHandler.class);
  protected final CoreContainer coreContainer;

  public CoreAdminHandler() {
    super();
    // Unlike most request handlers, CoreContainer initialization 
    // should happen in the constructor...  
    this.coreContainer = null;
  }


  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public CoreAdminHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @Override
  final public void init(NamedList args) {
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "CoreAdminHandler should not be configured in solrconf.xml\n" +
                    "it is a special Handler configured directly by the RequestDispatcher");
  }

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance that created this
   * handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    CoreContainer cores = getCoreContainer();
    if (cores == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Core container instance missing");
    }
    boolean doPersist = false;

    // Pick the action
    SolrParams params = req.getParams();
    CoreAdminAction action = CoreAdminAction.STATUS;
    String a = params.get(CoreAdminParams.ACTION);
    if (a != null) {
      action = CoreAdminAction.get(a);
      if (action == null) {
        doPersist = this.handleCustomAction(req, rsp);
      }
    }
    if (action != null) {
      switch (action) {
        case CREATE: {
          doPersist = this.handleCreateAction(req, rsp);
          break;
        }

        case RENAME: {
          doPersist = this.handleRenameAction(req, rsp);
          break;
        }

        case ALIAS: {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'ALIAS' is not supported " +
            req.getParams().get(CoreAdminParams.ACTION));
        }

        case UNLOAD: {
          doPersist = this.handleUnloadAction(req, rsp);
          break;
        }

        case STATUS: {
          doPersist = this.handleStatusAction(req, rsp);
          break;

        }

        case PERSIST: {
          doPersist = this.handlePersistAction(req, rsp);
          break;
        }

        case RELOAD: {
          doPersist = this.handleReloadAction(req, rsp);
          break;
        }

        case SWAP: {
          doPersist = this.handleSwapAction(req, rsp);
          break;
        }

        case MERGEINDEXES: {
          doPersist = this.handleMergeAction(req, rsp);
          break;
        }

        case PREPRECOVERY: {
          this.handleWaitForStateAction(req, rsp);
          break;
        }
        
        case REQUESTRECOVERY: {
          this.handleRequestRecoveryAction(req, rsp);
          break;
        }
        
        case DISTRIBURL: {
          this.handleDistribUrlAction(req, rsp);
          break;
        }
        
        default: {
          doPersist = this.handleCustomAction(req, rsp);
          break;
        }
        case LOAD:
          break;
      }
    }
    // Should we persist the changes?
    if (doPersist) {
      cores.persist();
      rsp.add("saved", cores.getConfigFile().getAbsolutePath());
    }
    rsp.setHttpCaching(false);
  }

  protected boolean handleMergeAction(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrParams params = req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    SolrCore core = coreContainer.getCore(cname);
    SolrQueryRequest wrappedReq = null;

    SolrCore[] sourceCores = null;
    RefCounted<SolrIndexSearcher>[] searchers = null;
    // stores readers created from indexDir param values
    DirectoryReader[] readersToBeClosed = null;
    Directory[] dirsToBeReleased = null;
    if (core != null) {
      try {
        String[] dirNames = params.getParams(CoreAdminParams.INDEX_DIR);
        if (dirNames == null || dirNames.length == 0) {
          String[] sources = params.getParams("srcCore");
          if (sources == null || sources.length == 0)
            throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                "At least one indexDir or srcCore must be specified");

          sourceCores = new SolrCore[sources.length];
          for (int i = 0; i < sources.length; i++) {
            String source = sources[i];
            SolrCore srcCore = coreContainer.getCore(source);
            if (srcCore == null)
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "Core: " + source + " does not exist");
            sourceCores[i] = srcCore;
          }
        } else  {
          readersToBeClosed = new DirectoryReader[dirNames.length];
          dirsToBeReleased = new Directory[dirNames.length];
          DirectoryFactory dirFactory = core.getDirectoryFactory();
          for (int i = 0; i < dirNames.length; i++) {
            Directory dir = dirFactory.get(dirNames[i], core.getSolrConfig().mainIndexConfig.lockType);
            dirsToBeReleased[i] = dir;
            // TODO: why doesn't this use the IR factory? what is going on here?
            readersToBeClosed[i] = DirectoryReader.open(dir);
          }
        }

        DirectoryReader[] readers = null;
        if (readersToBeClosed != null)  {
          readers = readersToBeClosed;
        } else {
          readers = new DirectoryReader[sourceCores.length];
          searchers = new RefCounted[sourceCores.length];
          for (int i = 0; i < sourceCores.length; i++) {
            SolrCore solrCore = sourceCores[i];
            // record the searchers so that we can decref
            searchers[i] = solrCore.getSearcher();
            readers[i] = searchers[i].get().getIndexReader();
          }
        }

        UpdateRequestProcessorChain processorChain =
                core.getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));
        wrappedReq = new LocalSolrQueryRequest(core, req.getParams());
        UpdateRequestProcessor processor =
                processorChain.createProcessor(wrappedReq, rsp);
        processor.processMergeIndexes(new MergeIndexesCommand(readers, req));
      } finally {
        if (searchers != null) {
          for (RefCounted<SolrIndexSearcher> searcher : searchers) {
            if (searcher != null) searcher.decref();
          }
        }
        if (sourceCores != null) {
          for (SolrCore solrCore : sourceCores) {
            if (solrCore != null) solrCore.close();
          }
        }
        if (readersToBeClosed != null) IOUtils.closeWhileHandlingException(readersToBeClosed);
        if (dirsToBeReleased != null) {
          for (Directory dir : dirsToBeReleased) {
            DirectoryFactory dirFactory = core.getDirectoryFactory();
            dirFactory.release(dir);
          }
        }
        if (wrappedReq != null) wrappedReq.close();
        core.close();
      }
    }
    return coreContainer.isPersistent();
  }

  /**
   * Handle Custom Action.
   * <p/>
   * This method could be overridden by derived classes to handle custom actions. <br> By default - this method throws a
   * solr exception. Derived classes are free to write their derivation if necessary.
   */
  protected boolean handleCustomAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported operation: " +
            req.getParams().get(CoreAdminParams.ACTION));
  }

  /**
   * Handle 'CREATE' action.
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   *
   * @throws SolrException in case of a configuration error.
   */
  protected boolean handleCreateAction(SolrQueryRequest req, SolrQueryResponse rsp) throws SolrException {
    try {
      SolrParams params = req.getParams();
      String name = params.get(CoreAdminParams.NAME);

      String instanceDir = params.get(CoreAdminParams.INSTANCE_DIR);
      if (instanceDir == null) {
        // instanceDir = coreContainer.getSolrHome() + "/" + name;
        instanceDir = name; // bare name is already relative to solr home
      }

      CoreDescriptor dcore = new CoreDescriptor(coreContainer, name, instanceDir);

      //  fillup optional parameters
      String opts = params.get(CoreAdminParams.CONFIG);
      if (opts != null)
        dcore.setConfigName(opts);

      opts = params.get(CoreAdminParams.SCHEMA);
      if (opts != null)
        dcore.setSchemaName(opts);

      opts = params.get(CoreAdminParams.DATA_DIR);
      if (opts != null)
        dcore.setDataDir(opts);

      CloudDescriptor cd = dcore.getCloudDescriptor();
      if (cd != null) {
        cd.setParams(req.getParams());

        opts = params.get(CoreAdminParams.COLLECTION);
        if (opts != null)
          cd.setCollectionName(opts);
        
        opts = params.get(CoreAdminParams.SHARD);
        if (opts != null)
          cd.setShardId(opts);
        
        opts = params.get(CoreAdminParams.ROLES);
        if (opts != null)
          cd.setRoles(opts);
        
        Integer numShards = params.getInt(ZkStateReader.NUM_SHARDS_PROP);
        if (numShards != null)
          cd.setNumShards(numShards);
      }
      
      // Process all property.name=value parameters and set them as name=value core properties
      Properties coreProperties = new Properties();
      Iterator<String> parameterNamesIterator = params.getParameterNamesIterator();
      while (parameterNamesIterator.hasNext()) {
          String parameterName = parameterNamesIterator.next();
          if(parameterName.startsWith(CoreAdminParams.PROPERTY_PREFIX)) {
              String parameterValue = params.get(parameterName);
              String propertyName = parameterName.substring(CoreAdminParams.PROPERTY_PREFIX.length()); // skip prefix
              coreProperties.put(propertyName, parameterValue);
          }
      }
      dcore.setCoreProperties(coreProperties);
      
      SolrCore core = coreContainer.create(dcore);
      coreContainer.register(name, core, false);
      rsp.add("core", core.getName());
      return coreContainer.isPersistent();
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Error executing default implementation of CREATE", ex);
    }
  }

  /**
   * Handle "RENAME" Action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   *
   * @throws SolrException
   */
  protected boolean handleRenameAction(SolrQueryRequest req, SolrQueryResponse rsp) throws SolrException {
    SolrParams params = req.getParams();

    String name = params.get(CoreAdminParams.OTHER);
    String cname = params.get(CoreAdminParams.CORE);
    boolean doPersist = false;

    if (cname.equals(name)) return doPersist;
    
    doPersist = coreContainer.isPersistent();
    coreContainer.rename(cname, name);
    
    return doPersist;
  }

  /**
   * Handle "ALIAS" action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   */
  @Deprecated
  protected boolean handleAliasAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();

    String name = params.get(CoreAdminParams.OTHER);
    String cname = params.get(CoreAdminParams.CORE);
    boolean doPersist = false;
    if (cname.equals(name)) return doPersist;

    SolrCore core = coreContainer.getCore(cname);
    if (core != null) {
      doPersist = coreContainer.isPersistent();
      coreContainer.register(name, core, false);
      // no core.close() since each entry in the cores map should increase the ref
    }
    return doPersist;
  }


  /**
   * Handle "UNLOAD" Action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   */
  protected boolean handleUnloadAction(SolrQueryRequest req, SolrQueryResponse rsp) throws SolrException {
    SolrParams params = req.getParams();
    String cname = params.get(CoreAdminParams.CORE);
    SolrCore core = coreContainer.remove(cname);
    if(core == null){
       throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "No such core exists '"+cname+"'");
    }
    if (params.getBool(CoreAdminParams.DELETE_INDEX, false)) {
      core.addCloseHook(new CloseHook() {
        @Override
        public void preClose(SolrCore core) {}

        @Override
        public void postClose(SolrCore core) {
          File dataDir = new File(core.getIndexDir());
          File[] files = dataDir.listFiles();
          if (files != null) {
            for (File file : files) {
              if (!file.delete()) {
                log.error(file.getAbsolutePath()
                    + " could not be deleted on core unload");
              }
            }
            if (!dataDir.delete()) log.error(dataDir.getAbsolutePath()
                + " could not be deleted on core unload");
          } else {
            log.error(dataDir.getAbsolutePath()
                + " could not be deleted on core unload");
          }
        }
      });
    }
    core.close();
    return coreContainer.isPersistent();

  }

  /**
   * Handle "STATUS" action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   */
  protected boolean handleStatusAction(SolrQueryRequest req, SolrQueryResponse rsp)
          throws SolrException {
    SolrParams params = req.getParams();

    String cname = params.get(CoreAdminParams.CORE);
    boolean doPersist = false;
    NamedList<Object> status = new SimpleOrderedMap<Object>();
    try {
      if (cname == null) {
        for (String name : coreContainer.getCoreNames()) {
          status.add(name, getCoreStatus(coreContainer, name));
        }
      } else {
        status.add(cname, getCoreStatus(coreContainer, cname));
      }
      rsp.add("status", status);
      doPersist = false; // no state change
      return doPersist;
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Error handling 'status' action ", ex);
    }
  }

  /**
   * Handler "PERSIST" action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   *
   * @throws SolrException
   */
  protected boolean handlePersistAction(SolrQueryRequest req, SolrQueryResponse rsp)
          throws SolrException {
    SolrParams params = req.getParams();
    boolean doPersist = false;
    String fileName = params.get(CoreAdminParams.FILE);
    if (fileName != null) {
      File file = new File(coreContainer.getConfigFile().getParentFile(), fileName);
      coreContainer.persistFile(file);
      rsp.add("saved", file.getAbsolutePath());
      doPersist = false;
    } else if (!coreContainer.isPersistent()) {
      throw new SolrException(SolrException.ErrorCode.FORBIDDEN, "Persistence is not enabled");
    } else
      doPersist = true;

    return doPersist;
  }

  /**
   * Handler "RELOAD" action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   */
  protected boolean handleReloadAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    String cname = params.get(CoreAdminParams.CORE);
    try {
      coreContainer.reload(cname);
      return false; // no change on reload
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error handling 'reload' action", ex);
    }
  }

  /**
   * Handle "SWAP" action
   *
   * @param req
   * @param rsp
   *
   * @return true if a modification has resulted that requires persistance 
   *         of the CoreContainer configuration.
   */
  protected boolean handleSwapAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    final SolrParams params = req.getParams();
    final SolrParams required = params.required();

    final String cname = params.get(CoreAdminParams.CORE);
    boolean doPersist = params.getBool(CoreAdminParams.PERSISTENT, coreContainer.isPersistent());
    String other = required.get(CoreAdminParams.OTHER);
    coreContainer.swap(cname, other);
    return doPersist;

  }
  
  protected void handleRequestRecoveryAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws IOException {
    final SolrParams params = req.getParams();
    
    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      cname = "";
    }
    SolrCore core = null;
    try {
      core = coreContainer.getCore(cname);
      if (core != null) {
        core.getUpdateHandler().getSolrCoreState().doRecovery(core);
      } else {
        SolrException.log(log, "Cound not find core to call recovery:" + cname);
      }
    } finally {
      // no recoveryStrat close for now
      if (core != null) {
        core.close();
      }
    }
  }
  
  protected void handleWaitForStateAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws IOException, InterruptedException {
    final SolrParams params = req.getParams();
    
    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      cname = "";
    }
    
    String nodeName = params.get("nodeName");
    String coreNodeName = params.get("coreNodeName");
    String waitForState = params.get("state");
    boolean checkLive = params.getBool("checkLive", true);
    int pauseFor = params.getInt("pauseFor", 0);
    SolrCore core =  null;

    try {
      core = coreContainer.getCore(cname);
      if (core == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "core not found:" + cname);
      }
      String state = null;
      int retry = 0;
      while (true) {
        // wait until we are sure the recovering node is ready
        // to accept updates
        CloudDescriptor cloudDescriptor = core.getCoreDescriptor()
            .getCloudDescriptor();
        CloudState cloudState = coreContainer
            .getZkController()
            .getCloudState();
        String collection = cloudDescriptor.getCollectionName();
        ZkNodeProps nodeProps = 
            cloudState.getSlice(collection,
                cloudDescriptor.getShardId()).getShards().get(coreNodeName);
        boolean live = false;
        if (nodeProps != null) {
          
          state = nodeProps.get(ZkStateReader.STATE_PROP);
          live = cloudState.liveNodesContain(nodeName);
          if (nodeProps != null && state.equals(waitForState)) {
            if (checkLive && live) {
              break;
            } else {
              break;
            }
          }
        }
        
        if (retry++ == 30) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "I was asked to wait on state " + waitForState + " for " + nodeName
                  + " but I still do not see the request state. I see state: " + state + " live:" + live);
        }
        
        Thread.sleep(1000);
      }
      
      // small safety net for any updates that started with state that
      // kept it from sending the update to be buffered -
      // pause for a while to let any outstanding updates finish
      
      Thread.sleep(pauseFor);
      
      // solrcloud_debug
//      try {
//        RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
//        SolrIndexSearcher searcher = searchHolder.get();
//        try {
//          System.out.println(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName() + " to replicate "
//              + searcher.search(new MatchAllDocsQuery(), 1).totalHits + " gen:" + core.getDeletionPolicy().getLatestCommit().getGeneration()  + " data:" + core.getDataDir());
//        } finally {
//          searchHolder.decref();
//        }
//      } catch (Exception e) {
//        
//      }
      
    } finally {
      if (core != null) {
        core.close();
      }
    }
  }
  
  protected void handleDistribUrlAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws IOException, InterruptedException, SolrServerException {
    // TODO: finish this and tests
    SolrParams params = req.getParams();
    
    SolrParams required = params.required();
    String path = required.get("path");
    String shard = params.get("shard");
    String collection = required.get("collection");
    
    SolrCore core = req.getCore();
    ZkController zkController = core.getCoreDescriptor().getCoreContainer()
        .getZkController();
    if (shard != null) {
      List<ZkCoreNodeProps> replicas = zkController.getZkStateReader().getReplicaProps(
          collection, shard, zkController.getNodeName(), core.getName());
      
      for (ZkCoreNodeProps node : replicas) {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer(node.getCoreUrl() + path);
        QueryRequest qr = new QueryRequest();
        server.request(qr);
      }

    }
  }

  protected NamedList<Object> getCoreStatus(CoreContainer cores, String cname) throws IOException {
    NamedList<Object> info = new SimpleOrderedMap<Object>();
    SolrCore core = cores.getCore(cname);
    if (core != null) {
      try {
        info.add("name", core.getName());
        info.add("instanceDir", normalizePath(core.getResourceLoader().getInstanceDir()));
        info.add("dataDir", normalizePath(core.getDataDir()));
        info.add("startTime", new Date(core.getStartTime()));
        info.add("uptime", System.currentTimeMillis() - core.getStartTime());
        RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
        try {
          SimpleOrderedMap<Object> indexInfo = LukeRequestHandler.getIndexInfo(searcher.get().getIndexReader());
          long size = getIndexSize(core);
          indexInfo.add("sizeInBytes", size);
          indexInfo.add("size", NumberUtils.readableSize(size));
          info.add("index", indexInfo);
        } finally {
          searcher.decref();
        }
      } finally {
        core.close();
      }
    }
    return info;
  }
  
  private long getIndexSize(SolrCore core) {
    return FileUtils.sizeOfDirectory(new File(core.getIndexDir()));
  }

  protected static String normalizePath(String path) {
    if (path == null)
      return null;
    path = path.replace('/', File.separatorChar);
    path = path.replace('\\', File.separatorChar);
    return path;
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage Multiple Solr Cores";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
