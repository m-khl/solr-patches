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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A logging processor.  This keeps track of all commands that have passed through
 * the chain and prints them on finish().  At the Debug (FINE) level, a message
 * will be logged for each command prior to the next stage in the chain.
 * 
 * If the Log level is not >= INFO the processor will not be created or added to the chain.
 * 
 * @since solr 1.3
 */
public class LogUpdateProcessorFactory extends UpdateRequestProcessorFactory {
  
  int maxNumToLog = 10;
  
  @Override
  public void init( final NamedList args ) {
    if( args != null ) {
      SolrParams params = SolrParams.toSolrParams( args );
      maxNumToLog = params.getInt( "maxNumToLog", maxNumToLog );
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    final Logger logger = LoggerFactory.getLogger(LogUpdateProcessor.class);
    boolean doLog = logger.isInfoEnabled();
    // LogUpdateProcessor.log.error("Will Log=" + doLog);
    if( doLog ) {
      // only create the log processor if we will use it
      final LogUpdateProcessor processor = new LogUpdateProcessor(req, rsp, this, next);
      assert processor.log == logger;
      return processor;
    }
    return null;
  }
}

class LogUpdateProcessor extends UpdateRequestProcessor {
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final NamedList<Object> toLog;

  int numAdds;
  int numDeletes;

  // hold on to the added list for logging and the response
  private List<String> adds;
  private List<String> deletes;

  private final int maxNumToLog;

  private final boolean logDebug = log.isDebugEnabled();//cache to avoid volatile-read

  public LogUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, LogUpdateProcessorFactory factory, UpdateRequestProcessor next) {
    super( next );
    this.req = req;
    this.rsp = rsp;
    maxNumToLog = factory.maxNumToLog;  // TODO: make configurable
    // TODO: make log level configurable as well, or is that overkill?
    // (ryan) maybe?  I added it mostly to show that it *can* be configurable

    this.toLog = new SimpleOrderedMap<Object>();
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (logDebug) { log.debug(cmd.toString()); }

    // call delegate first so we can log things like the version that get set later
    if (next != null) next.processAdd(cmd);

    // Add a list of added id's to the response
    if (adds == null) {
      adds = new ArrayList<String>();
      toLog.add("add",adds);
    }

    if (adds.size() < maxNumToLog) {
      long version = cmd.getVersion();
      String msg = cmd.getPrintableId();
      if (version != 0) msg = msg + " (" + version + ')';
      adds.add(msg);
    }

    numAdds++;
  }

  @Override
  public void processDelete( DeleteUpdateCommand cmd ) throws IOException {
    if (logDebug) { log.debug(cmd.toString()); }
    if (next != null) next.processDelete(cmd);

    if (cmd.isDeleteById()) {
      if (deletes == null) {
        deletes = new ArrayList<String>();
        toLog.add("delete",deletes);
      }
      if (deletes.size() < maxNumToLog) {
        long version = cmd.getVersion();
        String msg = cmd.getId();
        if (version != 0) msg = msg + " (" + version + ')';
        deletes.add(msg);
      }
    } else {
      if (toLog.size() < maxNumToLog) {
        long version = cmd.getVersion();
        String msg = cmd.query;
        if (version != 0) msg = msg + " (" + version + ')';
        toLog.add("deleteByQuery", msg);
      }
    }
    numDeletes++;

  }

  @Override
  public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
    if (logDebug) { log.debug(cmd.toString()); }
    if (next != null) next.processMergeIndexes(cmd);

    toLog.add("mergeIndexes", cmd.toString());
  }

  @Override
  public void processCommit( CommitUpdateCommand cmd ) throws IOException {
    if (logDebug) { log.debug(cmd.toString()); }
    if (next != null) next.processCommit(cmd);


    final String msg = cmd.optimize ? "optimize" : "commit";
    toLog.add(msg, "");
  }

  /**
   * @since Solr 1.4
   */
  @Override
  public void processRollback( RollbackUpdateCommand cmd ) throws IOException {
    if (logDebug) { log.debug(cmd.toString()); }
    if (next != null) next.processRollback(cmd);

    toLog.add("rollback", "");
  }


  @Override
  public void finish() throws IOException {
    if (next != null) next.finish();
    if (logDebug) { log.debug("finish"); }

    // LOG A SUMMARY WHEN ALL DONE (INFO LEVEL)
    
    // TODO: right now, update requests are logged twice...
    // this will slow down things compared to Solr 1.2
    // we should have extra log info on the SolrQueryResponse, to
    // be logged by SolrCore
    
    // if id lists were truncated, show how many more there were
    if (adds != null && numAdds > maxNumToLog) {
      adds.add("... (" + numAdds + " adds)");
    }
    if (deletes != null && numDeletes > maxNumToLog) {
      deletes.add("... (" + numDeletes + " deletes)");
    }
    long elapsed = rsp.getEndTime() - req.getStartTime();
    log.info( ""+toLog + " 0 " + (elapsed) );
  }
}



