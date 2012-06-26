package org.apache.solr.update.processor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class ThreadedUpdateProcessorFactiory extends
    UpdateRequestProcessorFactory implements SolrCoreAware{
  
  private final static AddUpdateCommand eof = new AddUpdateCommand(null){
    public UpdateCommand clone() {
      return eof;
    };
    private Object readResolve() throws ObjectStreamException{
      return eof;
    }
    public String toString() {
      return "eof";
    };
  };
  
  protected final ExecutorService executor = Executors.newCachedThreadPool();

  public class ThreadedUpdateProcessor extends UpdateRequestProcessor {
    
    private final Logger logger = LoggerFactory.getLogger(ThreadedUpdateProcessor.class);

    private static final int numBufferedTasks = 10;
    private static final int numPipes = 3;
    
    protected BlockingQueue<AddUpdateCommand> buffer = new LinkedBlockingQueue<AddUpdateCommand>(numBufferedTasks);
    protected List<Future<Integer>> pipes;
    protected UpdateRequestProcessorChain backingChain;
    protected SolrQueryRequest req;
    protected SolrQueryResponse rsp;
    
    public ThreadedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      super(next);
      this.req = req;
      this.rsp = rsp;
      backingChain = req.getCore().getUpdateProcessingChain(
          req.getParams().get("backing.chain")
      );
    }
    
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      final AddUpdateCommand copycat = (AddUpdateCommand) cmd.clone();
      try {
        buffer.put(copycat);
        startPipes();
      } catch (InterruptedException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "interrupted while queueing "+ copycat,e);
      }
    }
    
    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      stopPipes();
      super.processCommit(cmd);
    }
    
    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      stopPipes();
      super.processDelete(cmd);
    }
    
    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      stopPipes();
      super.processMergeIndexes(cmd);
    }
    
    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      stopPipes();
      super.processRollback(cmd);
    }
    
    private void startPipes() {
      if(pipes==null){
        pipes = new ArrayList<Future<Integer>>();
        for(int i=0;i<numPipes;i++){
          pipes.add(
              executor.submit(new Callable<Integer>() {

                private final UpdateRequestProcessor backingProcessor =
                        backingChain.createProcessor(req, rsp);
                
                {
                  if(backingProcessor instanceof ThreadedUpdateProcessor){
                    throw new SolrException(ErrorCode.SERVER_ERROR, 
                        "threaded update processors are cycled");
                  } 
                }
                
                @Override
                public Integer call() throws Exception {
                  AddUpdateCommand elem;
                  int cnt=0;
                  try{
                    for(;(elem = buffer.take())!=eof; cnt++){
                      try{
                        backingProcessor.processAdd(elem);
                      }catch(InterruptedIOException e){
                        logger.info("stopping pipe on"+elem+"beacuse of ",e);
                        break;
                      }//break on InterruptedEx too but it won't be thrown ever
                      
                      catch(Exception e){ // make sense for IO & Runtime. Really? 
                        logger.warn("ignoring exception for "+elem+" ",e);
                      }
                    }
                    logger.debug("finishing pipe on {}",elem);
                  }finally{
                    backingProcessor.finish();
                  }
                  return cnt;
                }
              })
          );
        }
      }
    }

    @Override
    public void finish() throws IOException {
      stopPipes();
      super.finish();
    }

    private void stopPipes() {
      if(pipes!=null){
        // drain buffer on urgent halt, e.g. rollback
        try{
          for(Future<Integer> i:pipes){
            try {
              buffer.put(eof);
            } catch (InterruptedException e) {
              logger.warn("ignoring while stopping pipes", e);
              // shouldn't I retry put? I should, I think
            }
          }
          
          List<Object> pipeCounters = new ArrayList<Object>();
          for(Future<Integer> pipe:pipes){
            final Integer progress;
            try{
              progress = pipe.get();
              pipeCounters.add(progress);
            }catch(InterruptedException e){
              pipeCounters.add(e);
              logger.warn("ignoring while awaiting pipe. interrupting a pipe", e);
              pipe.cancel(true);
            }catch(ExecutionException ee){
              pipeCounters.add(ee.getCause());
            }
          }
          logger.debug("by pipes processing counters:{}",pipeCounters);
       // drain buffer
          final ArrayList dust = new ArrayList();
          if(buffer.drainTo(dust)!=0){
            logger.error("buffer contains {} after stopping pipes", dust);
          }
          assert dust.isEmpty():" fail test beacuse of that";
        }finally{
          pipes=null;
        }
      }
    }
  }
  
  

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new ThreadedUpdateProcessor(req, rsp, next);
  }

  @Override
  public void inform(SolrCore core) {
    core.addCloseHook(new CloseHook() {
      
      @Override
      public void preClose(SolrCore core) {
        executor.shutdownNow();
      }
      
      @Override
      public void postClose(SolrCore core) {}
    });
  }
  
}
