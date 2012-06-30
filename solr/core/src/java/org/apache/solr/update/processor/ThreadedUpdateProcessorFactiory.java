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
import org.apache.solr.common.util.NamedList;
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
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.eclipse.jetty.util.log.Log;
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
    UpdateRequestProcessorFactory implements SolrCoreAware, NamedListInitializedPlugin{
  
  protected final static AddUpdateCommand eof = new AddUpdateCommand(null){
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

  private static final String backingChainParam = "backing.chain";
  
  protected ExecutorService executor;

  protected int buffer;

  protected int pipes;

  protected static class ThreadedUpdateProcessor extends UpdateRequestProcessor {
    
    protected static class Pipe implements Callable<Integer> {
      protected final UpdateRequestProcessor backingProcessor;
      protected final BlockingQueue<AddUpdateCommand> buffer ;
      
      protected Pipe(UpdateRequestProcessor backingProcessor, BlockingQueue<AddUpdateCommand> buffer) {
        this.backingProcessor = backingProcessor;
        if(backingProcessor instanceof ThreadedUpdateProcessor){
          throw new SolrException(ErrorCode.SERVER_ERROR, 
              "threaded update processors are cycled");
        } 
        this.buffer = buffer;
      }
      
      @Override
      public Integer call() throws Exception {
        AddUpdateCommand elem=null;
        int cnt=0;
        int failures=0;
        Exception kept = null;
        try{
          for(;(elem = buffer.take())!=eof; cnt++){
            try{
              backingProcessor.processAdd(elem);
            }catch(InterruptedIOException e){
              logger.info("stopping pipe on"+elem+"beacuse of ",e);
              break;
            }//break on InterruptedEx too but it won't be thrown ever
            catch(Exception e){ // make sense for IO & Runtime. Really? 
              logger.warn("{} exception for {} pipe failures {} ", new Object[]{
                  (kept==null? "storing":"ignoring"),
                  e, 
                  ++failures}); 
              if(kept==null){
                kept = e;
              }
              // ignoring is valid for analisys exception, but if hdd is broken how much sense in continuing 
            }
          }
          // and after the stream is over let's let them know that there was a failure
          if(kept!=null){
            throw kept;
          }
        }finally{
          logger.debug("finishing pipe on {}",elem);
          backingProcessor.finish();
        }
        return cnt;
      }
    }

    private final static Logger logger = LoggerFactory.getLogger(ThreadedUpdateProcessor.class);

    protected final int pipesNumber ;
    
    protected final BlockingQueue<AddUpdateCommand> buffer ;
    protected List<Future<Integer>> pipes;
    protected final UpdateRequestProcessorChain backingChain;
    protected final SolrQueryRequest req;
    protected final SolrQueryResponse rsp;
    protected final ExecutorService executor;
    protected final UpdateRequestProcessor delegate;
    
    public ThreadedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp,
        UpdateRequestProcessor next, ExecutorService executor,
        int bufferSize, int maxPipesNumber,
        UpdateRequestProcessorChain backingChain) {
      super(next);
      if(next!=null){
        throw new SolrException(ErrorCode.SERVER_ERROR, "please use "+backingChainParam +
        		"instead of regular processors chaining with "+next);
      }
      this.req = req;
      this.rsp = rsp;
      this.backingChain = backingChain;
      delegate = backingChain.createProcessor(req, rsp);
      this.pipesNumber = maxPipesNumber;    
      buffer = new LinkedBlockingQueue<AddUpdateCommand>(bufferSize);
      this.executor = executor;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      // I'm ready for 0 size buffer
      startPipes();
      final AddUpdateCommand copycat = (AddUpdateCommand) cmd.clone();
      try {
        buffer.put(copycat);
      } catch (InterruptedException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "interrupted while queueing "+ copycat,e);
      }
    }
    
    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      try{
        stopPipes();
      }finally{
        delegate.processCommit(cmd);
      }
    }
    
    /* 
     * TODO can be also async op
     */
    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      try{
        stopPipes();
      }finally{
        delegate.processDelete(cmd);
      }
    }
    
    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      try{
        stopPipes();
      }finally{
        delegate.processMergeIndexes(cmd);
      }
    }
    
    /* 
     * TODO can urgently halt queued tasks
     */
    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      try{
        stopPipes();
      }finally{
        delegate.processRollback(cmd);
      }
    }
    
    /**
     * TODO consider steady ramp-up instead run all
     * */
    protected void startPipes() {
      if(pipes==null){
        pipes = new ArrayList<Future<Integer>>();
        for(int i=0;i<pipesNumber;i++){
          final UpdateRequestProcessor backingProcessor =
              backingChain.createProcessor(req, rsp);
          pipes.add(
              executor.submit(new Pipe(backingProcessor, buffer))
          );
        }
      }
    }

    @Override
    public void finish() throws IOException {
      try{
        stopPipes();
      }finally{
        delegate.finish();
      }
      assert buffer.isEmpty() : "but it has "+buffer; // we might need wipe buffer with null
    }

    protected void stopPipes() throws IOException{
      if(pipes!=null){
        // drain buffer on urgent halt, e.g. rollback
        try{
          for(Future<Integer> i:pipes){
            // we can wait so long, therefore can be interrupted
            buffer.put(eof);
          }
          
          Throwable problem = null;
          List<Object> pipeCounters = new ArrayList<Object>();
          for(Future<Integer> pipe:pipes){
            final Integer progress;
            try{
              progress = pipe.get();
              pipeCounters.add(progress);
            }catch(ExecutionException ee){
              Throwable cause = ee.getCause();
              pipeCounters.add(cause );
              if(problem==null){
                problem = cause;
              }
            }
          }
          logger.debug("per pipes processing counters:{}",pipeCounters);
       // drain buffer
          final ArrayList dust = new ArrayList();
          if(buffer.drainTo(dust)!=0){
            logger.error("buffer contains {} after stopping pipes", dust);
          }
          assert dust.isEmpty():" fail test beacuse of that";
          if(problem!=null){
            throw new SolrException(ErrorCode.BAD_REQUEST, "rethrowing one of the pipes problem",problem);
          }
        }
        catch(InterruptedException ie){// in this case aggressively cancel everything 
          List<Boolean> cancels = new ArrayList<Boolean>();
          for(Future<Integer> pipe:pipes){
            cancels.add(
              pipe.cancel(true)
            );
          }//rethrow???
          logger.info("pipes have been canceled "+cancels, ie);
        }
        finally{
          pipes=null;
        }
      }
    }
  }
  
  @Override
  public void init(NamedList args) {
    buffer = (Integer) args.get("bufferSize");
    pipes = (Integer) args.get("pipesNumber");
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    String backingName = req.getParams().get(backingChainParam);
    UpdateRequestProcessorChain backing = req.getCore().getUpdateProcessingChain(
        backingName
    );
    for(UpdateRequestProcessorFactory f : backing.getFactories()){
      if(f instanceof ThreadedUpdateProcessorFactiory){
        throw new SolrException(ErrorCode.SERVER_ERROR, "backing chain "+backingName+
            " contains "+f);
      }
    }
    return new ThreadedUpdateProcessor(req, rsp, next,
        executor,
        buffer, pipes,
        backing);
  }

  protected UpdateRequestProcessorChain obtainChain(SolrQueryRequest req) {
    return req.getCore().getUpdateProcessingChain(
        req.getParams().get(backingChainParam)
    );
  }
  
  @Override
  public void inform(SolrCore core) {
    executor = Executors.newCachedThreadPool();
    
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
