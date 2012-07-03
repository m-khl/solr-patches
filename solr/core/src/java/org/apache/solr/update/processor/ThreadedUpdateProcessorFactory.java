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
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Forks incoming adds into multiple threads. 
 * Instead of regular chaining it chains on it's own with chain specified in 
 * "backing.chain" update parameter. 
 * supports two solrconfig.xml parameters <ul>
 * <li>
 * "bufferSize" length of the buffer for add commands
 * </li>
 * <li>"pipesNumber" number of parallel threads for handling updates</li>
 * </ul>
 * it spawns own cached thread pool, and shutdown it on core closing.
 * AddUpdate commands are queued for background execution by spawned processors.
 * All other commands awaits until all queued tasks are completed, and then are 
 * handled in the calling thread (request).  
 * if one of the update processors threads (aka pipes) catches exception, all other pipes
 * are stopped synchronously, but not in the same moment, and the root cause is propagated 
 * into request thread. 
 * for queuing spin-delay pattern is used to prevent dead lock when buffer is full, and
 * queuing thread is blocked, but buffer can't be processed due to dead pipes.       
 * */
public class ThreadedUpdateProcessorFactory extends
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

  long offerTimeout;

  protected static class ThreadedUpdateProcessor extends UpdateRequestProcessor {
    
    private static final int oneSec = 1000;

    protected static class Pipe implements Callable<Integer> {
      protected final UpdateRequestProcessor backingProcessor;
      protected final BlockingQueue<AddUpdateCommand> buffer ;
      protected final CountDownLatch latch;
      
      protected Pipe(UpdateRequestProcessor backingProcessor,
          BlockingQueue<AddUpdateCommand> buffer,
          CountDownLatch latch ) {
        this.backingProcessor = backingProcessor;
        this.buffer = buffer;
        this.latch = latch;
      }
      
      @Override
      public Integer call() throws Exception {
        AddUpdateCommand elem=null;
        int cnt=0;
        try{
          for(;(elem = buffer.take())!=eof; cnt++){
              backingProcessor.processAdd(elem);
          }
        }  catch(Exception e){
            logger.info("stopping pipe",e);
            throw e;
        }finally{
          try{
            backingProcessor.finish();
          }finally{
            latch.countDown();
            logger.debug("pipe stopped at {}",elem);
          }
        }
        return cnt;
      }
    }

    private final static Logger logger = LoggerFactory.getLogger(ThreadedUpdateProcessor.class);

    protected final int pipesNumber ;
    
    protected CountDownLatch running; 
    protected final BlockingQueue<AddUpdateCommand> buffer ;
    protected List<Future<Integer>> pipes;
    protected final UpdateRequestProcessorChain backingChain;
    protected final SolrQueryRequest req;
    protected final SolrQueryResponse rsp;
    protected final ExecutorService executor;
    protected final UpdateRequestProcessor delegate;
    protected final long offerTimeout;
    
    
    public ThreadedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp,
        UpdateRequestProcessor next, UpdateRequestProcessorChain backingChain,
        ExecutorService executor, int bufferSize,
        int maxPipesNumber) {
      this(req, rsp, next, backingChain, executor, bufferSize, maxPipesNumber, oneSec);
    }
    
    public ThreadedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp,
        UpdateRequestProcessor next, UpdateRequestProcessorChain backingChain,
        ExecutorService executor, int bufferSize,
        int maxPipesNumber,long timeout) {
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
      offerTimeout = timeout;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      // I'm ready for 0 size buffer
      startPipes();
      checkPipes();
      final AddUpdateCommand copycat = (AddUpdateCommand) cmd.clone();
      boolean out=false;
      while(!out){
        try{
          out = buffer.offer(copycat,offerTimeout,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "interrupted while queueing "+ copycat,e);
        }
        if(!out){// buffer was full for long time, let's check that consumers are alive
          checkPipes();
          logger.debug("spining on offer");
        }
      }
    }
    
    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      stopPipes();
      delegate.processCommit(cmd);
    }
    
    /* 
     * TODO can be also async op
     */
    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      stopPipes();
      delegate.processDelete(cmd);
    }
    
    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      stopPipes();
      delegate.processMergeIndexes(cmd);
    }
    
    /* 
     * TODO can urgently halt queued tasks
     */
    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      stopPipes();
      delegate.processRollback(cmd);
    }
    
    /**
     * TODO consider steady ramp-up instead run all
     * */
    protected void startPipes() {
      if(pipes==null){
        
        pipes = new ArrayList<Future<Integer>>(pipesNumber);
        running = new CountDownLatch(pipesNumber);
        
        for(int i=0;i<pipesNumber;i++){
          final UpdateRequestProcessor backingProcessor =
              backingChain.createProcessor(req, rsp);
          if(backingProcessor instanceof ThreadedUpdateProcessor){
            throw new SolrException(ErrorCode.SERVER_ERROR, 
                "threaded update processors are nested into each other");
          } 
          pipes.add(
              executor.submit(new Pipe(backingProcessor, buffer, running))
          );
        }
      }
    }
    /**
     * check that all pipes are running,
     * if one of them are dead, we need to stop processing
     * */
    protected void checkPipes() throws IOException{
      if(pipes!=null && running.getCount()!=pipesNumber){
        throw new IOException("pipe halting detected. abort processing");
      }
    }

    @Override
    public void finish() throws IOException {
      try{
        stopPipes();
      }finally{
        delegate.finish();
      }
      assert buffer.isEmpty() : "but it has "+buffer; // we might need to wipe buffer with null
    }

    protected void stopPipes() throws IOException{
      if(pipes!=null){
        try{
          checkPipes();
          // if all consumers are alive, we can stop them by correct eof flow, otherwise
          // throw and cancel all pipes
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
            // it can contain remain eofs
            logger.warn("buffer contains {} after stopping pipes", dust);
          }
          if(problem!=null){
            throw new SolrException(ErrorCode.BAD_REQUEST, "rethrowing one of the pipes problem",problem);
          }
        }
        catch(Exception ie){// in this case aggressively cancel everything 
          List<Boolean> cancels = new ArrayList<Boolean>();
          for(Future<Integer> pipe:pipes){
            cancels.add(
              pipe.cancel(true)
            );
          }//rethrow???
          logger.info("pipes have been canceled "+cancels, ie);
          try {
            running.await();
            throw new IOException("wrap", ie);
          } catch (InterruptedException e) {
            throw new IOException("wrap", e);
          }
        }
        finally{
          pipes=null;
          running=null;
        }
      }
    }
    
  }
  
  @Override
  public void init(NamedList args) {
    buffer = (Integer) args.get("bufferSize");
    pipes = (Integer) args.get("pipesNumber");
    offerTimeout = args.get("offerTimeout")==null ? 1000: (Integer)args.get("offerTimeout");
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    UpdateRequestProcessorChain backing = getBackingChain(req);
    return 
      new SingleThreadWatchdogProcessor(
        new  ThreadedUpdateProcessor(req, rsp, next,
          backing,
          executor, buffer,
          pipes, offerTimeout)
        );
  }

  protected UpdateRequestProcessorChain getBackingChain(SolrQueryRequest req) {
    String backingName = req.getParams().get(backingChainParam);
    UpdateRequestProcessorChain backing = req.getCore().getUpdateProcessingChain(
        backingName
    );
    for(UpdateRequestProcessorFactory f : backing.getFactories()){
      if(f instanceof ThreadedUpdateProcessorFactory){
        throw new SolrException(ErrorCode.SERVER_ERROR, "backing chain "+backingName+
            " contains "+f);
      }
    }
    return backing;
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
