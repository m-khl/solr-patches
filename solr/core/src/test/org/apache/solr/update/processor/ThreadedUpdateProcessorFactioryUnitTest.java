package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.ThreadedUpdateProcessorFactory.ThreadedUpdateProcessor;
import org.apache.solr.update.processor.ThreadedUpdateProcessorFactory.ThreadedUpdateProcessor.Pipe;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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

public class ThreadedUpdateProcessorFactioryUnitTest extends LuceneTestCase {
  
  private static final Logger logger = LoggerFactory.getLogger(ThreadedUpdateProcessorFactioryUnitTest.class);

  private final class BufferUpdateRequestProcessor extends
      UpdateRequestProcessor {
    
    final BlockingQueue<AddUpdateCommand> addUpdateCommands 
          = rarely()? new SynchronousQueue<AddUpdateCommand>(): 
            new LinkedBlockingQueue<AddUpdateCommand>(atLeast(1));
    final AtomicInteger finishes = new AtomicInteger();
    
    private BufferUpdateRequestProcessor(UpdateRequestProcessor next) {
      super(next);
    }
    
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      try {
        addUpdateCommands.put(cmd);
      } catch (InterruptedException e) {
        throw new IOException("wrap", e);
      }
      finally{
        logger.trace("added {}", cmd);
        super.processAdd(cmd);
      }
    }
    
    @Override
    public void finish() throws IOException {
      finishes.incrementAndGet();
      logger.trace("completed {}", finishes);
    }
  }
  

  static ExecutorService executor; 
  
  static ExecutorService cachedExecutor;    
      
  @BeforeClass
  public static void raise(){
    executor = random().nextBoolean() 
        ? Executors.newSingleThreadExecutor() 
            : Executors.newFixedThreadPool(random().nextBoolean() ? 2:3); 
    
    cachedExecutor = Executors.newCachedThreadPool();    
  }
  
  @AfterClass
  public static void shut(){
    cachedExecutor.shutdownNow();
    executor.shutdownNow();
  }
  
  @Test(timeout=1000)
  public void testConsuming() throws InterruptedException, ExecutionException{
    BufferUpdateRequestProcessor proc = new BufferUpdateRequestProcessor(null);
    
    BlockingQueue<AddUpdateCommand> queue = new LinkedBlockingQueue<AddUpdateCommand>();
    
    CountDownLatch latch = new CountDownLatch(1);
    Future<Integer> pipe = executor.submit(new Pipe( proc , queue, latch));
    
    try {
      pipe.get(1, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    queue.add(new AddUpdateCommand(null){ @Override
    public String toString() {
      return "expected1";
    }
    public String getPrintableId() {return toString();};
    }
    );
    queue.add(new AddUpdateCommand(null){ @Override
      public String toString() {
        return "expected2";
      }
    public String getPrintableId() {return toString();};
    });
    
    assertTrue((""+ proc.addUpdateCommands.take()).startsWith("expected"));
    assertTrue((""+ proc.addUpdateCommands.take()).startsWith("expected"));
    assertEquals(0, proc.finishes.intValue());
    
    if(random().nextBoolean()){
      logger.debug("eoffing");
      queue.add(ThreadedUpdateProcessorFactory.eof);
      assertEquals(2, (int)pipe.get());
      assertEquals(1, proc.finishes.intValue());
    }else{
      logger.debug("cancelling");
      pipe.cancel(true);
      try{
        pipe.get();
        fail();
      }catch(CancellationException e){
      }
      latch.await();
      assertEquals(1, proc.finishes.intValue());
    }
  }
  /**
   * I wanna put some stuff onto two pipes 
   * let them hand on long running underlying processor 
   * and then interrupt the initial thread
   * I expect that pipes will be stopped with calling finish afterwards
   * @throws IOException 
   * @throws InterruptedException 
   * */
  @Test(timeout=1000)
  public void testInterrupt() throws IOException, InterruptedException {
    
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new HashMap<String,String[]>());
    
    final int pipes = 2;
    CountDownLatch latch = new CountDownLatch(pipes);
    HangOnProcesssorFactory hangOnProcesssorFactory = new HangOnProcesssorFactory(latch);
    final ThreadedUpdateProcessor proc = new ThreadedUpdateProcessor(req, null,
        null, new UpdateRequestProcessorChain(new UpdateRequestProcessorFactory[] {
            new LogUpdateProcessorFactory(), hangOnProcesssorFactory}, null), cachedExecutor, 1,
        pipes);
    
    Future<?> reqThread = cachedExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          for(int i = 0 ; i<pipes; i++){
            final int id = i;
            proc.processAdd(new AddUpdateCommand(req){public String toString() {return "cmd#"+id;};});
          }
        } catch (IOException e) {
          logger.debug("thrown from adds", e);
          throw new RuntimeException(e);
        }finally{
          try {
            proc.finish();
          } catch (IOException e) {
            logger.debug("thrown from finish", e);
            throw new RuntimeException(e);
          }
        }
      }
    });
    latch.await();
    // let's take all running pipes, will they be visible?
    ArrayList<Future<Integer>> running = new ArrayList<Future<Integer>>(proc.pipes);
    assertEquals(pipes, running.size());
    for(Future<Integer> f: running){
      assertFalse(f.isDone());
    }
    // ok everybody there
    reqThread.cancel(true);
    // than spin wait
    boolean complete=false;
    while(!complete){
      complete = true;
      for (org.apache.solr.update.processor.ThreadedUpdateProcessorFactioryUnitTest.HangOnProcesssorFactory.HangOnProcessor 
          whatTheHell : hangOnProcesssorFactory.processors) {
        if(!whatTheHell.finished){
          complete = false;
          Thread.sleep(1);
          break;
        }
      }
    }
    
    for(Future<Integer> f: running){
      assertTrue(f.isDone());
      assertTrue(f.isCancelled());
    }
    assertTrue(reqThread.isCancelled());
    assertTrue(reqThread.isDone());
  }

  private static class HangOnProcesssorFactory extends UpdateRequestProcessorFactory{

    protected static final class HangOnProcessor extends UpdateRequestProcessor {
      volatile boolean finished = false;
      final CountDownLatch latch;
      
      protected HangOnProcessor(UpdateRequestProcessor next, CountDownLatch latch) {
        super(next);
        this.latch = latch;
      }
      
      @Override
      public void processAdd(AddUpdateCommand cmd) throws IOException {
        latch.countDown();
        try {
          Thread.sleep(10000000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        super.processAdd(cmd);
      }
      
      @Override
      public void finish() throws IOException {
        assert !finished;
        finished = true;
        super.finish();
      }
    }
    
    final List<HangOnProcessor> processors = new ArrayList<HangOnProcessor>();
    final CountDownLatch latch;
    
    public HangOnProcesssorFactory(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req,
        SolrQueryResponse rsp, UpdateRequestProcessor next) {
      HangOnProcessor proc = new HangOnProcessor(next, latch);
      processors.add(proc);
      return proc;
    }
    
  }
  
}
