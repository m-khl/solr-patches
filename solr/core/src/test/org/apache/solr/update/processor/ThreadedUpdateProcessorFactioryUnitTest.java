package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
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
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.ThreadedUpdateProcessorFactiory.ThreadedUpdateProcessor.Pipe;
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
      addUpdateCommands.add(cmd);
      logger.trace("added {}", cmd);
      super.processAdd(cmd);
    }
    
    @Override
    public void finish() throws IOException {
      finishes.incrementAndGet();
      logger.trace("completed {}", finishes);
    }
  }
  

  ExecutorService executor = random().nextBoolean() 
      ? Executors.newSingleThreadExecutor() 
          : Executors.newFixedThreadPool(random().nextBoolean() ? 2:3); 
  
  @Test
  public void testConsuming() throws InterruptedException, ExecutionException{
    BufferUpdateRequestProcessor proc = new BufferUpdateRequestProcessor(null);
    
    BlockingQueue<AddUpdateCommand> queue = new LinkedBlockingQueue<AddUpdateCommand>();
    
    Future<Integer> pipe = executor.submit(new Pipe(proc, queue));
    
    try {
      pipe.get(1, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
    }
    queue.add(new AddUpdateCommand(null){ @Override
    public String toString() {
      return "expected1";
    }});
    queue.add(new AddUpdateCommand(null){ @Override
      public String toString() {
        return "expected2";
      }});
    
    assertTrue((""+ proc.addUpdateCommands.take()).startsWith("expected"));
    assertTrue((""+ proc.addUpdateCommands.take()).startsWith("expected"));
    assertEquals(0, proc.finishes.intValue());
    
    if(random().nextBoolean()){
      logger.debug("eoffing");
      queue.add(ThreadedUpdateProcessorFactiory.eof);
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
      while(proc.finishes.intValue()!=1){
        Thread.yield();
      }
    }
    
    
  }
  
}
