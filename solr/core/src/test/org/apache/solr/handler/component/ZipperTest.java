package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZipperTest extends LuceneTestCase{

  private static int resultIds[];
  private static List<Integer> partitions[];
  private Iterator[] partIters;
  private SolrZipper zipper;

  @BeforeClass
  public static void setup(){
    resultIds = new int[atLeast(100)];
    int id = random.nextInt(20)-10;
    for(int i=0; i<resultIds.length;i++){
      id += rarely() ? random.nextInt(5)+1 : 1;
    }
    
    partitions = new List[atLeast(3)];
    for(int p=0 ; p < partitions.length; p++){
      partitions[p] = new ArrayList<Integer>();
    }
    
    Integer singlePartitionMode = rarely() ? random.nextInt(partitions.length) : null;
    for(int i : resultIds){
      if(singlePartitionMode==null){
        // as result same i can be placed into the same partition twice or more
        // it's not really necessary for Zipper, but I don't think it's a problem
        // to support per partition dupe
        for(int d=0 ; d < random.nextInt(partitions.length/2)+1; d++){
          partitions[random.nextInt(partitions.length)].add(i);
        }
      }else{
        partitions[singlePartitionMode].add(i);
      }
    }
  }
  
  @Before
  public void prepareIters(){
    partIters = new Iterator[partitions.length];
    for(int i=0; i < partIters.length; i ++){
      partIters [i] = partitions[i].iterator() ;
    }
  }
    
  @Test
  public void testZipperSingleThread() {
    zipper = new SolrZipper(partitions.length, resultIds.length, "id");
    
    List<Map.Entry<Iterator<Integer>,StreamingResponseCallback>> streams
                    = new ArrayList<Map.Entry<Iterator<Integer>,StreamingResponseCallback>>(); 
    for(int i=0; i<partitions.length; i++){
      final StreamingResponseCallback inbound = zipper.addInboundCallback();
      final Iterator iterator = partIters[i];
      streams.add(new Map.Entry(){
        @Override
        public Object getKey() {
          return iterator;
        }
        @Override
        public Object getValue() {
          return inbound;
        }
        @Override
        public Object setValue(Object value) {
          return null;
        }
      });
    }
    
    for(int id: resultIds){
      // kick all partitions first
      
      Collections.shuffle(streams);
      for(Iterator<Map.Entry<Iterator<Integer>,StreamingResponseCallback>> tuples = streams.iterator();tuples.hasNext();){
        final Map.Entry<Iterator<Integer>,StreamingResponseCallback> tuple = tuples.next();
        if(tuple.getKey().hasNext()){
          final SolrDocument doc = new SolrDocument();
          doc.addField("id", tuple.getKey().next());
          tuple.getValue().streamSolrDocument(doc);
        }else{
          tuple.getValue().streamSolrDocument(null);
          tuples.remove();
        }
      }
    
      boolean callHasNext = usually();
      if(callHasNext){        
        assertTrue("eof but expect to see "+id, zipper.hasNext());
      }
      try{
        SolrDocument doc = zipper.next();
        assertEquals(id,doc.get("id"));
        
      }catch(NoSuchElementException ee){
        fail("eof but expect to see "+id + (callHasNext ? " btw, hasNext() just return true":""));
      }
    }
  }

  @After
  public void tollgate() {
    if(usually()){
      assertTrue("expect to see eof", zipper.hasNext());
    }
    try{
      SolrDocument doc = zipper.next();
      fail("expect to eof, but "+ doc);
    }catch(NoSuchElementException ee){
    }
  }
  
  @Test
  public void testZipperMultiThread() throws InterruptedException {
    zipper = new SolrZipper(partitions.length, 
        rarely() ? random.nextInt(5)+1 : random.nextInt(resultIds.length+10)+1,
            "id");
    
    ExecutorService exec = random.nextBoolean() ? 
        Executors.newCachedThreadPool() :
        Executors.newFixedThreadPool(random.nextInt(partitions.length)+partitions.length);
        
    List<Callable<Void>> searches = new ArrayList<Callable<Void>>();
    for(int i=0; i<partitions.length; i++){
      final StreamingResponseCallback inbound = zipper.addInboundCallback();
      final Iterator iter = partIters[i];
      final Callable<Void> c = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          while(iter.hasNext()){
            SolrDocument doc = new SolrDocument();
            doc.addField("id", iter.next());
            inbound.streamSolrDocument(doc);
            if(rarely()){
              Thread.sleep(random.nextInt(2), random.nextInt(10)*100);
            }
          }
          inbound.streamSolrDocument(null);
          return null;
        }
      };
      searches.add(c) ;
    }
    Collections.shuffle(searches);
    List<Future> futures = new ArrayList<Future>();
    for(Callable<Void> c : searches){
      final Future<Void> fu = exec.submit(c);
      futures.add(fu);
    }
    
    boolean skipHasNext = rarely();
    int i=0;
    while(skipHasNext || zipper.hasNext()){
      try{
        assertEquals(resultIds[i++], zipper.next());
      }catch(NoSuchElementException ee){
        assertTrue("hasNext() just return true", skipHasNext);
        break; // otherwise
      }
      skipHasNext = rarely();
    }
    assertEquals(resultIds.length-1, i);
    for(Future<Void> f : futures){
      assertTrue(f.isDone());
    }
    assertEquals(Collections.emptyList(), exec.shutdownNow());
  }
  
}
