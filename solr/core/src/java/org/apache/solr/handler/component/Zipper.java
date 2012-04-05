package org.apache.solr.handler.component;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * merges size inbound search results output them into the single one outbound
 * every inbound stream will be invoked in the own thread.
 * */
public class Zipper<T> implements Iterator<T> {
  
  private final BlockingQueue<T> buffers[];
  private final NavigableMap<T,BlockingQueue<T>> heap;

  private T next = null;
  
  private int inboundNum = 0;
  private int outboundNum = 0;
  
  private static Object eof = new Object(){
    public String toString() { return "eof";}
    private Object readResolve() { return eof; }
  };
  
  public Zipper(int size, Comparator<T> cmp){
    this(size, 10, cmp);
  }
  
  Zipper(int size, int inboundBufferSize, Comparator<T> cmp){
    buffers = new BlockingQueue[size];
    for(int i=0; i<buffers.length; i++){
      buffers[i] = new LinkedBlockingQueue<T>(inboundBufferSize);
    }
    heap = new TreeMap<T,BlockingQueue<T>>(cmp);
  }
  
  public interface Inbound<T>{
    void onElement(T elem) ;
    void eof() ;
  }
  
  public Inbound<T> addInbound(){
    final BlockingQueue<T> buffer = buffers[inboundNum++];
    return new Inbound<T>() {
      @Override
      public void onElement(T elem)  {
          try {
            buffer.put(elem);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
      }
      
      @Override
      public void eof() {
          try {
            buffer.put((T) eof);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
      }
    };
  }

  @Override
  public boolean hasNext() {
    if(next == null){
        next = pullNext();
    }
    assert next!=null;
    return next!=eof;
  }

  @Override
  public T next() {
    if(next == null){
      next = pullNext();
    }
    assert next!=null;
    if(next==eof){
      throw new NoSuchElementException();
    }
    T t = next;
    next = null;
    return t;
  }

  private T pullNext() {
    // come through buffers open every one, first time only 
    for(;outboundNum<buffers.length;outboundNum++){
      final BlockingQueue<T> buff = buffers[outboundNum];
      pullIntoHeap(buff);
    }
    // then pick the top
    final Iterator<Entry<T,BlockingQueue<T>>> top = heap.entrySet().iterator();
    
    if(!top.hasNext()){
      return (T) eof;
    }
    
    final Entry<T,BlockingQueue<T>> entry = top.next();
    top.remove();
    
    pullIntoHeap(entry.getValue());
    // top iter is invalid
    return entry.getKey();
  }

  private void pullIntoHeap(final BlockingQueue<T> buff) {
    T head;
    
    try {
      head = buff.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    
    if(head!=eof){
      heap.put(head, buff);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Zipper.remove()");
  }
}
