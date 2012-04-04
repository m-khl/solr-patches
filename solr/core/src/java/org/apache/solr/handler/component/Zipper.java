package org.apache.solr.handler.component;

import java.util.Iterator;

import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;

/**
 * merges size inbound search results output them into the single one outbound
 * every inbound stream will be invoked in the own thread.
 * */
public class Zipper implements Iterator<SolrDocument> {
  
  public Zipper(int size){
    
  }
  
  Zipper(int size, int inboundBufferSize){
    
  }
  
  public StreamingResponseCallback addInbound(){
    return null;
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public SolrDocument next() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void remove() {
    // TODO Auto-generated method stub
    
  }
}
