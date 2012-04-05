package org.apache.solr.handler.component;

import java.util.Comparator;

import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;

public class SolrZipper extends Zipper<SolrDocument> {

  public SolrZipper(int size, int buffSize,  final String pkFieldName) {
    super(size, buffSize, new Comparator<SolrDocument>() {
      @Override
      public int compare(SolrDocument o1, SolrDocument o2) {
        return ((Comparable) o1.getFieldValue(pkFieldName)).compareTo(o2.getFieldValue(pkFieldName));
      }
    });
  }
  
  @Override
  public Inbound<SolrDocument> addInbound() {
    throw new UnsupportedOperationException("not avalable, use addInboundCallback() instead pls ");
  }
  
  public StreamingResponseCallback addInboundCallback(){
    final Inbound<SolrDocument> sink = super.addInbound();
    return new StreamingResponseCallback() {
      
      @Override
      public void streamSolrDocument(SolrDocument doc) {
        if(doc==null){
          sink.eof();
        }else
          sink.onElement(doc);
      }
      
      @Override
      public void streamDocListInfo(long numFound, long start, Float maxScore) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
