package org.apache.solr.response;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.solr.request.SolrQueryRequest;

/**
 * refuse to do its work after everything is done. 
 * */
public class StreamingAwareBinaryResponseWriter extends BinaryResponseWriter {

  @Override
  public void write(OutputStream out, SolrQueryRequest req,
      SolrQueryResponse response) throws IOException {
    
    if(!req.getParams().getBool("response-streaming", false)){
      super.write(out, req, response);
    }
    
  }
}
