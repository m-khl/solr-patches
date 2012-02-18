package org.apache.solr.search.join;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrCache;

public class BlockJoinChildQParser extends  BlockJoinParentQParser {

  public BlockJoinChildQParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req, SolrCache parentCache) {
    super(qstr, localParams, params, req, parentCache);
    // TODO Auto-generated constructor stub
  }
  
}
