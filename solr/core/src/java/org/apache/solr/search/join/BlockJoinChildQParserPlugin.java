package org.apache.solr.search.join;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrCache;

public class BlockJoinChildQParserPlugin extends BlockJoinParentQParserPlugin {

  @Override
  protected QParser createBJQParser(String qstr,
      SolrParams localParams, SolrParams params, SolrQueryRequest req,
      SolrCache parentCache) {
    return  new BlockJoinChildQParser(qstr, localParams, params, req,
        parentCache);
  }
}
