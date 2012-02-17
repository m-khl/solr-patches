package org.apache.solr.search;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Usage:
 * {!parent filter="PARENT:true"}CHILD_PRICE:10
 * 
 * To obtain just parent filter from the cache, omit child query:
 * {!parent filter="PARENT:true"}
 * 
 * This parser needs user cache to store parent filter as CachingWrapperFilter 
 * provide it's name via tag in solrconfig.xml:
 * 
 * <str name="parent-filter-cache">blockJoinParentFilterCache</str>
 * 
 *  no default value is provided for cache name. 
 * User cache with this name should be configured, otherwise query might not be performant 
 *  * 
 * */
public class BlockJoinParentQParserPlugin extends QParserPlugin {

    private static final class BlockJoinParentQParser extends QParser {
        private final SolrCache parentCache;

        private BlockJoinParentQParser(String qstr, SolrParams localParams,
                SolrParams params, SolrQueryRequest req, SolrCache parentCache) {
            super(qstr, localParams, params, req);
            this.parentCache = parentCache;
        }

        @Override
        public Query parse() throws ParseException {
            String filter = localParams.get("filter");
            QParser parentParser = subQuery(filter, null);
            Query parentQ = parentParser.getQuery();
            Filter parentFilter = cachedParentFilter(req, parentQ);
            
            String queryText = localParams.get(QueryParsing.V);
            // there is no child query, return parent filter from cache
            if(queryText==null || "".equals(queryText)){
                SolrConstantScoreQuery wrapped = new SolrConstantScoreQuery(parentFilter);
                wrapped.setCache(false);
                return wrapped;
            }
            
            QParser childrenParser = subQuery(queryText, null);
            
            return new ToParentBlockJoinQuery(childrenParser.getQuery(),
                    parentFilter
                    ,ToParentBlockJoinQuery.ScoreMode.None); // TODO support more scores
        }

        protected Filter cachedParentFilter(SolrQueryRequest req, Query parentQ) {
            // lazily retrieve from solr cache
            Filter filter = null;
            if(parentCache!=null){
                filter = (Filter) parentCache.get(parentQ);
            }
            Filter result;
            if(filter==null){
                result = createParentFilter(parentQ);
                if(parentCache!=null){
                    parentCache.put(parentQ, result);
                }
            } else {
                result = filter;
            }
            return result;
        }

        protected Filter createParentFilter(Query parentQ) {
            return new CachingWrapperFilter(new QueryWrapperFilter(parentQ) /*,? re-cache dels*/){
            };
        }
    }

    private String parentCacheName;

    @Override
    public QParser createParser(String qstr, SolrParams localParams,
            SolrParams params, SolrQueryRequest req) {
        
        final SolrCache parentCache = (parentCacheName==null) ? null 
                : req.getSearcher().getCache(parentCacheName);
        
        BlockJoinParentQParser parser = new BlockJoinParentQParser(qstr, localParams, params, req,
                parentCache);
        
        return parser;
    }

    @Override
    public void init(NamedList args) {
        
        if(args!=null){
            parentCacheName = (String) args.get("parent-filter-cache");
        }
    }

}
