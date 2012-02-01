package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * {!parent filter="PARENT:true"}CHILD_PRICE:10
 * */
public class BJQParserPlugin extends QParserPlugin {

    private String parentCacheName;

    @Override
    public QParser createParser(String qstr, SolrParams localParams,
            SolrParams params, SolrQueryRequest req) {
        
        final SolrCache parentCache = (parentCacheName==null) ? null 
                : req.getSearcher().getCache(parentCacheName);
        
        return new QParser(qstr, localParams, params, req) {
            
            @Override
            public Query parse() throws ParseException {
                String filter = localParams.get("filter");
                QParser parentParser = subQuery(filter, null);
                Query parentQ = parentParser.getQuery();
                Filter parentFilter = cachedParentFilter(req, parentQ);
                
                String queryText = localParams.get(QueryParsing.V);
                // there is no child query, return parent filter from cache
                if(queryText==null || "".equals(queryText)){
                    return new FilteredQuery(new MatchAllDocsQuery(),
                            parentFilter);
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
                   /* @Override
                    protected DocIdSet docIdSetToCache(DocIdSet docIdSet,
                            IndexReader reader) throws IOException {
                        DocIdSet result = super.docIdSetToCache(docIdSet, reader);
                        // it never return null but let's handle hypothetical side cases
                        if(!(result instanceof FixedBitSet)){
                            final FixedBitSet bits = new FixedBitSet(reader.maxDoc());
                            bits.or(result.iterator());
                            return bits;
                        }
                        return result;
                    }*/
                };
            }
        };
    }

    @Override
    public void init(NamedList args) {
        parentCacheName = (String) args.get("parent-filter-cache");
    }

}
