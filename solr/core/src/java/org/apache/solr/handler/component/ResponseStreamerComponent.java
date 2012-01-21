package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResponseStreamer;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.PostFilter;

public class ResponseStreamerComponent extends SearchComponent {

    private final class StreamingPostFilter extends MatchAllDocsQuery implements PostFilter{

        private static final int postFilterMagicNumber = 101;
        private final ResponseStreamer requestScope;
        private final SolrQueryRequest request;
        
        public StreamingPostFilter(ResponseStreamer requestScope, SolrQueryRequest req) {
            this.requestScope = requestScope;
            this.request = req;
        }

        @Override
        public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
            return new DelegatingCollector(){
                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return false;
                }
                @Override
                public void collect(int doc) throws IOException {
                    requestScope.collect(docBase + doc, request);
                    // delegate collects nothing super.collect(doc);
                }
                
            };
        }

        @Override
        public boolean getCache() {
            return false;
        }

        @Override
        public boolean getCacheSep() {
            return false;
        }

        @Override
        public int getCost() {
            return postFilterMagicNumber;
        }

        @Override
        public void setCache(boolean cache) {
        }

        @Override
        public void setCacheSep(boolean cacheSep) {
        }

        @Override
        public void setCost(int cost) {
        }
        
    }

    @Override
    public String getDescription() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSource() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSourceId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getVersion() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        SolrQueryRequest req = rb.req;
        if(req.getParams().getBool("response-streaming", false)){
            // add post filter
            List<Query> fqs = rb.getFilters();
            if(fqs==null){
                fqs = new ArrayList<Query>();
                rb.setFilters(fqs);
            }
            ResponseStreamer streamer = (ResponseStreamer) req.getCore().
                                        getQueryResponseWriter(req);
            
            ResponseStreamer requestStreamer = streamer.requestScope(req);
            
            fqs.add(new StreamingPostFilter(requestStreamer, req));
            
            HttpServletResponse response = (HttpServletResponse) req.getContext().get("response");
            response.setContentType(requestStreamer.getContentType());
        }
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
    }

}
