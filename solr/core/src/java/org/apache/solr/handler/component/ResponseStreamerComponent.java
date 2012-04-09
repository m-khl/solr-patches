package org.apache.solr.handler.component;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.PostFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResponseStreamerComponent extends SearchComponent {

    private static final Logger logger = LoggerFactory.getLogger(ResponseStreamerComponent.class);

    private final class StreamingPostFilter extends MatchAllDocsQuery implements PostFilter{

        private static final int postFilterMagicNumber = 101;
        private final Codec codec;
        private final SolrQueryRequest request;
        
        public StreamingPostFilter(Codec codec, SolrQueryRequest req) {
            this.codec = codec;
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
                    codec.marshalDocNum(docBase + doc);
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
        if(req.getParams().getBool("response-streaming", false) && req.getParams().get("shards")==null){
            // add post filter
            List<Query> fqs = rb.getFilters();
            if(fqs==null){
                fqs = new ArrayList<Query>();
                rb.setFilters(fqs);
            }
            Codec codec = new Codec(rb.req, rb.rsp){
              @Override
              public void writeIterator(Iterator iter) throws IOException {
                // fair enough, body will be written later
                writeTag(ITERATOR);
              }
            };
            FastOutputStream wrap = FastOutputStream.wrap(
                  (OutputStream) rb.req.getContext().get("outputStream"));
            codec.init(wrap);
            
            HttpServletResponse response = (HttpServletResponse) rb.req.getContext().get("response");
            response.setContentType(rb.req.getCore().getQueryResponseWriter(rb.req).getContentType(rb.req, null));
            
            SolrQueryResponse preamble = new SolrQueryResponse();
            preamble.add("response", Collections.emptyList().iterator());
            
            codec.marshal(preamble.getValues(), wrap);
            
            req.getContext().put("streaming-codec", codec);
            fqs.add(new StreamingPostFilter(codec, req));
            
            
        }
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
      if(rb.req.getParams().getBool("response-streaming", false)){
      // write end_obj 
        ((Codec) rb.req.getContext().get("streaming-codec")).marshalEnd();
      }
    }

    static class Codec extends JavaBinCodec{
      
      private final SolrQueryRequest req; 
      private final SolrQueryResponse resp;
      
      public Codec(final SolrQueryRequest req, SolrQueryResponse resp) {
          super(new BinaryResponseWriter.Resolver(req, resp.getReturnFields()){
            {
              schema = req.getSchema();
              searcher = req.getSearcher();
            }
          });
          this.req = req;
          this.resp = resp;
      }
      
      public void marshalEnd() throws IOException{
        writeVal(END_OBJ);
        daos.flushBuffer();
      }
      
      public void marshalDocNum(int id) throws IOException{
          Document doc = req.getSearcher().doc(id, resp.getReturnFields().getLuceneFieldNames());
          SolrDocument sdoc = ((BinaryResponseWriter.Resolver) resolver).getDoc(doc);
          writeSolrDocument(sdoc);
      }
    }
    
    @Override
    public void finishStage(ResponseBuilder rb) {
      final NamedList nl = rb.rsp.getValues();
      for(int e = 0; e< nl.size(); e++){
        if(nl.getName(e).equals("response") && nl.getVal(e) instanceof SolrDocumentList && ((SolrDocumentList) nl.getVal(e)).size()==0){
          logger.warn("wiping :{} remain list is are:{}",nl.remove(e), nl);
          ;
          
        }
      }
    }
    
    /**
     * zipper becomes response and available for streaming by javabean codec
     * */
    @Override
    public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
      final Iterator<SolrDocument> zipper = (Iterator<SolrDocument>) sreq.responses.get(0).getSolrResponse().getResponse().get("zipper");
      rb.rsp.add("response", new Iterator<SolrDocument>(){
        @Override
        public boolean hasNext() {
          return zipper.hasNext();
        }

        @Override
        public SolrDocument next() {
          final SolrDocument next = zipper.next();
          logger.info("streaming " + next);
          return next;
        }

        @Override
        public void remove() {
          zipper.remove();
        }
        
      } );
      rb.resultIds = Collections.emptyMap();
      rb._responseDocs = new SolrDocumentList();
    }
}
