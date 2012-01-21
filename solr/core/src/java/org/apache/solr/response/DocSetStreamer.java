package org.apache.solr.response;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocSetStreamer implements ResponseStreamer, SolrCoreAware {

    public static final String outputStream = "outputStream";
    private static final Logger log = LoggerFactory.getLogger(DocSetStreamer.class);
    protected Set<String> fields = null;
    
    public DocSetStreamer() {
    }
    
    @Override
    public void collect(int docNum, SolrQueryRequest req) throws IOException {
        // obtain lucene doc 
        final Set<String> fl = getFields(req);
        Document doc = req.getSearcher().doc(docNum, fl);
        // obtain output stream
        OutputStream os = getStream(req);
        
        int i = 0; 
        for(String field : fl){
            if(i>0){
                os.write(',');
            }
            if(dumpField(doc, os, field))
                i++;
        }
        // os.write('\n');
    }

    protected OutputStream getStream(SolrQueryRequest req) {
        return getOutputStream(req);
    }

    public static OutputStream getOutputStream(SolrQueryRequest req) {
        return (OutputStream) req.getContext().get(outputStream);
    }

    protected Set<String> getFields(SolrQueryRequest req) {
        return fields;
    }

    protected boolean dumpField(Document doc, OutputStream os, String field)
            throws IOException {
//        BytesRef binaryValue = doc.getBinaryValue(field);
//        if(binaryValue!=null){
        int i = ((NumericField)doc.getField(field)).numericValue().intValue();
        os.write((byte)(i >> 24));
        os.write((byte)(i >> 16));
        os.write((byte)(i >>  8));
        os.write((byte) i);
            return true;
    //    }else{
      //      return false;
        //}
    }

    @Override
    public void write(OutputStream out, SolrQueryRequest request,
            SolrQueryResponse response) throws IOException {
//        out.write(this.toString().getBytes());
//        out.write(" doesn't support write() ops".getBytes());

    }

    /** @return null to bypass Solr filter */
    @Override
    public String getContentType(SolrQueryRequest request,
            SolrQueryResponse response) {
        return null;
    }

    @Override
    public String getContentType() {
        return CONTENT_TYPE_TEXT_ASCII;
    }
    
    @Override
    public void init(NamedList args) {
        String fl = (String) args.get("field");
        if(fl!=null && !"".equals(fl)){
            fields = Collections.singleton(fl); 
        }
    }

    @Override
    public void write(Writer writer, SolrQueryRequest request,
            SolrQueryResponse response) throws IOException {
        writer.append(this.toString());
        writer.append(" doesn't support write() ops");
    }

    @Override
    public ResponseStreamer requestScope(final SolrQueryRequest req) {
        return new DocSetStreamer(){
            final OutputStream stream = super.getStream(req); 
            
            {
                this.fields = DocSetStreamer.this.fields;
            }
            @Override
            protected OutputStream getStream(SolrQueryRequest req) {
                return stream;
            }
        };
    }

    @Override
    public void inform(SolrCore core) {
        if(fields==null){
            fields = Collections.singleton(core.getSchema().getUniqueKeyField().getName());
        }
        log.info("dump {} field",fields);
    }



}
