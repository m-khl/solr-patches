package org.apache.solr.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;

public class ResponseStreamingRequestParsers extends SolrRequestParsers {

    public ResponseStreamingRequestParsers(Config globalConfig) {
        super(globalConfig);
    }

    @Override
    public SolrQueryRequest parse(SolrCore core, String path,
            HttpServletRequest req, HttpServletResponse resp) throws Exception {
        SolrQueryRequest parsedResult = super.parse(core, path, req, resp);
        
        parsedResult.getContext().put("response", resp);
        parsedResult.getContext().put("outputStream", resp.getOutputStream());
        //parsedResult.getContext().put("outputWriter", resp.getWriter());
        
        return parsedResult;
    }
}
