package org.apache.solr.handler.component;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HttpShardHandler.SimpleSolrResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

public class StreamingShardHandlerFactory extends HttpShardHandlerFactory implements SolrCoreAware {

  private static final class Merger extends SolrZipper {
    ShardRequest shardsRequest;
    protected Throwable exception;
    
    private Merger(int size, int buffSize, String pkFieldName, ShardRequest sreq) {
      super(size, buffSize, pkFieldName);
      shardsRequest = sreq;
    }
  }

  protected String uniqKeyFieldName ; 
  
  @Override
  public ShardHandler getShardHandler() {
    return new HttpShardHandler(this){
      // on demand creation
      private Merger merger = null;
      
      @Override
      public void submit(final ShardRequest sreq, final String shard,
          final ModifiableSolrParams params) {
        
        if(merger==null){
          merger = new Merger(sreq.actualShards.length, 10, uniqKeyFieldName, sreq);
        }
          
        // do this outside of the callable for thread safety reasons
        final List<String> urls = getURLs(shard);
        
        final StreamingResponseCallback callback = merger.addInboundCallback();
        Callable<ShardResponse> task = new Callable<ShardResponse>() {
          public ShardResponse call() throws Exception {
        
            ShardResponse srsp = new ShardResponse();
            srsp.setShardRequest(sreq);
            srsp.setShard(shard);
            SimpleSolrResponse ssr = new SimpleSolrResponse();
            srsp.setSolrResponse(ssr);
            long startTime = System.currentTimeMillis();
        
            try {
              params.remove(CommonParams.WT); // use default (currently javabin)
              params.remove(CommonParams.VERSION);
        
              // SolrRequest req = new QueryRequest(SolrRequest.METHOD.POST, "/select");
              // use generic request to avoid extra processing of queries
              QueryRequest req = new QueryRequest(params);
              req.setMethod(SolrRequest.METHOD.POST);
        
              // no need to set the response parser as binary is the default
              // req.setResponseParser(new BinaryResponseParser());
        
              // if there are no shards available for a slice, urls.size()==0
              if (urls.size()==0) {
                // TODO: what's the right error code here? We should use the same thing when
                // all of the servers for a shard are down.
                throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "no servers hosting shard: " + shard);
              }

              /**
    ResponseParser parser = new StreamingBinaryResponseParser( callback );
    QueryRequest req = new QueryRequest( params );
               */
              req.setStreamingResponseCallback( callback );
              req.setResponseParser( new StreamingBinaryResponseParser( callback ) );    
        
              if (urls.size() <= 1) {
                String url = urls.get(0);
                srsp.setShardAddress(url);
                SolrServer server = new CommonsHttpSolrServer(url, httpShardHandlerFactory.client);
                ssr.nl = server.request(req);
              } else {
//                final IllegalStateException e = new IllegalStateException("I'm not sure about load balancing and streaming");
//                log.error("not ready",e);
//                throw e;
                LBHttpSolrServer.Rsp rsp = httpShardHandlerFactory.loadbalancer.request(new LBHttpSolrServer.Req(req, urls));
                ssr.nl = rsp.getResponse();
                srsp.setShardAddress(rsp.getServer());
              }
            } catch (Throwable th) {
              srsp.setException(th);
              if (th instanceof SolrException) {
                srsp.setResponseCode(((SolrException)th).code());
              } else {
                srsp.setResponseCode(-1);
              }
            }
            finally{
              callback.streamSolrDocument(null);
            }
        
            ssr.elapsedTime = System.currentTimeMillis() - startTime;
        
            return srsp;
          }
        };
        
        pending.add( completionService.submit(task) );
      }
      
      @Override
      ShardResponse take() {
        throw new UnsupportedOperationException();
      }
      
      @Override
      public ShardResponse takeCompletedOrError() {
        if(merger == null){
          return null;
        }
        ShardResponse result = new ShardResponse();
        result.setShardRequest(merger.shardsRequest);
        
        merger.shardsRequest.responses.add(result);
        
        final SimpleSolrResponse rsp = new SimpleSolrResponse();
        result.setSolrResponse(rsp);
        rsp.setResponse(new NamedList<Object>());
        rsp.getResponse().add("zipper", merger);
        
        merger = null;
        return result;
      }
    };
  }

  @Override
  public void inform(SolrCore core) {
    uniqKeyFieldName = core.getSchema().getUniqueKeyField().getName();
  }
}
