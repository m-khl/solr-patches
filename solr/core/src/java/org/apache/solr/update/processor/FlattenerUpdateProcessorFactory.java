package org.apache.solr.update.processor;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddBlockCommand;
import org.apache.solr.update.AddUpdateCommand;

/**
 * transforms the given solr input document which includes nested docs 
 * into the block searchable by BJQ
 * 
 * nested docs should be places as collections of SolrInputDocuments 
 * */
public class FlattenerUpdateProcessorFactory extends
    UpdateRequestProcessorFactory {

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new FlattenerUpdateProcessor(req,rsp, next);
  }
  
}


class FlattenerUpdateProcessor extends UpdateRequestProcessor {

  public FlattenerUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    final SolrInputDocument sidoc = cmd.getSolrInputDocument();
    List<SolrInputDocument> block = new ArrayList<SolrInputDocument>();
    unwrapNestedDocs(sidoc, block);
    if(block.size()==1){
      super.processAdd(cmd);
    }else{
      Collections.reverse(block);
      final AddBlockCommand blockCmd = new AddBlockCommand(cmd.getReq());
      blockCmd.setDocs(block);
      super.processAddBlock(blockCmd);
    }
  }

  protected void unwrapNestedDocs(final SolrInputDocument sidoc,
      Collection<SolrInputDocument> block) {
    block.add(sidoc);
    for(final Iterator<Entry<String,SolrInputField>>
       fieldsIter = sidoc.entrySet().iterator();
       fieldsIter.hasNext();){
      final Entry<String,SolrInputField> tuple = fieldsIter.next();
      
      if(tuple.getValue().getValue() instanceof Collection){
        Collection<?> vals = (Collection)tuple.getValue().getValue();
        boolean first = true;
        boolean nestedDocs = false;
        for(Iterator<?> iter = vals.iterator(); iter.hasNext() & (first || nestedDocs);){
          final Object val = iter.next();
          if(first){
            first = false;
            nestedDocs = val instanceof SolrInputDocument;
            if(nestedDocs){
              fieldsIter.remove();
            }
          }
          if(nestedDocs){
            unwrapNestedDocs((SolrInputDocument)val,block
                //, tuple.getKey() - we can supply name of relation, but what for?
                );
          }
        }
      }
      
    }
  }
}
