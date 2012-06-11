package org.apache.solr.update;

import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;

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

public class AddBlockCommand extends UpdateCommand implements Iterable<Iterable<? extends IndexableField>> {
  
  private Iterable<SolrInputDocument> solrDocs;
  public int commitWithin = -1;

  public AddBlockCommand(SolrQueryRequest req) {
    super(req);
  }

  @Override
  public String name() {
    return "addBlock";
  }

  public Iterable<SolrInputDocument> getSolrDocs() {
    return solrDocs;
  }

  @Override
  public Iterator<Iterable<? extends IndexableField>> iterator() {
    return new Iterator<Iterable<? extends IndexableField>>() {
      private Iterator<SolrInputDocument> in = solrDocs.iterator();
      
      @Override
      public boolean hasNext() {
        return in.hasNext();
      }

      @Override
      public Document next() {
        return DocumentBuilder.toDocument(in.next(), req.getSchema());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }
  
  public void setDocs(Iterable<SolrInputDocument> incoming){
    solrDocs = incoming;
  }

  @Override
  public String toString() {
    return "AddBlockCommand [solrDocs=" + solrDocs + ", flags=" + flags
        + ", version=" + version + "]";
  }
  
  
}
