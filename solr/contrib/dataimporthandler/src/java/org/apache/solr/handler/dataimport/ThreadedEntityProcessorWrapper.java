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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.EntityProcessorBase.ON_ERROR;
import static org.apache.solr.handler.dataimport.EntityProcessorBase.ABORT;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each Entity may have only a single EntityProcessor .  But the same entity can be run by
 * multiple EntityProcessorWrapper (1 per thread) . this helps running transformations in multiple threads
 * @since Solr 3.1
 */

public class ThreadedEntityProcessorWrapper extends EntityProcessorWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadedEntityProcessorWrapper.class);

  final DocBuilder.EntityRunner entityRunner;
  /** single EntityRunner per children entity */
  final Map<DataConfig.Entity ,DocBuilder.EntityRunner> children;
  
  final protected AtomicBoolean entityEnded = new AtomicBoolean(false);

   final private int number;

  public ThreadedEntityProcessorWrapper(EntityProcessor delegate, DocBuilder docBuilder,
                                  DocBuilder.EntityRunner entityRunner,
                                  VariableResolverImpl resolver,
                                  Map<DataConfig.Entity ,DocBuilder.EntityRunner> childrenRunners,
                                  int num) {
    super(delegate, docBuilder);
    this.entityRunner = entityRunner;
    this.resolver = resolver;
    this.children = childrenRunners;
    this.number = num;
  }

  void threadedInit(Context context){
    rowcache = null;
    this.context = context;
    resolver = (VariableResolverImpl) context.getVariableResolver();
    //context has to be set correctly . keep the copy of the old one so that it can be restored in destroy
    if (entityName == null) {
      onError = resolver.replaceTokens(context.getEntityAttribute(ON_ERROR));
      if (onError == null) onError = ABORT;
      entityName = context.getEntityAttribute(DataConfig.NAME);
    }    
  }

  /**
   * for root entity it retrieves single row, transforms it, 
   *    and loop until transfomer passes the first row
   *    
   * for child entities whole page is pulled. where the page is non-null children entity rows.
   * then the whole page is transformed and emitted to a {@link rowcache}
   * 
   * the rationale is avoid stealing child rows by parent entity threads. For every parent row 
   * the linked children rows (page) is pulled under lock obtained on {@link delegate} 
   *  
   * */
  @Override
  public Map<String, Object> nextRow() {
    if (rowcache != null) {
      return getFromRowCache();
    }
    
    List<Map<String, Object>> transformedRows = new ArrayList<Map<String,Object>>();
    boolean eof = false;
    while (transformedRows.isEmpty() && !eof) { // looping while transformer bans raw rows
        List<Map<String, Object>> rawRows = new ArrayList<Map<String, Object>>();
      synchronized (delegate) {
          Map<String, Object> arow = null;
          // for paginated case we need to loop through whole page, other wise single row is enough
          boolean retrieveWholePage = !entityRunner.entity.isDocRoot;
          for (int i = 0; 
              retrieveWholePage ? !eof : i==0; // otherwise only single row
                  i++) {
              arow = pullRow();
              if (arow != null) {
                  rawRows.add(arow);
              }else { // there is no row, eof
                  eof = true;
              }
          }
      }
      for(Map<String, Object> rawRow : rawRows){
          // transforming emits N rows
          List<Map<String, Object>> result = transformRow(rawRow);
       // but post-transforming is applied only to the first one (legacy as-is)
          if(!result.isEmpty() && result.get(0) != null){ 
              delegate.postTransform(result.get(0));
              transformedRows.addAll(result);
          }
      }
    }
    if(!transformedRows.isEmpty()){
        rowcache = transformedRows;
        return getFromRowCache();
    }else{ // caused by eof
        return null;
    }
  }

  /**
   * pulls single row from {@link delegate}, checks and sets {@link entityRunner.entityEnded}.
   * it expect to be called in synchronised(delegate) section
   * @return row from delegate
   * */
  protected Map<String, Object> pullRow() {
      Map<String, Object> arow = null;
    if(entityEnded.get()){
        return null;
    }
    try {
      arow = delegate.nextRow();
    } catch (Exception e) {
      if (ABORT.equals(onError)) {
        wrapAndThrow(SEVERE, e);
      } else {
        //SKIP is not really possible. If this calls the nextRow() again the Entityprocessor would be in an inconistent state
        LOG.error("Exception in entity : " + entityName, e);
        return null;
      }
    }
    LOG.debug("arow : {}", arow);
    if(arow == null){
        entityEnded.set(true);
    }
    return arow;
  }

  public void init(DocBuilder.EntityRow rows) {
    for (DocBuilder.EntityRow row = rows; row != null; row = row.tail) resolver.addNamespace(row.name, row.row);
  }

  public int getNumber() {
    return number;
  }


 
}
