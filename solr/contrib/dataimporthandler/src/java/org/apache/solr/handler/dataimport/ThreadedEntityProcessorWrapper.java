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

import org.apache.solr.handler.dataimport.DataConfig.Entity;
import org.apache.solr.handler.dataimport.DocBuilder.EntityRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each Entity may have only a single EntityProcessor .  But the same entity can be run by
 * multiple EntityProcessorWrapper (1 per thread) . this helps running transformations in multiple threads
 * @since Solr 3.1
 */

public abstract class ThreadedEntityProcessorWrapper extends EntityProcessorWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(ThreadedEntityProcessorWrapper.class);

  final DocBuilder.EntityRunner entityRunner;
  /** single EntityRunner per children entity */
  final Map<DataConfig.Entity ,DocBuilder.EntityRunner> children;
  
  final protected AtomicBoolean entityEnded = new AtomicBoolean(false);

  final private int number;
  
  protected ThreadedEntityProcessorWrapper(EntityProcessor delegate, DocBuilder docBuilder,
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

  public static ThreadedEntityProcessorWrapper create(EntityProcessor delegate, DocBuilder docBuilder,
                                                          DocBuilder.EntityRunner entityRunner,
                                                          VariableResolverImpl resolver,
                                                          Map<DataConfig.Entity ,DocBuilder.EntityRunner> childrenRunners,
                                                          int num){
      boolean root = entityRunner.entity.isDocRoot;
      if(!root){ // child entities 
          return new ChildEntityProcessorWrapper(delegate, docBuilder, entityRunner, resolver,
                childrenRunners, num);
      } else {
          if(num == 0) { // use DocBuilder's writer
              return new FirstRootEntityProcessorWrapper(delegate, docBuilder, entityRunner,
                    resolver, childrenRunners, num);
          }else { // allocate writer per this thread
              return new RootEntityProcessorWrapper(delegate, docBuilder, entityRunner, resolver,
                    childrenRunners, num);
          }
      }
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
          // for paged case we need to loop through whole page, other wise single row is enough
              for (int i = 0; delegate.isPaged() ? !eof : i==0; i++) {
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
          if(!result.isEmpty() && result.get(0) != null){ // but post-transforming is applied only to the first one (legacy as-is)
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
    if(entityEnded.get()) return null;
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("arow : " + arow);
    }
    if(arow == null) entityEnded.set(true);
    return arow;
  }

  public void init(DocBuilder.EntityRow rows) {
    for (DocBuilder.EntityRow row = rows; row != null; row = row.tail) resolver.addNamespace(row.name, row.row);
  }

  public int getNumber() {
    return number;
  }

  @Override
  public abstract void destroy() ;
  
  public abstract DIHWriter getWriter();
  
  
  /** allocates and destroys own writer to the single thread */
  private static final class RootEntityProcessorWrapper extends
        ThreadedEntityProcessorWrapper {
    
    private DIHWriter writer;

    private RootEntityProcessorWrapper(EntityProcessor delegate,
            DocBuilder docBuilder, EntityRunner entityRunner,
            VariableResolverImpl resolver,
            Map<Entity, EntityRunner> childrenRunners, int num) {
        super(delegate, docBuilder, entityRunner, resolver,
                childrenRunners, num);
    }

    @Override
    public DIHWriter getWriter() {
        if(writer==null){
            writer = docBuilder.createWriter();
        }
        return writer;
    }

    @Override
    public void destroy() { // it's my. I wipe it myself
        if(writer!=null){
            writer.close();
        }
    }
  }

  /** uses DocBuilder's writer, dont' closes it */
  private static final class FirstRootEntityProcessorWrapper extends
        ThreadedEntityProcessorWrapper {
    private FirstRootEntityProcessorWrapper(EntityProcessor delegate,
            DocBuilder docBuilder, EntityRunner entityRunner,
            VariableResolverImpl resolver,
            Map<Entity, EntityRunner> childrenRunners, int num) {
        super(delegate, docBuilder, entityRunner, resolver,
                childrenRunners, num);
    }

    @Override
    public DIHWriter getWriter() {
        return docBuilder.getWriter();
    }

    @Override
    public void destroy() {
        // does nothing, DocBuilder closes writer itself
    }
  }

  /** never ever provides writer */
  private static final class ChildEntityProcessorWrapper extends
        ThreadedEntityProcessorWrapper {
    private ChildEntityProcessorWrapper(EntityProcessor delegate,
            DocBuilder docBuilder, EntityRunner entityRunner,
            VariableResolverImpl resolver,
            Map<Entity, EntityRunner> childrenRunners, int num) {
        super(delegate, docBuilder, entityRunner, resolver,
                childrenRunners, num);
    }

    @Override
    public DIHWriter getWriter() {
        throw new UnsupportedOperationException("not available for " +
                    "children entity entity processor wrappers");
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException("not available for " +
                    "children entity entity processor wrappers");
    }
  }


}
