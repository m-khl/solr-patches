/**
 * 
 */
package org.apache.solr.handler.dataimport;

import java.util.HashMap;

import org.apache.solr.handler.dataimport.DataConfig.Entity;
import org.apache.solr.handler.dataimport.DataImporter.RequestParams;
import org.junit.Test;
/**
 * @author fwe
 *
 */
public class TestContextImpl extends AbstractDataImportHandlerTestCase {
  
  @Test
  public void testEntityScope() {
    ContextImpl ctx = new ContextImpl(new Entity(), new VariableResolverImpl(), null, "something", new HashMap<String,Object>(), null, null);
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_ENTITY);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_ENTITY);
    
    assertEquals(lala, got);
    
  }
  @Test
  public void testCoreScope() {
    DataImporter di = new DataImporter();
    di.loadAndInit("<dataConfig><document /></dataConfig>");
    DIHWriter.Factory writer = new DIHWriter.Factory() {
        @Override
        public DIHWriter create() {
            return new SolrWriter(null, null);
        }
    };
    DocBuilder db = new DocBuilder(di, writer,new SimplePropertiesWriter(), new RequestParams());
    ContextImpl ctx = new ContextImpl(new Entity(), new VariableResolverImpl(), null, "something", new HashMap<String,Object>(), null, db);
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_SOLR_CORE);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_SOLR_CORE);
    assertEquals(lala, got);
    
  }
  @Test
  public void testDocumentScope() {
    ContextImpl ctx = new ContextImpl(new Entity(), new VariableResolverImpl(), null, "something", new HashMap<String,Object>(), null, null);
    ctx.setDoc(new DocBuilder.DocWrapper());
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_DOC);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_DOC);
    
    assertEquals(lala, got);
    
  }
  @Test
  public void testGlobalScope() {
    ContextImpl ctx = new ContextImpl(new Entity(), new VariableResolverImpl(), null, "something", new HashMap<String,Object>(), null, null);
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_GLOBAL);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_GLOBAL);
    
    assertEquals(lala, got);
    
  }
  
}
