/*
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
package org.apache.solr.search.function;

import java.io.*;
import java.util.*;

import org.apache.lucene.index.*;
import org.apache.lucene.queries.function.*;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.*;
import org.apache.solr.search.FunctionQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.function.FileFloatSource.Cache.Closure;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.VersionedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Obtains float field values from an external file.
 *
 * @see org.apache.solr.schema.ExternalFileField
 * @see org.apache.solr.schema.ExternalFileFieldReloader
 */

public class FileFloatSource extends ValueSource {
  
  private static final Logger log = LoggerFactory.getLogger(FileFloatSource.class);
  
  static class Data {
    private final SchemaField field;
    private final SchemaField keyField;
    private final float defVal;
    private final String dataDir;
    
    Data(SchemaField field, SchemaField keyField,
        float defVal, String dataDir) {
      this.field = field;
      this.keyField = keyField;
      this.defVal = defVal;
      this.dataDir = dataDir;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((dataDir == null) ? 0 : dataDir.hashCode());
      result = prime * result + Float.floatToIntBits(defVal);
      result = prime * result + ((field == null) ? 0 : field.hashCode());
      result = prime * result + ((keyField == null) ? 0 : keyField.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Data other = (Data) obj;
      if (dataDir == null) {
        if (other.dataDir != null) return false;
      } else if (!dataDir.equals(other.dataDir)) return false;
      if (Float.floatToIntBits(defVal) != Float.floatToIntBits(other.defVal)) return false;
      if (field == null) {
        if (other.field != null) return false;
      } else if (!field.equals(other.field)) return false;
      if (keyField == null) {
        if (other.keyField != null) return false;
      } else if (!keyField.equals(other.keyField)) return false;
      return true;
    }
  }

  final Data data ;

  public FileFloatSource(SchemaField field, SchemaField keyField, float defVal, String datadir) {
    data = new Data(field, keyField, defVal, datadir);
  }


  private static Versioned<float[]> getFromContext(Map<Object,Object> context,
      Data data2) {
    return (Versioned<float[]>) context.get(data2);
  }

  private static void setToContext(Map<Object,Object> context, Data data2,
      Versioned<float[]> values2) {
    Object older = context.put(data2,values2);
    assert older==null;
  }

  @Override
  public String description() {
    return "float(" + data.field +')';
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final int off = readerContext.docBase;
    IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(readerContext);

    final float[] arr = getCachedVersionedFloats(topLevelContext.reader(), this.data).target;
    
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return arr[doc + off];
      }

      @Override
      public Object objectVal(int doc) {
        return floatVal(doc);   // TODO: keep track of missing values
      }
    };
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    FileFloatSource other = (FileFloatSource) obj;
    if (data == null) {
      if (other.data != null) return false;
    } else if (!data.equals(other.data)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "FileFloatSource(field="+data.field.getName()+",keyField="+data.keyField.getName()
            + ",defVal="+data.defVal+",dataDir="+data.dataDir+")";

  }

  /**
   * Remove all cached entries.  Values are lazily loaded next time getValues() is
   * called.
   */
  public static void resetCache(){
    floatCache.resetCache();
  }

  /**
   * Refresh the cache for an IndexReader.  The new values are loaded in the background
   * and then swapped in, so queries against the cache should not block while the reload
   * is happening.
   * @param reader the IndexReader whose cache needs refreshing
   */
  public void refreshCache(IndexReader reader) {
    log.info("Refreshing FlaxFileFloatSource cache for field {}", this.data.field.getName());
    floatCache.refresh(reader, this.data);
    log.info("FlaxFileFloatSource cache for field {} reloaded", this.data.field.getName());
  }

  /** synchronously load floats from new (or it mightbe old) file and puts into cache,
   * for later queries */
  public void reload(SolrQueryRequest req){
    
    IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(
        req.getSearcher().getTopReaderContext());
    IndexReader reader = topLevelContext.reader();
    
    final float[] floats = getFloats(data, reader);
    
    floatCache.increment(reader, (Object)this.data, new Closure() {
      @Override
      public Object process(Object ascender) {
        Versioned<float[]> old = (Versioned<float[]>) ascender;
        return new Versioned(floats, old==null ? 0L : old.version+1);
      }
    });
  }
  
  /** test introspection. returns key for internal cache lookups, may NOT trigger file loading */
  static Object getFieldValueSourceKey(SolrQueryRequest req, String fieldName){
    SchemaField schemaField = req.getSchema().getField(fieldName);
    ExternalFileField fileField = (ExternalFileField) schemaField.getType();
    
    return new Data(schemaField,fileField.getKeyField(), fileField.getDefVal(), req.getCore().getDataDir()) ;
    //((FileFloatSource) ((FunctionQuery) QParser.getParser("{!func}"+fieldName, null, req).getQuery()).getValueSource()).data;
  }

  /** test introspection. does not cause side effects like entries initialization
   * can throw NPE  */
  static float[] getCachedValue(SolrQueryRequest req, Object fooKey) {
    Map readersCache = (Map) floatCache.readerCache.get(req.getSearcher().getIndexReader());
    return ((Versioned<float[]>) readersCache.get(fooKey)).target;
  }

  private final static Versioned<float[]> getCachedVersionedFloats(IndexReader reader, Data data2) {
    return (Versioned<float[]>)floatCache.get(reader, data2);
  }

  static Cache floatCache = new Cache() {
    @Override
    protected Object createValue(IndexReader reader, Object key) {
      return new Versioned(getFloats( (Data)key, reader), 0L);
    }
  };

  /** Internal cache. (from lucene FieldCache) */
  abstract static class Cache {
    final Map readerCache = new WeakHashMap();

    protected abstract Object createValue(IndexReader reader, Object key);

    public void refresh(IndexReader reader, Object key) {
      Object refreshedValues = createValue(reader, key);
      synchronized (readerCache) {
        Map innerCache = (Map) readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new HashMap();
          readerCache.put(reader, innerCache);
        }
        innerCache.put(key, refreshedValues);
      }
    }

    public Object get(IndexReader reader, Object key) {
      Map innerCache;
      Object value;
      synchronized (readerCache) {
        innerCache = (Map) readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new HashMap();
          readerCache.put(reader, innerCache);
          value = null;
        } else {
          value = innerCache.get(key);
        }
        if (value == null) {
          value = new CreationPlaceholder();
          innerCache.put(key, value);
        }
      }
      if (value instanceof CreationPlaceholder) {
        synchronized (value) {
          CreationPlaceholder progress = (CreationPlaceholder) value;
          if (progress.value == null) {
            progress.value = createValue(reader, key);
            synchronized (readerCache) {
              innerCache.put(key, progress.value);
              onlyForTesting = progress.value;
            }
          }
          return progress.value;
        }
      }

      return value;
    }
    
    public void resetCache(){
      synchronized(readerCache){
        // Map.clear() is optional and can throw UnsipportedOperationException,
        // but readerCache is WeakHashMap and it supports clear().
        readerCache.clear();
      }
    }
    
    interface Closure{
      Object process(Object ascender);
    }
    
    /**
     * applies the given action to an existing entry, or null if it is not exist
     * */
    public void increment(IndexReader reader, Object key, Closure action){
      Map innerCache;
      synchronized (readerCache) {
        innerCache = (Map) readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new HashMap();
          readerCache.put(reader, innerCache);
        } 
        Object ascender = innerCache.get(key);
          Object value = action.process(ascender);
          Object pushedOut = innerCache.put(key, value );
          onlyForTesting = value;
          assert ascender==pushedOut: "it seems like a race for "+key ;
        }
      }
  }

  static Object onlyForTesting; // set to the last value

  static final class CreationPlaceholder {
    Object value;
  }

    /** Expert: Every composite-key in the internal cache is of this type. */
  /**private static class Entry {
    final FileFloatSource ffs;
    public Entry(FileFloatSource ffs) {
      this.ffs = ffs;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Entry)) return false;
      Entry other = (Entry)o;
      return ffs.equals(other.ffs);
    }

    @Override
    public int hashCode() {
      return ffs.hashCode();
    }
  }*/



  private static float[] getFloats(Data ffs, IndexReader reader) {
    float[] vals = new float[reader.maxDoc()];
    if (ffs.defVal != 0) {
      Arrays.fill(vals, ffs.defVal);
    }
    InputStream is;
    String fname = "external_" + ffs.field.getName();
    try {
      is = VersionedFile.getLatestFile(ffs.dataDir, fname);
    } catch (IOException e) {
      // log, use defaults
      SolrCore.log.error("Error opening external value source file: " +e);
      return vals;
    }

    BufferedReader r = new BufferedReader(new InputStreamReader(is, IOUtils.CHARSET_UTF_8));

    String idName = ffs.keyField.getName();
    FieldType idType = ffs.keyField.getType();

    // warning: lucene's termEnum.skipTo() is not optimized... it simply does a next()
    // because of this, simply ask the reader for a new termEnum rather than
    // trying to use skipTo()

    List<String> notFound = new ArrayList<String>();
    int notFoundCount=0;
    int otherErrors=0;

    char delimiter='=';

    BytesRef internalKey = new BytesRef();

    try {
      TermsEnum termsEnum = MultiFields.getTerms(reader, idName).iterator(null);
      DocsEnum docsEnum = null;

      // removing deleted docs shouldn't matter
      // final Bits liveDocs = MultiFields.getLiveDocs(reader);

      for (String line; (line=r.readLine())!=null;) {
        int delimIndex = line.lastIndexOf(delimiter);
        if (delimIndex < 0) continue;

        int endIndex = line.length();
        String key = line.substring(0, delimIndex);
        String val = line.substring(delimIndex+1, endIndex);

        float fval;
        try {
          idType.readableToIndexed(key, internalKey);
          fval=Float.parseFloat(val);
        } catch (Exception e) {
          if (++otherErrors<=10) {
            SolrCore.log.error( "Error loading external value source + fileName + " + e
              + (otherErrors<10 ? "" : "\tSkipping future errors for this file.")
            );
          }
          continue;  // go to next line in file.. leave values as default.
        }

        if (!termsEnum.seekExact(internalKey, false)) {
          if (notFoundCount<10) {  // collect first 10 not found for logging
            notFound.add(key);
          }
          notFoundCount++;
          continue;
        }

        docsEnum = termsEnum.docs(null, docsEnum, 0);
        int doc;
        while ((doc = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          vals[doc] = fval;
        }
      }

    } catch (IOException e) {
      // log, use defaults
      SolrCore.log.error("Error loading external value source: " +e);
    } finally {
      // swallow exceptions on close so we don't override any
      // exceptions that happened in the loop
      try{r.close();}catch(Exception e){}
    }

    SolrCore.log.info("Loaded external value source " + fname
      + (notFoundCount==0 ? "" : " :"+notFoundCount+" missing keys "+notFound)
    );

    return vals;
  }
  
  static class Versioned<T>{
    private final long version;
    T target;

    public Versioned(T target, long timestamp) {
      this.target = target;
      this.version = timestamp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((target == null) ? 0 : target.hashCode());
      result = prime * result + (int) (version ^ (version >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Versioned other = (Versioned) obj;
      if (target == null) {
        if (other.target != null) return false;
      } else if (!target.equals(other.target)) return false;
      if (version != other.version) return false;
      return true;
    }
  }

  public static class ReloadCacheRequestHandler extends RequestHandlerBase {
    
    static final Logger log = LoggerFactory.getLogger(ReloadCacheRequestHandler.class);

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception {
      FileFloatSource.resetCache();
      log.debug("readerCache has been reset.");

      UpdateRequestProcessor processor =
        req.getCore().getUpdateProcessingChain(null).createProcessor(req, rsp);
      try{
        RequestHandlerUtils.handleCommit(req, processor, req.getParams(), true);
      }
      finally{
        processor.finish();
      }
    }

    @Override
    public String getDescription() {
      return "Reload readerCache request handler";
    }

    @Override
    public String getSource() {
      return "$URL$";
    }
  }
  
  public static class ReloadFieldRequestHandler extends RequestHandlerBase {

    static final Logger log = LoggerFactory.getLogger(ReloadFieldRequestHandler.class);

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
        throws Exception {
      log.debug("readerCache has been reset.");
      String fieldName = req.getParams().get("field");
      
      ((FileFloatSource) ((FunctionQuery) QParser.getParser(fieldName,
          FunctionQParserPlugin.NAME, req).getQuery()).getValueSource())
              .reload(req);
    }

    @Override
    public String getDescription() {
      return "Reload Field request handler";
    }

    @Override
    public String getSource() {
      return "$URL$";
    }
  }
  
}
