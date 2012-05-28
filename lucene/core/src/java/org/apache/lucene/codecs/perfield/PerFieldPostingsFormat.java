package org.apache.lucene.codecs.perfield;

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
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader; // javadocs
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.IOUtils;

/**
 * Enables per field format support.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is 
 * written into the index. In order for the field to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} to resolve format names.
 * <p>
 * Files written by each posting format have an additional suffix containing the 
 * format name. For example, in a per-field configuration instead of <tt>_1.prx</tt> 
 * filenames would look like <tt>_1_Lucene40.prx</tt>.
 * @see ServiceLoader
 * @lucene.experimental
 */

public abstract class PerFieldPostingsFormat extends PostingsFormat {
  public static final String PER_FIELD_NAME = "PerField40";
  
  public static final String PER_FIELD_FORMAT_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".format";

  public PerFieldPostingsFormat() {
    super(PER_FIELD_NAME);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new FieldsWriter(state);
  }
    
  private class FieldsWriter extends FieldsConsumer {

    private final Map<PostingsFormat,FieldsConsumer> formats = new IdentityHashMap<PostingsFormat,FieldsConsumer>();

    private final SegmentWriteState segmentWriteState;

    public FieldsWriter(SegmentWriteState state) throws IOException {
      segmentWriteState = state;
    }

    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      final PostingsFormat format = getPostingsFormatForField(field.name);
      if (format == null) {
        throw new IllegalStateException("invalid null PostingsFormat for field=\"" + field.name + "\"");
      }
      
      String previousValue = field.putAttribute(PER_FIELD_FORMAT_KEY, format.getName());
      assert previousValue == null;

      FieldsConsumer consumer = formats.get(format);
      if (consumer == null) {
        // First time we are seeing this format; create a new instance
        final String segmentSuffix = getFullSegmentSuffix(field.name,
                                                          segmentWriteState.segmentSuffix,
                                                          format.getName());
        consumer = format.fieldsConsumer(new SegmentWriteState(segmentWriteState, segmentSuffix));
        formats.put(format, consumer);
      }

      // TODO: we should only provide the "slice" of FIS
      // that this PF actually sees ... then stuff like
      // .hasProx could work correctly?
      // NOTE: .hasProx is already broken in the same way for the non-perfield case,
      // if there is a fieldinfo with prox that has no postings, you get a 0 byte file.
      return consumer.addField(field);
    }

    @Override
    public void close() throws IOException {
      // Close all subs
      IOUtils.close(formats.values());
    }
  }

  static String getFullSegmentSuffix(String fieldName, String outerSegmentSuffix, String segmentSuffix) {
    if (outerSegmentSuffix.length() == 0) {
      return segmentSuffix;
    } else {
      // TODO: support embedding; I think it should work but
      // we need a test confirm to confirm
      // return outerSegmentSuffix + "_" + segmentSuffix;
      throw new IllegalStateException("cannot embed PerFieldPostingsFormat inside itself (field \"" + fieldName + "\" returned PerFieldPostingsFormat)");
    }
  }

  private class FieldsReader extends FieldsProducer {

    private final Map<String,FieldsProducer> fields = new TreeMap<String,FieldsProducer>();
    private final Map<PostingsFormat,FieldsProducer> formats = new IdentityHashMap<PostingsFormat,FieldsProducer>();

    public FieldsReader(final SegmentReadState readState) throws IOException {

      // Read _X.per and init each format:
      boolean success = false;
      try {
        // Read field name -> format name
        for (FieldInfo fi : readState.fieldInfos) {
          if (fi.isIndexed()) {
            final String fieldName = fi.name;
            final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
            if (formatName != null) {
              // null formatName means the field is in fieldInfos, but has no postings!
              PostingsFormat format = PostingsFormat.forName(formatName);
              if (!formats.containsKey(format)) {
                formats.put(format, format.fieldsProducer(new SegmentReadState(readState, formatName)));
              }
              fields.put(fieldName, formats.get(format));
            }
          }
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(formats.values());
        }
      }
    }

    private final class FieldsIterator extends FieldsEnum {
      private final Iterator<String> it;
      private String current;

      public FieldsIterator() {
        it = fields.keySet().iterator();
      }

      @Override
      public String next() throws IOException {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }

        return current;
      }

      @Override
      public Terms terms() throws IOException {
        return fields.get(current).terms(current);
      }
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return new FieldsIterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      FieldsProducer fieldsProducer = fields.get(field);
      return fieldsProducer == null ? null : fieldsProducer.terms(field);
    }
    
    @Override
    public int size() {
      return fields.size();
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(formats.values());
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new FieldsReader(state);
  }

  // NOTE: only called during writing; for reading we read
  // all we need from the index (ie we save the field ->
  // format mapping)
  public abstract PostingsFormat getPostingsFormatForField(String field);
}
