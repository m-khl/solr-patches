package org.apache.lucene.queries.function.valuesource;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.StringIndexDocValues;
import org.apache.lucene.queries.function.ValueSource; //javadoc

import java.io.IOException;
import java.util.Map;

/**
 * An implementation for retrieving {@link FunctionValues} instances for string based fields.
 */
public class BytesRefFieldSource extends FieldCacheSource {

  public BytesRefFieldSource(String field) {
    super(field);
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    return new StringIndexDocValues(this, readerContext, field) {

      @Override
      protected String toTerm(String readableValue) {
        return readableValue;
      }

      @Override
      public Object objectVal(int doc) {
        return strVal(doc);
      }

      @Override
      public String toString(int doc) {
        return description() + '=' + strVal(doc);
      }

    };
  }
}
