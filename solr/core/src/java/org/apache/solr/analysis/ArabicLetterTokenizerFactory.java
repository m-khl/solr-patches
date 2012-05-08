package org.apache.solr.analysis;
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

import org.apache.lucene.analysis.ar.ArabicLetterTokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.Map;


/**
 * Factory for {@link ArabicLetterTokenizer}
 * @deprecated (3.1) Use StandardTokenizerFactory instead.
 **/
@Deprecated
public class ArabicLetterTokenizerFactory extends TokenizerFactory {

  private static final Logger log = LoggerFactory.getLogger(ArabicLetterTokenizerFactory.class);

  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    assureMatchVersion();
    log.warn(getClass().getSimpleName() + " is deprecated. Use StandardTokenizeFactory instead.");
  }

  public ArabicLetterTokenizer create(Reader input) {
    return new ArabicLetterTokenizer(luceneMatchVersion, input);
  }
}
