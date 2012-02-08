package org.apache.lucene.analysis.de;

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
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Tests {@link GermanNormalizationFilter}
 */
public class TestGermanNormalizationFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String field, Reader reader) {
      final Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
      final TokenStream stream = new GermanNormalizationFilter(tokenizer);
      return new TokenStreamComponents(tokenizer, stream);
    }
  };
  
  /**
   * Tests that a/o/u + e is equivalent to the umlaut form
   */
  public void testBasicExamples() throws IOException {
    checkOneTerm(analyzer, "Schaltflächen", "Schaltflachen");
    checkOneTerm(analyzer, "Schaltflaechen", "Schaltflachen");
  }

  /**
   * Tests the specific heuristic that ue is not folded after a vowel or q.
   */
  public void testUHeuristic() throws IOException {
    checkOneTerm(analyzer, "dauer", "dauer");
  }
  
  /**
   * Tests german specific folding of sharp-s
   */
  public void testSpecialFolding() throws IOException {
    checkOneTerm(analyzer, "weißbier", "weissbier");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, analyzer, 10000*RANDOM_MULTIPLIER);
  }
}
