package org.apache.lucene.analysis.kuromoji;

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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.kuromoji.Segmenter.Mode;
import org.apache.lucene.util.IOUtils;

public class TestSearchMode extends BaseTokenStreamTestCase {
  private final static String SEGMENTATION_FILENAME = "search-segmentation-tests.txt";
  private final Segmenter segmenter = new Segmenter(Mode.SEARCH);
  private final Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new KuromojiTokenizer(segmenter, reader);
      return new TokenStreamComponents(tokenizer, tokenizer);
    }
  };
  
  /** Test search mode segmentation */
  public void testSearchSegmentation() throws IOException {
    InputStream is = TestSearchMode.class.getResourceAsStream(SEGMENTATION_FILENAME);
    if (is == null) {
      throw new FileNotFoundException("Cannot find " + SEGMENTATION_FILENAME + " in test classpath");
    }
    try {
      LineNumberReader reader = new LineNumberReader(new InputStreamReader(is, IOUtils.CHARSET_UTF_8));
      String line = null;
      while ((line = reader.readLine()) != null) {
        // Remove comments
        line = line.replaceAll("#.*$", "");
        // Skip empty lines or comment lines
        if (line.trim().isEmpty()) {
          continue;
        }
        if (VERBOSE) {
          System.out.println("Line no. " + reader.getLineNumber() + ": " + line);
        }
        String[] fields = line.split("\t", 2);
        String sourceText = fields[0];
        String[] expectedTokens = fields[1].split("\\s+");
        assertAnalyzesTo(analyzer, sourceText, expectedTokens);
      }
    } finally {
      is.close();
    }
  }
}
