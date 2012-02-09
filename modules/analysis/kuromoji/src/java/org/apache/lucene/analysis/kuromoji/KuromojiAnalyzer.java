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

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

public class KuromojiAnalyzer extends StopwordAnalyzerBase {
  private final Segmenter segmenter;
  private final Set<String> stoptags;
  
  public KuromojiAnalyzer(Version matchVersion) {
    this(matchVersion, new Segmenter(), DefaultSetHolder.DEFAULT_STOP_SET, DefaultSetHolder.DEFAULT_STOP_TAGS);
  }
  
  public KuromojiAnalyzer(Version matchVersion, Segmenter segmenter, CharArraySet stopwords, Set<String> stoptags) {
    super(matchVersion, stopwords);
    this.segmenter = segmenter;
    this.stoptags = stoptags;
  }
  
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  public static Set<String> getDefaultStopTags(){
    return DefaultSetHolder.DEFAULT_STOP_TAGS;
  }
  
  /**
   * Atomically loads DEFAULT_STOP_SET, DEFAULT_STOP_TAGS in a lazy fashion once the 
   * outer class accesses the static final set the first time.
   */
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;
    static final Set<String> DEFAULT_STOP_TAGS;

    static {
      try {
        DEFAULT_STOP_SET = loadStopwordSet(false, KuromojiAnalyzer.class, "stopwords.txt", "#");
        final CharArraySet tagset = loadStopwordSet(false, KuromojiAnalyzer.class, "stoptags.txt", "#");
        DEFAULT_STOP_TAGS = new HashSet<String>();
        for (Object element : tagset) {
          char chars[] = (char[]) element;
          DEFAULT_STOP_TAGS.add(new String(chars));
        }
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }
  
  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    Tokenizer tokenizer = new KuromojiTokenizer(this.segmenter, reader);
    TokenStream stream = new LowerCaseFilter(matchVersion, tokenizer);
    stream = new CJKWidthFilter(stream);
    stream = new KuromojiPartOfSpeechStopFilter(true, stream, stoptags);
    stream = new StopFilter(matchVersion, stream, stopwords);
    stream = new KuromojiBaseFormFilter(stream);
    return new TokenStreamComponents(tokenizer, stream);
  }
}
