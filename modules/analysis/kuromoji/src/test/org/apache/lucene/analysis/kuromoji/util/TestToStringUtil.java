package org.apache.lucene.analysis.kuromoji.util;

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

import org.apache.lucene.util.LuceneTestCase;

public class TestToStringUtil extends LuceneTestCase {
  public void testPOS() {
    assertEquals("noun-suffix-verbal", ToStringUtil.getPOSTranslation("名詞-接尾-サ変接続"));
  }
  
  public void testHepburn() {
    assertEquals("majan", ToStringUtil.getRomanization("マージャン"));
    assertEquals("uroncha", ToStringUtil.getRomanization("ウーロンチャ"));
    assertEquals("chahan", ToStringUtil.getRomanization("チャーハン"));
    assertEquals("chashu", ToStringUtil.getRomanization("チャーシュー"));
    assertEquals("shumai", ToStringUtil.getRomanization("シューマイ"));
  }
}
