package org.apache.lucene.codecs.preflexrw;

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

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfosFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Writes 3.x-like indexes (not perfect emulation yet) for testing only!
 * @lucene.experimental
 */
public class PreFlexRWCodec extends Lucene3xCodec {
  private final PostingsFormat postings = new PreFlexRWPostingsFormat();
  private final NormsFormat norms = new PreFlexRWNormsFormat();
  private final FieldInfosFormat fieldInfos = new PreFlexRWFieldInfosFormat();
  private final TermVectorsFormat termVectors = new PreFlexRWTermVectorsFormat();
  private final SegmentInfosFormat segmentInfos = new PreFlexRWSegmentInfosFormat();
  // TODO: this should really be a different impl
  private final LiveDocsFormat liveDocs = new Lucene40LiveDocsFormat();
  // TODO: this should really be a different impl
  private final StoredFieldsFormat storedFields = new Lucene40StoredFieldsFormat();
  
  @Override
  public PostingsFormat postingsFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return postings;
    } else {
      return super.postingsFormat();
    }
  }

  @Override
  public NormsFormat normsFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return norms;
    } else {
      return super.normsFormat();
    }
  }

  @Override
  public SegmentInfosFormat segmentInfosFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return segmentInfos ;
    } else {
      return super.segmentInfosFormat();
    }
  }

  @Override
  public FieldInfosFormat fieldInfosFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return fieldInfos;
    } else {
      return super.fieldInfosFormat();
    }
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return termVectors;
    } else {
      return super.termVectorsFormat();
    }
  }

  @Override
  public LiveDocsFormat liveDocsFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return liveDocs;
    } else {
      return super.liveDocsFormat();
    }
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return storedFields;
    } else {
      return super.storedFieldsFormat();
    }
  }
}
