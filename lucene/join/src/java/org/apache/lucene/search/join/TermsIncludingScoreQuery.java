package org.apache.lucene.search.join;

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
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Set;

class TermsIncludingScoreQuery extends Query {

  final String field;
  final boolean multipleValuesPerDocument;
  final BytesRefHash terms;
  final float[] scores;
  final int[] ords;
  final Query originalQuery;
  final Query unwrittenOriginalQuery;

  TermsIncludingScoreQuery(String field, boolean multipleValuesPerDocument, BytesRefHash terms, float[] scores, Query originalQuery) {
    this.field = field;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.originalQuery = originalQuery;
    this.ords = terms.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    this.unwrittenOriginalQuery = originalQuery;
  }

  private TermsIncludingScoreQuery(String field, boolean multipleValuesPerDocument, BytesRefHash terms, float[] scores, int[] ords, Query originalQuery, Query unwrittenOriginalQuery) {
    this.field = field;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.originalQuery = originalQuery;
    this.ords = ords;
    this.unwrittenOriginalQuery = unwrittenOriginalQuery;
  }

  public String toString(String string) {
    return String.format("TermsIncludingScoreQuery{field=%s;originalQuery=%s}", field, unwrittenOriginalQuery);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    originalQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query originalQueryRewrite = originalQuery.rewrite(reader);
    if (originalQueryRewrite != originalQuery) {
      Query rewritten = new TermsIncludingScoreQuery(field, multipleValuesPerDocument, terms, scores,
          ords, originalQueryRewrite, originalQuery);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return this;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    final Weight originalWeight = originalQuery.createWeight(searcher);
    return new Weight() {

      private TermsEnum segmentTermsEnum;

      public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
        SVInnerScorer scorer = (SVInnerScorer) scorer(context, true, false, context.reader().getLiveDocs());
        if (scorer != null) {
          if (scorer.advance(doc) == doc) {
            return scorer.explain();
          }
        }
        return new ComplexExplanation(false, 0.0f, "Not a match");
      }

      public Query getQuery() {
        return TermsIncludingScoreQuery.this;
      }

      public float getValueForNormalization() throws IOException {
        return originalWeight.getValueForNormalization() * TermsIncludingScoreQuery.this.getBoost() * TermsIncludingScoreQuery.this.getBoost();
      }

      public void normalize(float norm, float topLevelBoost) {
        originalWeight.normalize(norm, topLevelBoost * TermsIncludingScoreQuery.this.getBoost());
      }

      public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null;
        }

        segmentTermsEnum = terms.iterator(segmentTermsEnum);
        if (multipleValuesPerDocument) {
          return new MVInnerScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc());
        } else {
          return new SVInnerScorer(this, acceptDocs, segmentTermsEnum);
        }
      }
    };
  }

  // This impl assumes that the 'join' values are used uniquely per doc per field. Used for one to many relations.
  class SVInnerScorer extends Scorer {

    final BytesRef spare = new BytesRef();
    final Bits acceptDocs;
    final TermsEnum termsEnum;

    int upto;
    DocsEnum docsEnum;
    DocsEnum reuse;
    int scoreUpto;

    SVInnerScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum) {
      super(weight);
      this.acceptDocs = acceptDocs;
      this.termsEnum = termsEnum;
    }

    public float score() throws IOException {
      return scores[ords[scoreUpto]];
    }

    public Explanation explain() throws IOException {
      return new ComplexExplanation(true, score(), "Score based on join value " + termsEnum.term().utf8ToString());
    }

    public int docID() {
      return docsEnum != null ? docsEnum.docID() : DocIdSetIterator.NO_MORE_DOCS;
    }

    public int nextDoc() throws IOException {
      if (docsEnum != null) {
        int docId = docsEnum.nextDoc();
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          docsEnum = null;
        } else {
          return docId;
        }
      }

      do {
        if (upto == terms.size()) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }

        scoreUpto = upto;
        TermsEnum.SeekStatus status = termsEnum.seekCeil(terms.get(ords[upto++], spare), true);
        if (status == TermsEnum.SeekStatus.FOUND) {
          docsEnum = reuse = termsEnum.docs(acceptDocs, reuse, false);
        }
      } while (docsEnum == null);

      return docsEnum.nextDoc();
    }

    public int advance(int target) throws IOException {
      int docId;
      do {
        docId = nextDoc();
        if (docId < target) {
          int tempDocId = docsEnum.advance(target);
          if (tempDocId == target) {
            docId = tempDocId;
            break;
          }
        } else if (docId == target) {
          break;
        }
        docsEnum = null; // goto the next ord.
      } while (docId != DocIdSetIterator.NO_MORE_DOCS);
      return docId;
    }
  }

  // This impl that tracks whether a docid has already been emitted. This check makes sure that docs aren't emitted
  // twice for different join values. This means that the first encountered join value determines the score of a document
  // even if other join values yield a higher score.
  class MVInnerScorer extends SVInnerScorer {

    final FixedBitSet alreadyEmittedDocs;

    MVInnerScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, int maxDoc) {
      super(weight, acceptDocs, termsEnum);
      alreadyEmittedDocs = new FixedBitSet(maxDoc);
    }

    public int nextDoc() throws IOException {
      if (docsEnum != null) {
        int docId;
        do {
          docId = docsEnum.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
        } while (alreadyEmittedDocs.get(docId));
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          docsEnum = null;
        } else {
          alreadyEmittedDocs.set(docId);
          return docId;
        }
      }

      for (;;) {
        do {
          if (upto == terms.size()) {
            return DocIdSetIterator.NO_MORE_DOCS;
          }

          scoreUpto = upto;
          TermsEnum.SeekStatus status = termsEnum.seekCeil(terms.get(ords[upto++], spare), true);
          if (status == TermsEnum.SeekStatus.FOUND) {
            docsEnum = reuse = termsEnum.docs(acceptDocs, reuse, false);
          }
        } while (docsEnum == null);

        int docId;
        do {
          docId = docsEnum.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
        } while (alreadyEmittedDocs.get(docId));
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          docsEnum = null;
        } else {
          alreadyEmittedDocs.set(docId);
          return docId;
        }
      }
    }
  }

}
