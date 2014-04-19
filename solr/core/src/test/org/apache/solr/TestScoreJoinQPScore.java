package org.apache.solr;

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

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.ScoreJoinQParserPlugin;
import org.junit.BeforeClass;

public class TestScoreJoinQPScore extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    // TODO badass
    QParserPlugin.standardPlugins[QParserPlugin.standardPlugins.length-2]=ScoreJoinQParserPlugin.NAME;
    QParserPlugin.standardPlugins[QParserPlugin.standardPlugins.length-1]=ScoreJoinQParserPlugin.class;
    initCore("solrconfig.xml","schema12.xml");
  }

  public void testSimple() throws Exception {
    final String idField = "id";
    final String toField = "productId_s";

    clearIndex();

    // 0
    assertU(add(doc("t_description", "random text",
        "name", "name1",
        idField, "1")));

// 1
    
    assertU(add(doc("price_s", "10.0",
        idField, "2",
        toField, "1")));
// 2
    assertU(add(doc("price_s", "20.0",
        idField, "3", 
        toField, "1")));
// 3
    assertU(add(doc("t_description", "more random text",
        "name", "name2",
        idField, "4")));
// 4
    assertU(add(doc("price_s", "10.0",
        idField, "5", 
        toField, "4")));
// 5
    assertU(add(doc("price_s", "20.0",
        idField, "6",
        toField, "4")));

    assertU(commit());
    
    // Search for product
    assertJQ(req("q","{!scorejoin from="+idField+" to="+toField+" score=None}name:name2", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'5'},{'id':'6'}]}" );
    
    /*Query joinQuery =
        JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);

    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(4, result.scoreDocs[0].doc);
    assertEquals(5, result.scoreDocs[1].doc);
    */
     assertJQ(req("q","{!scorejoin from="+idField+" to="+toField+" score=None}name:name1", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'2'},{'id':'3'}]}" );

    /*joinQuery = JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name1")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(1, result.scoreDocs[0].doc);
    assertEquals(2, result.scoreDocs[1].doc);*/

    // Search for offer
     assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=None}id:5", "fl","id")
         ,"/response=={'numFound':1,'start':0,'docs':[{'id':'4'}]}" );
    /*joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "5")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);

    indexSearcher.getIndexReader().close();
    dir.close();*/
  }

  public void testSimpleWithScoring() throws Exception {
    final String idField = "id";
    final String toField = "movieId_s";

    clearIndex();

// 0
    assertU(add(doc("t_description", "A random movie",
    "name", "Movie 1",
    idField, "1")));
// 1
    
    assertU(add(doc("title", "The first subtitle of this movie", 
    idField, "2",
    toField, "1")));
    

// 2
    
    assertU(add(doc("title", "random subtitle; random event movie", 
    idField, "3", 
    toField, "1"))); 

// 3
    
    assertU(add(doc("t_description", "A second random movie", 
    "name", "Movie 2", 
    idField, "4")));
// 4
    
    assertU(add(doc("title", "a very random event happened during christmas night",
    idField, "5",
    toField, "4")));
    

// 5
    
    assertU(add(doc("title", "movie end movie test 123 test 123 random", 
    idField, "6", 
    toField, "4"))); 
    

    assertU(optimize());

    // Search for movie via subtitle
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:random", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'1'},{'id':'4'}]}" );
    //dump(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:random", "fl","id,score", "debug", "true"));
    /*
    Query joinQuery =
        JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "random")), indexSearcher, ScoreMode.Max);
    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(0, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);*/

    
    // Score mode max.
    //dump(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:movie", "fl","id,score", "debug", "true"));
    
   // dump(req("q","title:movie", "fl","id,score", "debug", "true"));
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'4'},{'id':'1'}]}" );
    
    /*joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Max);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);
    assertEquals(0, result.scoreDocs[1].doc);*/
    
    // Score mode total
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Total}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'1'},{'id':'4'}]}" );
  /*  joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Total);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(0, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);
*/
    //Score mode avg
    assertJQ(req("q","{!scorejoin from="+toField+" to="+idField+" score=Avg}title:movie", "fl","id")
        ,"/response=={'numFound':2,'start':0,'docs':[{'id':'4'},{'id':'1'}]}" );
    
  /*  joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Avg);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);
    assertEquals(0, result.scoreDocs[1].doc);*/
  }
/*
  @Test
  @Slow
  public void testSingleValueRandomJoin() throws Exception {
    int maxIndexIter = TestUtil.nextInt(random(), 6, 12);
    int maxSearchIter = TestUtil.nextInt(random(), 13, 26);
    executeRandomJoin(false, maxIndexIter, maxSearchIter, TestUtil.nextInt(random(), 87, 764));
  }

  @Test
  @Slow
  // This test really takes more time, that is why the number of iterations are smaller.
  public void testMultiValueRandomJoin() throws Exception {
    int maxIndexIter = TestUtil.nextInt(random(), 3, 6);
    int maxSearchIter = TestUtil.nextInt(random(), 6, 12);
    executeRandomJoin(true, maxIndexIter, maxSearchIter, TestUtil.nextInt(random(), 11, 57));
  }

  private void executeRandomJoin(boolean multipleValuesPerDocument, int maxIndexIter, int maxSearchIter, int numberOfDocumentsToIndex) throws Exception {
    for (int indexIter = 1; indexIter <= maxIndexIter; indexIter++) {
      if (VERBOSE) {
        System.out.println("indexIter=" + indexIter);
      }
      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(
          random(),
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)).setMergePolicy(newLogMergePolicy())
      );
      final boolean scoreDocsInOrder = TestScoreJoinQParser.random().nextBoolean();
      IndexIterationContext context = createContext(numberOfDocumentsToIndex, w, multipleValuesPerDocument, scoreDocsInOrder);

      IndexReader topLevelReader = w.getReader();
      w.close();
      for (int searchIter = 1; searchIter <= maxSearchIter; searchIter++) {
        if (VERBOSE) {
          System.out.println("searchIter=" + searchIter);
        }
        IndexSearcher indexSearcher = newSearcher(topLevelReader);

        int r = random().nextInt(context.randomUniqueValues.length);
        boolean from = context.randomFrom[r];
        String randomValue = context.randomUniqueValues[r];
        FixedBitSet expectedResult = createExpectedResult(randomValue, from, indexSearcher.getIndexReader(), context);

        final Query actualQuery = new TermQuery(new Term("value", randomValue));
        if (VERBOSE) {
          System.out.println("actualQuery=" + actualQuery);
        }
        final ScoreMode scoreMode = ScoreMode.values()[random().nextInt(ScoreMode.values().length)];
        if (VERBOSE) {
          System.out.println("scoreMode=" + scoreMode);
        }

        final Query joinQuery;
        if (from) {
          joinQuery = JoinUtil.createJoinQuery("from", multipleValuesPerDocument, "to", actualQuery, indexSearcher, scoreMode);
        } else {
          joinQuery = JoinUtil.createJoinQuery("to", multipleValuesPerDocument, "from", actualQuery, indexSearcher, scoreMode);
        }
        if (VERBOSE) {
          System.out.println("joinQuery=" + joinQuery);
        }

        // Need to know all documents that have matches. TopDocs doesn't give me that and then I'd be also testing TopDocsCollector...
        final FixedBitSet actualResult = new FixedBitSet(indexSearcher.getIndexReader().maxDoc());
        final TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(10, false);
        indexSearcher.search(joinQuery, new Collector() {

          int docBase;

          @Override
          public void collect(int doc) throws IOException {
            actualResult.set(doc + docBase);
            topScoreDocCollector.collect(doc);
          }

          @Override
          public void setNextReader(AtomicReaderContext context) {
            docBase = context.docBase;
            topScoreDocCollector.setNextReader(context);
          }

          @Override
          public void setScorer(Scorer scorer) throws IOException {
            topScoreDocCollector.setScorer(scorer);
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return scoreDocsInOrder;
          }
        });
        // Asserting bit set...
        if (VERBOSE) {
          System.out.println("expected cardinality:" + expectedResult.cardinality());
          DocIdSetIterator iterator = expectedResult.iterator();
          for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            System.out.println(String.format(Locale.ROOT, "Expected doc[%d] with id value %s", doc, indexSearcher.doc(doc).get("id")));
          }
          System.out.println("actual cardinality:" + actualResult.cardinality());
          iterator = actualResult.iterator();
          for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            System.out.println(String.format(Locale.ROOT, "Actual doc[%d] with id value %s", doc, indexSearcher.doc(doc).get("id")));
          }
        }
        assertEquals(expectedResult, actualResult);

        // Asserting TopDocs...
        TopDocs expectedTopDocs = createExpectedTopDocs(randomValue, from, scoreMode, context);
        TopDocs actualTopDocs = topScoreDocCollector.topDocs();
        assertEquals(expectedTopDocs.totalHits, actualTopDocs.totalHits);
        assertEquals(expectedTopDocs.scoreDocs.length, actualTopDocs.scoreDocs.length);
        if (scoreMode == ScoreMode.None) {
          continue;
        }

        assertEquals(expectedTopDocs.getMaxScore(), actualTopDocs.getMaxScore(), 0.0f);
        for (int i = 0; i < expectedTopDocs.scoreDocs.length; i++) {
          if (VERBOSE) {
            System.out.printf("Expected doc: %d | Actual doc: %d\n", expectedTopDocs.scoreDocs[i].doc, actualTopDocs.scoreDocs[i].doc);
            System.out.printf("Expected score: %f | Actual score: %f\n", expectedTopDocs.scoreDocs[i].score, actualTopDocs.scoreDocs[i].score);
          }
          assertEquals(expectedTopDocs.scoreDocs[i].doc, actualTopDocs.scoreDocs[i].doc);
          assertEquals(expectedTopDocs.scoreDocs[i].score, actualTopDocs.scoreDocs[i].score, 0.0f);
          Explanation explanation = indexSearcher.explain(joinQuery, expectedTopDocs.scoreDocs[i].doc);
          assertEquals(expectedTopDocs.scoreDocs[i].score, explanation.getValue(), 0.0f);
        }
      }
      topLevelReader.close();
      dir.close();
    }
  }

  private IndexIterationContext createContext(int nDocs, RandomIndexWriter writer, boolean multipleValuesPerDocument, boolean scoreDocsInOrder) throws IOException {
    return createContext(nDocs, writer, writer, multipleValuesPerDocument, scoreDocsInOrder);
  }

  private IndexIterationContext createContext(int nDocs, RandomIndexWriter fromWriter, RandomIndexWriter toWriter, boolean multipleValuesPerDocument, boolean scoreDocsInOrder) throws IOException {
    IndexIterationContext context = new IndexIterationContext();
    int numRandomValues = nDocs / 2;
    context.randomUniqueValues = new String[numRandomValues];
    Set<String> trackSet = new HashSet<>();
    context.randomFrom = new boolean[numRandomValues];
    for (int i = 0; i < numRandomValues; i++) {
      String uniqueRandomValue;
      do {
        uniqueRandomValue = TestUtil.randomRealisticUnicodeString(random());
//        uniqueRandomValue = _TestUtil.randomSimpleString(random);
      } while ("".equals(uniqueRandomValue) || trackSet.contains(uniqueRandomValue));
      // Generate unique values and empty strings aren't allowed.
      trackSet.add(uniqueRandomValue);
      context.randomFrom[i] = random().nextBoolean();
      context.randomUniqueValues[i] = uniqueRandomValue;
    }

    RandomDoc[] docs = new RandomDoc[nDocs];
    for (int i = 0; i < nDocs; i++) {
      String id = Integer.toString(i);
      int randomI = random().nextInt(context.randomUniqueValues.length);
      String value = context.randomUniqueValues[randomI];
      Document document = new Document();
      document.add(newTextField(random(), "id", id, Field.Store.NO));
      document.add(newTextField(random(), "value", value, Field.Store.NO));

      boolean from = context.randomFrom[randomI];
      int numberOfLinkValues = multipleValuesPerDocument ? 2 + random().nextInt(10) : 1;
      docs[i] = new RandomDoc(id, numberOfLinkValues, value, from);
      for (int j = 0; j < numberOfLinkValues; j++) {
        String linkValue = context.randomUniqueValues[random().nextInt(context.randomUniqueValues.length)];
        docs[i].linkValues.add(linkValue);
        if (from) {
          if (!context.fromDocuments.containsKey(linkValue)) {
            context.fromDocuments.put(linkValue, new ArrayList<RandomDoc>());
          }
          if (!context.randomValueFromDocs.containsKey(value)) {
            context.randomValueFromDocs.put(value, new ArrayList<RandomDoc>());
          }

          context.fromDocuments.get(linkValue).add(docs[i]);
          context.randomValueFromDocs.get(value).add(docs[i]);
          document.add(newTextField(random(), "from", linkValue, Field.Store.NO));
        } else {
          if (!context.toDocuments.containsKey(linkValue)) {
            context.toDocuments.put(linkValue, new ArrayList<RandomDoc>());
          }
          if (!context.randomValueToDocs.containsKey(value)) {
            context.randomValueToDocs.put(value, new ArrayList<RandomDoc>());
          }

          context.toDocuments.get(linkValue).add(docs[i]);
          context.randomValueToDocs.get(value).add(docs[i]);
          document.add(newTextField(random(), "to", linkValue, Field.Store.NO));
        }
      }

      final RandomIndexWriter w;
      if (from) {
        w = fromWriter;
      } else {
        w = toWriter;
      }

      w.addDocument(document);
      if (random().nextInt(10) == 4) {
        w.commit();
      }
      if (VERBOSE) {
        System.out.println("Added document[" + docs[i].id + "]: " + document);
      }
    }

    // Pre-compute all possible hits for all unique random values. On top of this also compute all possible score for
    // any ScoreMode.
    IndexSearcher fromSearcher = newSearcher(fromWriter.getReader());
    IndexSearcher toSearcher = newSearcher(toWriter.getReader());
    for (int i = 0; i < context.randomUniqueValues.length; i++) {
      String uniqueRandomValue = context.randomUniqueValues[i];
      final String fromField;
      final String toField;
      final Map<String, Map<Integer, JoinScore>> queryVals;
      if (context.randomFrom[i]) {
        fromField = "from";
        toField = "to";
        queryVals = context.fromHitsToJoinScore;
      } else {
        fromField = "to";
        toField = "from";
        queryVals = context.toHitsToJoinScore;
      }
      final Map<BytesRef, JoinScore> joinValueToJoinScores = new HashMap<>();
      if (multipleValuesPerDocument) {
        fromSearcher.search(new TermQuery(new Term("value", uniqueRandomValue)), new Collector() {

          private Scorer scorer;
          private SortedSetDocValues docTermOrds;
          final BytesRef joinValue = new BytesRef();

          @Override
          public void collect(int doc) throws IOException {
            docTermOrds.setDocument(doc);
            long ord;
            while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              docTermOrds.lookupOrd(ord, joinValue);
              JoinScore joinScore = joinValueToJoinScores.get(joinValue);
              if (joinScore == null) {
                joinValueToJoinScores.put(BytesRef.deepCopyOf(joinValue), joinScore = new JoinScore());
              }
              joinScore.addScore(scorer.score());
            }
          }

          @Override
          public void setNextReader(AtomicReaderContext context) throws IOException {
            docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), fromField);
          }

          @Override
          public void setScorer(Scorer scorer) {
            this.scorer = scorer;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return false;
          }
        });
      } else {
        fromSearcher.search(new TermQuery(new Term("value", uniqueRandomValue)), new Collector() {

          private Scorer scorer;
          private BinaryDocValues terms;
          private Bits docsWithField;
          private final BytesRef spare = new BytesRef();

          @Override
          public void collect(int doc) throws IOException {
            terms.get(doc, spare);
            BytesRef joinValue = spare;
            if (joinValue.length == 0 && !docsWithField.get(doc)) {
              return;
            }

            JoinScore joinScore = joinValueToJoinScores.get(joinValue);
            if (joinScore == null) {
              joinValueToJoinScores.put(BytesRef.deepCopyOf(joinValue), joinScore = new JoinScore());
            }
            joinScore.addScore(scorer.score());
          }

          @Override
          public void setNextReader(AtomicReaderContext context) throws IOException {
            terms = FieldCache.DEFAULT.getTerms(context.reader(), fromField, true);
            docsWithField = FieldCache.DEFAULT.getDocsWithField(context.reader(), fromField);
          }

          @Override
          public void setScorer(Scorer scorer) {
            this.scorer = scorer;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {
            return false;
          }
        });
      }

      final Map<Integer, JoinScore> docToJoinScore = new HashMap<>();
      if (multipleValuesPerDocument) {
        if (scoreDocsInOrder) {
          AtomicReader slowCompositeReader = SlowCompositeReaderWrapper.wrap(toSearcher.getIndexReader());
          Terms terms = slowCompositeReader.terms(toField);
          if (terms != null) {
            DocsEnum docsEnum = null;
            TermsEnum termsEnum = null;
            SortedSet<BytesRef> joinValues = new TreeSet<>(BytesRef.getUTF8SortedAsUnicodeComparator());
            joinValues.addAll(joinValueToJoinScores.keySet());
            for (BytesRef joinValue : joinValues) {
              termsEnum = terms.iterator(termsEnum);
              if (termsEnum.seekExact(joinValue)) {
                docsEnum = termsEnum.docs(slowCompositeReader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                JoinScore joinScore = joinValueToJoinScores.get(joinValue);

                for (int doc = docsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docsEnum.nextDoc()) {
                  // First encountered join value determines the score.
                  // Something to keep in mind for many-to-many relations.
                  if (!docToJoinScore.containsKey(doc)) {
                    docToJoinScore.put(doc, joinScore);
                  }
                }
              }
            }
          }
        } else {
          toSearcher.search(new MatchAllDocsQuery(), new Collector() {

            private SortedSetDocValues docTermOrds;
            private final BytesRef scratch = new BytesRef();
            private int docBase;

            @Override
            public void collect(int doc) throws IOException {
              docTermOrds.setDocument(doc);
              long ord;
              while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                docTermOrds.lookupOrd(ord, scratch);
                JoinScore joinScore = joinValueToJoinScores.get(scratch);
                if (joinScore == null) {
                  continue;
                }
                Integer basedDoc = docBase + doc;
                // First encountered join value determines the score.
                // Something to keep in mind for many-to-many relations.
                if (!docToJoinScore.containsKey(basedDoc)) {
                  docToJoinScore.put(basedDoc, joinScore);
                }
              }
            }

            @Override
            public void setNextReader(AtomicReaderContext context) throws IOException {
              docBase = context.docBase;
              docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), toField);
            }

            @Override
            public boolean acceptsDocsOutOfOrder() {return false;}
            @Override
            public void setScorer(Scorer scorer) {}
          });
        }
      } else {
        toSearcher.search(new MatchAllDocsQuery(), new Collector() {

          private BinaryDocValues terms;
          private int docBase;
          private final BytesRef spare = new BytesRef();

          @Override
          public void collect(int doc) {
            terms.get(doc, spare);
            JoinScore joinScore = joinValueToJoinScores.get(spare);
            if (joinScore == null) {
              return;
            }
            docToJoinScore.put(docBase + doc, joinScore);
          }

          @Override
          public void setNextReader(AtomicReaderContext context) throws IOException {
            terms = FieldCache.DEFAULT.getTerms(context.reader(), toField, false);
            docBase = context.docBase;
          }

          @Override
          public boolean acceptsDocsOutOfOrder() {return false;}
          @Override
          public void setScorer(Scorer scorer) {}
        });
      }
      queryVals.put(uniqueRandomValue, docToJoinScore);
    }

    fromSearcher.getIndexReader().close();
    toSearcher.getIndexReader().close();

    return context;
  }

  private TopDocs createExpectedTopDocs(String queryValue,
                                        final boolean from,
                                        final ScoreMode scoreMode,
                                        IndexIterationContext context) {

    Map<Integer, JoinScore> hitsToJoinScores;
    if (from) {
      hitsToJoinScores = context.fromHitsToJoinScore.get(queryValue);
    } else {
      hitsToJoinScores = context.toHitsToJoinScore.get(queryValue);
    }
    List<Map.Entry<Integer,JoinScore>> hits = new ArrayList<>(hitsToJoinScores.entrySet());
    Collections.sort(hits, new Comparator<Map.Entry<Integer, JoinScore>>() {

      @Override
      public int compare(Map.Entry<Integer, JoinScore> hit1, Map.Entry<Integer, JoinScore> hit2) {
        float score1 = hit1.getValue().score(scoreMode);
        float score2 = hit2.getValue().score(scoreMode);

        int cmp = Float.compare(score2, score1);
        if (cmp != 0) {
          return cmp;
        }
        return hit1.getKey() - hit2.getKey();
      }

    });
    ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(10, hits.size())];
    for (int i = 0; i < scoreDocs.length; i++) {
      Map.Entry<Integer,JoinScore> hit = hits.get(i);
      scoreDocs[i] = new ScoreDoc(hit.getKey(), hit.getValue().score(scoreMode));
    }
    return new TopDocs(hits.size(), scoreDocs, hits.isEmpty() ? Float.NaN : hits.get(0).getValue().score(scoreMode));
  }

  private FixedBitSet createExpectedResult(String queryValue, boolean from, IndexReader topLevelReader, IndexIterationContext context) throws IOException {
    final Map<String, List<RandomDoc>> randomValueDocs;
    final Map<String, List<RandomDoc>> linkValueDocuments;
    if (from) {
      randomValueDocs = context.randomValueFromDocs;
      linkValueDocuments = context.toDocuments;
    } else {
      randomValueDocs = context.randomValueToDocs;
      linkValueDocuments = context.fromDocuments;
    }

    FixedBitSet expectedResult = new FixedBitSet(topLevelReader.maxDoc());
    List<RandomDoc> matchingDocs = randomValueDocs.get(queryValue);
    if (matchingDocs == null) {
      return new FixedBitSet(topLevelReader.maxDoc());
    }

    for (RandomDoc matchingDoc : matchingDocs) {
      for (String linkValue : matchingDoc.linkValues) {
        List<RandomDoc> otherMatchingDocs = linkValueDocuments.get(linkValue);
        if (otherMatchingDocs == null) {
          continue;
        }

        for (RandomDoc otherSideDoc : otherMatchingDocs) {
          DocsEnum docsEnum = MultiFields.getTermDocsEnum(topLevelReader, MultiFields.getLiveDocs(topLevelReader), "id", new BytesRef(otherSideDoc.id), 0);
          assert docsEnum != null;
          int doc = docsEnum.nextDoc();
          expectedResult.set(doc);
        }
      }
    }
    return expectedResult;
  }

  private static class IndexIterationContext {

    String[] randomUniqueValues;
    boolean[] randomFrom;
    Map<String, List<RandomDoc>> fromDocuments = new HashMap<>();
    Map<String, List<RandomDoc>> toDocuments = new HashMap<>();
    Map<String, List<RandomDoc>> randomValueFromDocs = new HashMap<>();
    Map<String, List<RandomDoc>> randomValueToDocs = new HashMap<>();

    Map<String, Map<Integer, JoinScore>> fromHitsToJoinScore = new HashMap<>();
    Map<String, Map<Integer, JoinScore>> toHitsToJoinScore = new HashMap<>();

  }

  private static class RandomDoc {

    final String id;
    final List<String> linkValues;
    final String value;
    final boolean from;

    private RandomDoc(String id, int numberOfLinkValues, String value, boolean from) {
      this.id = id;
      this.from = from;
      linkValues = new ArrayList<>(numberOfLinkValues);
      this.value = value;
    }
  }

  private static class JoinScore {

    float maxScore;
    float total;
    int count;

    void addScore(float score) {
      total += score;
      if (score > maxScore) {
        maxScore = score;
      }
      count++;
    }

    float score(ScoreMode mode) {
      switch (mode) {
        case None:
          return 1.0f;
        case Total:
          return total;
        case Avg:
          return total / count;
        case Max:
          return maxScore;
      }
      throw new IllegalArgumentException("Unsupported ScoreMode: " + mode);
    }

  }
*/

  private SolrQueryRequest dump(SolrQueryRequest req) throws Exception {
    System.out.println(req);
    System.out.println(h.query(req));
    return req;
  }
}
