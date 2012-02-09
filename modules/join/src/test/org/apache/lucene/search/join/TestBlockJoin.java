package org.apache.lucene.search.join;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util._TestUtil;

public class TestBlockJoin extends LuceneTestCase {

  // One resume...
  private Document makeResume(String name, String country) {
    Document resume = new Document();
    resume.add(newField("docType", "resume", StringField.TYPE_UNSTORED));
    resume.add(newField("name", name, StringField.TYPE_STORED));
    resume.add(newField("country", country, StringField.TYPE_UNSTORED));
    return resume;
  }

  // ... has multiple jobs
  private Document makeJob(String skill, int year) {
    Document job = new Document();
    job.add(newField("skill", skill, StringField.TYPE_STORED));
    job.add(new NumericField("year", year));
    job.add(new StoredField("year", year));
    return job;
  }

  // ... has multiple qualifications
  private Document makeQualification(String qualification, int year) {
    Document job = new Document();
    job.add(newField("qualification", qualification, StringField.TYPE_STORED));
    job.add(new NumericField("year", year));
    return job;
  }

  public void testSimple() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);

    final List<Document> docs = new ArrayList<Document>();

    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    docs.clear();
    docs.add(makeJob("ruby", 2005));
    docs.add(makeJob("java", 2006));
    docs.add(makeResume("Frank", "United States"));
    w.addDocuments(docs);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    Filter parentsFilter = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("docType", "resume"))));

    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery childQuery = new BooleanQuery();
    childQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childQuery.add(new BooleanClause(NumericRangeQuery.newIntRange("year", 2006, 2011, true, true), Occur.MUST));

    // Define parent document criteria (find a resident in the UK)
    Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery, parentsFilter, ToParentBlockJoinQuery.ScoreMode.Avg);

    // Combine the parent and nested child queries into a single query for a candidate
    BooleanQuery fullQuery = new BooleanQuery();
    fullQuery.add(new BooleanClause(parentQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(childJoinQuery, Occur.MUST));

    ToParentBlockJoinCollector c = new ToParentBlockJoinCollector(Sort.RELEVANCE, 1, true, false);

    s.search(fullQuery, c);
    
    TopGroups<Integer> results = c.getTopGroups(childJoinQuery, null, 0, 10, 0, true);

    //assertEquals(1, results.totalHitCount);
    assertEquals(1, results.totalGroupedHitCount);
    assertEquals(1, results.groups.length);

    final GroupDocs<Integer> group = results.groups[0];
    assertEquals(1, group.totalHits);

    Document childDoc = s.doc(group.scoreDocs[0].doc);
    //System.out.println("  doc=" + group.scoreDocs[0].doc);
    assertEquals("java", childDoc.get("skill"));
    assertNotNull(group.groupValue);
    Document parentDoc = s.doc(group.groupValue);
    assertEquals("Lisa", parentDoc.get("name"));


    //System.out.println("TEST: now test up");

    // Now join "up" (map parent hits to child docs) instead...:
    ToChildBlockJoinQuery parentJoinQuery = new ToChildBlockJoinQuery(parentQuery, parentsFilter, random.nextBoolean());
    BooleanQuery fullChildQuery = new BooleanQuery();
    fullChildQuery.add(new BooleanClause(parentJoinQuery, Occur.MUST));
    fullChildQuery.add(new BooleanClause(childQuery, Occur.MUST));
    
    //System.out.println("FULL: " + fullChildQuery);
    TopDocs hits = s.search(fullChildQuery, 10);
    assertEquals(1, hits.totalHits);
    childDoc = s.doc(hits.scoreDocs[0].doc);
    //System.out.println("CHILD = " + childDoc + " docID=" + hits.scoreDocs[0].doc);
    assertEquals("java", childDoc.get("skill"));
    assertEquals(2007, ((StoredField) childDoc.getField("year")).numericValue());
    assertEquals("Lisa", getParentDoc(r, parentsFilter, hits.scoreDocs[0].doc).get("name"));
    r.close();
    dir.close();
  }

  private Document getParentDoc(IndexReader reader, Filter parents, int childDocID) throws IOException {
    final AtomicReaderContext[] leaves = reader.getTopReaderContext().leaves();
    final int subIndex = ReaderUtil.subIndex(childDocID, leaves);
    final AtomicReaderContext leaf = leaves[subIndex];
    final FixedBitSet bits = (FixedBitSet) parents.getDocIdSet(leaf, null);
    return leaf.reader().document(bits.nextSetBit(childDocID - leaf.docBase));
  }
  
  public void testBoostBug() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    
    ToParentBlockJoinQuery q = new ToParentBlockJoinQuery(new MatchAllDocsQuery(), new QueryWrapperFilter(new MatchAllDocsQuery()), ToParentBlockJoinQuery.ScoreMode.Avg);
    s.search(q, 10);
    BooleanQuery bq = new BooleanQuery();
    bq.setBoost(2f); // we boost the BQ
    bq.add(q, BooleanClause.Occur.MUST);
    s.search(bq, 10);
    r.close();
    dir.close();
  }

  private String[][] getRandomFields(int maxUniqueValues) {

    final String[][] fields = new String[_TestUtil.nextInt(random, 2, 4)][];
    for(int fieldID=0;fieldID<fields.length;fieldID++) {
      final int valueCount;
      if (fieldID == 0) {
        valueCount = 2;
      } else {
        valueCount = _TestUtil.nextInt(random, 1, maxUniqueValues);
      }
        
      final String[] values = fields[fieldID] = new String[valueCount];
      for(int i=0;i<valueCount;i++) {
        values[i] = _TestUtil.randomRealisticUnicodeString(random);
        //values[i] = _TestUtil.randomSimpleString(random);
      }
    }

    return fields;
  }

  private Term randomParentTerm(String[] values) {
    return new Term("parent0", values[random.nextInt(values.length)]);
  }

  private Term randomChildTerm(String[] values) {
    return new Term("child0", values[random.nextInt(values.length)]);
  }

  private Sort getRandomSort(String prefix, int numFields) {
    final List<SortField> sortFields = new ArrayList<SortField>();
    // TODO: sometimes sort by score; problem is scores are
    // not comparable across the two indices
    // sortFields.add(SortField.FIELD_SCORE);
    if (random.nextBoolean()) {
      sortFields.add(new SortField(prefix + random.nextInt(numFields), SortField.Type.STRING, random.nextBoolean()));
    } else if (random.nextBoolean()) {
      sortFields.add(new SortField(prefix + random.nextInt(numFields), SortField.Type.STRING, random.nextBoolean()));
      sortFields.add(new SortField(prefix + random.nextInt(numFields), SortField.Type.STRING, random.nextBoolean()));
    }
    // Break ties:
    sortFields.add(new SortField(prefix + "ID", SortField.Type.INT));
    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  public void testRandom() throws Exception {
    // We build two indices at once: one normalized (which
    // ToParentBlockJoinQuery/Collector,
    // ToChildBlockJoinQuery can query) and the other w/
    // the same docs, just fully denormalized:
    final Directory dir = newDirectory();
    final Directory joinDir = newDirectory();

    final int numParentDocs = _TestUtil.nextInt(random, 100*RANDOM_MULTIPLIER, 300*RANDOM_MULTIPLIER);
    //final int numParentDocs = 30;

    // Values for parent fields:
    final String[][] parentFields = getRandomFields(numParentDocs/2);
    // Values for child fields:
    final String[][] childFields = getRandomFields(numParentDocs);

    // TODO: parallel star join, nested join cases too!
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);
    final RandomIndexWriter joinW = new RandomIndexWriter(random, joinDir);
    for(int parentDocID=0;parentDocID<numParentDocs;parentDocID++) {
      Document parentDoc = new Document();
      Document parentJoinDoc = new Document();
      Field id = newField("parentID", ""+parentDocID, StringField.TYPE_STORED);
      parentDoc.add(id);
      parentJoinDoc.add(id);
      parentJoinDoc.add(newField("isParent", "x", StringField.TYPE_UNSTORED));
      for(int field=0;field<parentFields.length;field++) {
        if (random.nextDouble() < 0.9) {
          Field f = newField("parent" + field,
                             parentFields[field][random.nextInt(parentFields[field].length)],
                             StringField.TYPE_UNSTORED);
          parentDoc.add(f);
          parentJoinDoc.add(f);
        }
      }

      final List<Document> joinDocs = new ArrayList<Document>();

      if (VERBOSE) {
        StringBuilder sb = new StringBuilder();
        sb.append("parentID=" + parentDoc.get("parentID"));
        for(int fieldID=0;fieldID<parentFields.length;fieldID++) {
          String s = parentDoc.get("parent" + fieldID);
          if (s != null) {
            sb.append(" parent" + fieldID + "=" + s);
          }
        }
        System.out.println("  " + sb.toString());
      }

      final int numChildDocs = _TestUtil.nextInt(random, 1, 20);
      for(int childDocID=0;childDocID<numChildDocs;childDocID++) {
        // Denormalize: copy all parent fields into child doc:
        Document childDoc = _TestUtil.cloneDocument(parentDoc);
        Document joinChildDoc = new Document();
        joinDocs.add(joinChildDoc);

        Field childID = newField("childID", ""+childDocID, StringField.TYPE_STORED);
        childDoc.add(childID);
        joinChildDoc.add(childID);

        for(int childFieldID=0;childFieldID<childFields.length;childFieldID++) {
          if (random.nextDouble() < 0.9) {
            Field f = newField("child" + childFieldID,
                               childFields[childFieldID][random.nextInt(childFields[childFieldID].length)],
                               StringField.TYPE_UNSTORED);
            childDoc.add(f);
            joinChildDoc.add(f);
          }
        }

        if (VERBOSE) {
          StringBuilder sb = new StringBuilder();
          sb.append("childID=" + joinChildDoc.get("childID"));
          for(int fieldID=0;fieldID<childFields.length;fieldID++) {
            String s = joinChildDoc.get("child" + fieldID);
            if (s != null) {
              sb.append(" child" + fieldID + "=" + s);
            }
          }
          System.out.println("    " + sb.toString());
        }

        w.addDocument(childDoc);
      }

      // Parent last:
      joinDocs.add(parentJoinDoc);
      joinW.addDocuments(joinDocs);
    }

    final IndexReader r = w.getReader();
    w.close();
    final IndexReader joinR = joinW.getReader();
    joinW.close();

    if (VERBOSE) {
      System.out.println("TEST: reader=" + r);
      System.out.println("TEST: joinReader=" + joinR);

      for(int docIDX=0;docIDX<joinR.maxDoc();docIDX++) {
        System.out.println("  docID=" + docIDX + " doc=" + joinR.document(docIDX));
      }
    }

    final IndexSearcher s = newSearcher(r);
    s.setDefaultFieldSortScoring(true, true);

    final IndexSearcher joinS = newSearcher(joinR);

    final Filter parentsFilter = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("isParent", "x"))));

    final int iters = 200*RANDOM_MULTIPLIER;

    for(int iter=0;iter<iters;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + (1+iter) + " of " + iters);
      }

      final Query childQuery;
      if (random.nextInt(3) == 2) {
        final int childFieldID = random.nextInt(childFields.length);
        childQuery = new TermQuery(new Term("child" + childFieldID,
                                            childFields[childFieldID][random.nextInt(childFields[childFieldID].length)]));
      } else if (random.nextInt(3) == 2) {
        BooleanQuery bq = new BooleanQuery();
        childQuery = bq;
        final int numClauses = _TestUtil.nextInt(random, 2, 4);
        boolean didMust = false;
        for(int clauseIDX=0;clauseIDX<numClauses;clauseIDX++) {
          Query clause;
          BooleanClause.Occur occur;
          if (!didMust && random.nextBoolean()) {
            occur = random.nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT;
            clause = new TermQuery(randomChildTerm(childFields[0]));
            didMust = true;
          } else {
            occur = BooleanClause.Occur.SHOULD;
            final int childFieldID = _TestUtil.nextInt(random, 1, childFields.length-1);
            clause = new TermQuery(new Term("child" + childFieldID,
                                            childFields[childFieldID][random.nextInt(childFields[childFieldID].length)]));
          }
          bq.add(clause, occur);
        }
      } else {
        BooleanQuery bq = new BooleanQuery();
        childQuery = bq;
        
        bq.add(new TermQuery(randomChildTerm(childFields[0])),
               BooleanClause.Occur.MUST);
        final int childFieldID = _TestUtil.nextInt(random, 1, childFields.length-1);
        bq.add(new TermQuery(new Term("child" + childFieldID, childFields[childFieldID][random.nextInt(childFields[childFieldID].length)])),
               random.nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT);
      }

      final int x = random.nextInt(4);
      final ToParentBlockJoinQuery.ScoreMode agg;
      if (x == 0) {
        agg = ToParentBlockJoinQuery.ScoreMode.None;
      } else if (x == 1) {
        agg = ToParentBlockJoinQuery.ScoreMode.Max;
      } else if (x == 2) {
        agg = ToParentBlockJoinQuery.ScoreMode.Total;
      } else {
        agg = ToParentBlockJoinQuery.ScoreMode.Avg;
      }

      final ToParentBlockJoinQuery childJoinQuery = new ToParentBlockJoinQuery(childQuery, parentsFilter, agg);

      // To run against the block-join index:
      final Query parentJoinQuery;

      // Same query as parentJoinQuery, but to run against
      // the fully denormalized index (so we can compare
      // results):
      final Query parentQuery;

      if (random.nextBoolean()) {
        parentQuery = childQuery;
        parentJoinQuery = childJoinQuery;
      } else {
        // AND parent field w/ child field
        final BooleanQuery bq = new BooleanQuery();
        parentJoinQuery = bq;
        final Term parentTerm = randomParentTerm(parentFields[0]);
        if (random.nextBoolean()) {
          bq.add(childJoinQuery, BooleanClause.Occur.MUST);
          bq.add(new TermQuery(parentTerm),
                 BooleanClause.Occur.MUST);
        } else {
          bq.add(new TermQuery(parentTerm),
                 BooleanClause.Occur.MUST);
          bq.add(childJoinQuery, BooleanClause.Occur.MUST);
        }

        final BooleanQuery bq2 = new BooleanQuery();
        parentQuery = bq2;
        if (random.nextBoolean()) {
          bq2.add(childQuery, BooleanClause.Occur.MUST);
          bq2.add(new TermQuery(parentTerm),
                  BooleanClause.Occur.MUST);
        } else {
          bq2.add(new TermQuery(parentTerm),
                  BooleanClause.Occur.MUST);
          bq2.add(childQuery, BooleanClause.Occur.MUST);
        }
      }

      final Sort parentSort = getRandomSort("parent", parentFields.length);
      final Sort childSort = getRandomSort("child", childFields.length);

      if (VERBOSE) {
        System.out.println("\nTEST: query=" + parentQuery + " joinQuery=" + parentJoinQuery + " parentSort=" + parentSort + " childSort=" + childSort);
      }

      // Merge both sorts:
      final List<SortField> sortFields = new ArrayList<SortField>(Arrays.asList(parentSort.getSort()));
      sortFields.addAll(Arrays.asList(childSort.getSort()));
      final Sort parentAndChildSort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));

      final TopDocs results = s.search(parentQuery, null, r.numDocs(),
                                       parentAndChildSort);

      if (VERBOSE) {
        System.out.println("\nTEST: normal index gets " + results.totalHits + " hits");
        final ScoreDoc[] hits = results.scoreDocs;
        for(int hitIDX=0;hitIDX<hits.length;hitIDX++) {
          final Document doc = s.doc(hits[hitIDX].doc);
          //System.out.println("  score=" + hits[hitIDX].score + " parentID=" + doc.get("parentID") + " childID=" + doc.get("childID") + " (docID=" + hits[hitIDX].doc + ")");
          System.out.println("  parentID=" + doc.get("parentID") + " childID=" + doc.get("childID") + " (docID=" + hits[hitIDX].doc + ")");
          FieldDoc fd = (FieldDoc) hits[hitIDX];
          if (fd.fields != null) {
            System.out.print("    ");
            for(Object o : fd.fields) {
              if (o instanceof BytesRef) {
                System.out.print(((BytesRef) o).utf8ToString() + " ");
              } else {
                System.out.print(o + " ");
              }
            }
            System.out.println();
          }
        }
      }

      final boolean trackScores;
      final boolean trackMaxScore;
      if (agg == ToParentBlockJoinQuery.ScoreMode.None) {
        trackScores = false;
        trackMaxScore = false;
      } else {
        trackScores = random.nextBoolean();
        trackMaxScore = random.nextBoolean();
      }
      final ToParentBlockJoinCollector c = new ToParentBlockJoinCollector(parentSort, 10, trackScores, trackMaxScore);

      joinS.search(parentJoinQuery, c);

      final int hitsPerGroup = _TestUtil.nextInt(random, 1, 20);
      //final int hitsPerGroup = 100;
      final TopGroups<Integer> joinResults = c.getTopGroups(childJoinQuery, childSort, 0, hitsPerGroup, 0, true);

      if (VERBOSE) {
        System.out.println("\nTEST: block join index gets " + (joinResults == null ? 0 : joinResults.groups.length) + " groups; hitsPerGroup=" + hitsPerGroup);
        if (joinResults != null) {
          final GroupDocs<Integer>[] groups = joinResults.groups;
          for(int groupIDX=0;groupIDX<groups.length;groupIDX++) {
            final GroupDocs<Integer> group = groups[groupIDX];
            if (group.groupSortValues != null) {
              System.out.print("  ");
              for(Object o : group.groupSortValues) {
                if (o instanceof BytesRef) {
                  System.out.print(((BytesRef) o).utf8ToString() + " ");
                } else {
                  System.out.print(o + " ");
                }
              }
              System.out.println();
            }

            assertNotNull(group.groupValue);
            final Document parentDoc = joinS.doc(group.groupValue);
            System.out.println("  group parentID=" + parentDoc.get("parentID") + " (docID=" + group.groupValue + ")");
            for(int hitIDX=0;hitIDX<group.scoreDocs.length;hitIDX++) {
              final Document doc = joinS.doc(group.scoreDocs[hitIDX].doc);
              //System.out.println("    score=" + group.scoreDocs[hitIDX].score + " childID=" + doc.get("childID") + " (docID=" + group.scoreDocs[hitIDX].doc + ")");
              System.out.println("    childID=" + doc.get("childID") + " child0=" + doc.get("child0") + " (docID=" + group.scoreDocs[hitIDX].doc + ")");
            }
          }
        }
      }

      if (results.totalHits == 0) {
        assertNull(joinResults);
      } else {
        compareHits(r, joinR, results, joinResults);
      }

      // Test joining in the opposite direction (parent to
      // child):

      // Get random query against parent documents:
      final Query parentQuery2;
      if (random.nextInt(3) == 2) {
        final int fieldID = random.nextInt(parentFields.length);
        parentQuery2 = new TermQuery(new Term("parent" + fieldID,
                                              parentFields[fieldID][random.nextInt(parentFields[fieldID].length)]));
      } else if (random.nextInt(3) == 2) {
        BooleanQuery bq = new BooleanQuery();
        parentQuery2 = bq;
        final int numClauses = _TestUtil.nextInt(random, 2, 4);
        boolean didMust = false;
        for(int clauseIDX=0;clauseIDX<numClauses;clauseIDX++) {
          Query clause;
          BooleanClause.Occur occur;
          if (!didMust && random.nextBoolean()) {
            occur = random.nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT;
            clause = new TermQuery(randomParentTerm(parentFields[0]));
            didMust = true;
          } else {
            occur = BooleanClause.Occur.SHOULD;
            final int fieldID = _TestUtil.nextInt(random, 1, parentFields.length-1);
            clause = new TermQuery(new Term("parent" + fieldID,
                                            parentFields[fieldID][random.nextInt(parentFields[fieldID].length)]));
          }
          bq.add(clause, occur);
        }
      } else {
        BooleanQuery bq = new BooleanQuery();
        parentQuery2 = bq;
        
        bq.add(new TermQuery(randomParentTerm(parentFields[0])),
               BooleanClause.Occur.MUST);
        final int fieldID = _TestUtil.nextInt(random, 1, parentFields.length-1);
        bq.add(new TermQuery(new Term("parent" + fieldID, parentFields[fieldID][random.nextInt(parentFields[fieldID].length)])),
               random.nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.MUST_NOT);
      }

      if (VERBOSE) {
        System.out.println("\nTEST: top down: parentQuery2=" + parentQuery2);
      }

      // Maps parent query to child docs:
      final ToChildBlockJoinQuery parentJoinQuery2 = new ToChildBlockJoinQuery(parentQuery2, parentsFilter, random.nextBoolean());

      // To run against the block-join index:
      final Query childJoinQuery2;

      // Same query as parentJoinQuery, but to run against
      // the fully denormalized index (so we can compare
      // results):
      final Query childQuery2;

      if (random.nextBoolean()) {
        childQuery2 = parentQuery2;
        childJoinQuery2 = parentJoinQuery2;
      } else {
        // AND child field w/ parent query:
        final BooleanQuery bq = new BooleanQuery();
        childJoinQuery2 = bq;
        final Term childTerm = randomChildTerm(childFields[0]);
        if (random.nextBoolean()) {
          bq.add(parentJoinQuery2, BooleanClause.Occur.MUST);
          bq.add(new TermQuery(childTerm),
                 BooleanClause.Occur.MUST);
        } else {
          bq.add(new TermQuery(childTerm),
                 BooleanClause.Occur.MUST);
          bq.add(parentJoinQuery2, BooleanClause.Occur.MUST);
        }

        final BooleanQuery bq2 = new BooleanQuery();
        childQuery2 = bq2;
        if (random.nextBoolean()) {
          bq2.add(parentQuery2, BooleanClause.Occur.MUST);
          bq2.add(new TermQuery(childTerm),
                  BooleanClause.Occur.MUST);
        } else {
          bq2.add(new TermQuery(childTerm),
                  BooleanClause.Occur.MUST);
          bq2.add(parentQuery2, BooleanClause.Occur.MUST);
        }
      }

      final Sort childSort2 = getRandomSort("child", childFields.length);

      // Search denormalized index:
      if (VERBOSE) {
        System.out.println("TEST: run top down query=" + childQuery2 + " sort=" + childSort2);
      }
      final TopDocs results2 = s.search(childQuery2, null, r.numDocs(),
                                        childSort2);
      if (VERBOSE) {
        System.out.println("  " + results2.totalHits + " totalHits:");
        for(ScoreDoc sd : results2.scoreDocs) {
          final Document doc = s.doc(sd.doc);
          System.out.println("  childID=" + doc.get("childID") + " parentID=" + doc.get("parentID") + " docID=" + sd.doc);
        }
      }

      // Search join index:
      if (VERBOSE) {
        System.out.println("TEST: run top down join query=" + childJoinQuery2 + " sort=" + childSort2);
      }
      TopDocs joinResults2 = joinS.search(childJoinQuery2, null, joinR.numDocs(), childSort2);
      if (VERBOSE) {
        System.out.println("  " + joinResults2.totalHits + " totalHits:");
        for(ScoreDoc sd : joinResults2.scoreDocs) {
          final Document doc = joinS.doc(sd.doc);
          final Document parentDoc = getParentDoc(joinR, parentsFilter, sd.doc);
          System.out.println("  childID=" + doc.get("childID") + " parentID=" + parentDoc.get("parentID") + " docID=" + sd.doc);
        }
      }

      compareChildHits(r, joinR, results2, joinResults2);
    }

    r.close();
    joinR.close();
    dir.close();
    joinDir.close();
  }

  private void compareChildHits(IndexReader r, IndexReader joinR, TopDocs results, TopDocs joinResults) throws Exception {
    assertEquals(results.totalHits, joinResults.totalHits);
    assertEquals(results.scoreDocs.length, joinResults.scoreDocs.length);
    for(int hitCount=0;hitCount<results.scoreDocs.length;hitCount++) {
      ScoreDoc hit = results.scoreDocs[hitCount];
      ScoreDoc joinHit = joinResults.scoreDocs[hitCount];
      Document doc1 = r.document(hit.doc);
      Document doc2 = joinR.document(joinHit.doc);
      assertEquals("hit " + hitCount + " differs",
                   doc1.get("childID"), doc2.get("childID"));
      // don't compare scores -- they are expected to differ


      assertTrue(hit instanceof FieldDoc);
      assertTrue(joinHit instanceof FieldDoc);

      FieldDoc hit0 = (FieldDoc) hit;
      FieldDoc joinHit0 = (FieldDoc) joinHit;
      assertEquals(hit0.fields, joinHit0.fields);
    }
  }

  private void compareHits(IndexReader r, IndexReader joinR, TopDocs results, TopGroups<Integer> joinResults) throws Exception {
    // results is 'complete'; joinResults is a subset
    int resultUpto = 0;
    int joinGroupUpto = 0;

    final ScoreDoc[] hits = results.scoreDocs;
    final GroupDocs<Integer>[] groupDocs = joinResults.groups;

    while(joinGroupUpto < groupDocs.length) {
      final GroupDocs<Integer> group = groupDocs[joinGroupUpto++];
      final ScoreDoc[] groupHits = group.scoreDocs;
      assertNotNull(group.groupValue);
      final Document parentDoc = joinR.document(group.groupValue);
      final String parentID = parentDoc.get("parentID");
      //System.out.println("GROUP groupDoc=" + group.groupDoc + " parent=" + parentDoc);
      assertNotNull(parentID);
      assertTrue(groupHits.length > 0);
      for(int hitIDX=0;hitIDX<groupHits.length;hitIDX++) {
        final Document nonJoinHit = r.document(hits[resultUpto++].doc);
        final Document joinHit = joinR.document(groupHits[hitIDX].doc);
        assertEquals(parentID,
                     nonJoinHit.get("parentID"));
        assertEquals(joinHit.get("childID"),
                     nonJoinHit.get("childID"));
      }

      if (joinGroupUpto < groupDocs.length) {
        // Advance non-join hit to the next parentID:
        //System.out.println("  next joingroupUpto=" + joinGroupUpto + " gd.length=" + groupDocs.length + " parentID=" + parentID);
        while(true) {
          assertTrue(resultUpto < hits.length);
          if (!parentID.equals(r.document(hits[resultUpto].doc).get("parentID"))) {
            break;
          }
          resultUpto++;
        }
      }
    }
  }

  public void testMultiChildTypes() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);

    final List<Document> docs = new ArrayList<Document>();

    docs.add(makeJob("java", 2007));
    docs.add(makeJob("python", 2010));
    docs.add(makeQualification("maths", 1999));
    docs.add(makeResume("Lisa", "United Kingdom"));
    w.addDocuments(docs);

    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);

    // Create a filter that defines "parent" documents in the index - in this case resumes
    Filter parentsFilter = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("docType", "resume"))));

    // Define child document criteria (finds an example of relevant work experience)
    BooleanQuery childJobQuery = new BooleanQuery();
    childJobQuery.add(new BooleanClause(new TermQuery(new Term("skill", "java")), Occur.MUST));
    childJobQuery.add(new BooleanClause(NumericRangeQuery.newIntRange("year", 2006, 2011, true, true), Occur.MUST));

    BooleanQuery childQualificationQuery = new BooleanQuery();
    childQualificationQuery.add(new BooleanClause(new TermQuery(new Term("qualification", "maths")), Occur.MUST));
    childQualificationQuery.add(new BooleanClause(NumericRangeQuery.newIntRange("year", 1980, 2000, true, true), Occur.MUST));


    // Define parent document criteria (find a resident in the UK)
    Query parentQuery = new TermQuery(new Term("country", "United Kingdom"));

    // Wrap the child document query to 'join' any matches
    // up to corresponding parent:
    ToParentBlockJoinQuery childJobJoinQuery = new ToParentBlockJoinQuery(childJobQuery, parentsFilter, ToParentBlockJoinQuery.ScoreMode.Avg);
    ToParentBlockJoinQuery childQualificationJoinQuery = new ToParentBlockJoinQuery(childQualificationQuery, parentsFilter, ToParentBlockJoinQuery.ScoreMode.Avg);

    // Combine the parent and nested child queries into a single query for a candidate
    BooleanQuery fullQuery = new BooleanQuery();
    fullQuery.add(new BooleanClause(parentQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(childJobJoinQuery, Occur.MUST));
    fullQuery.add(new BooleanClause(childQualificationJoinQuery, Occur.MUST));

    // Collects all job and qualification child docs for
    // each resume hit in the top N (sorted by score):
    ToParentBlockJoinCollector c = new ToParentBlockJoinCollector(Sort.RELEVANCE, 10, true, false);

    s.search(fullQuery, c);

    // Examine "Job" children
    boolean showNullPointerIssue=true;
    if (showNullPointerIssue) {
      TopGroups<Integer> jobResults = c.getTopGroups(childJobJoinQuery, null, 0, 10, 0, true);

      //assertEquals(1, results.totalHitCount);
      assertEquals(1, jobResults.totalGroupedHitCount);
      assertEquals(1, jobResults.groups.length);

      final GroupDocs<Integer> group = jobResults.groups[0];
      assertEquals(1, group.totalHits);

      Document childJobDoc = s.doc(group.scoreDocs[0].doc);
      //System.out.println("  doc=" + group.scoreDocs[0].doc);
      assertEquals("java", childJobDoc.get("skill"));
      assertNotNull(group.groupValue);
      Document parentDoc = s.doc(group.groupValue);
      assertEquals("Lisa", parentDoc.get("name"));
    }

    // Now Examine qualification children
    TopGroups<Integer> qualificationResults = c.getTopGroups(childQualificationJoinQuery, null, 0, 10, 0, true);

    assertEquals(1, qualificationResults.totalGroupedHitCount);
    assertEquals(1, qualificationResults.groups.length);

    final GroupDocs<Integer> qGroup = qualificationResults.groups[0];
    assertEquals(1, qGroup.totalHits);

    Document childQualificationDoc = s.doc(qGroup.scoreDocs[0].doc);
    assertEquals("maths", childQualificationDoc.get("qualification"));
    assertNotNull(qGroup.groupValue);
    Document parentDoc = s.doc(qGroup.groupValue);
    assertEquals("Lisa", parentDoc.get("name"));


    r.close();
    dir.close();
  }

  public void testAdvanceSingleParentSingleChild() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);
    Document childDoc = new Document();
    childDoc.add(newField("child", "1", StringField.TYPE_UNSTORED));
    Document parentDoc = new Document();
    parentDoc.add(newField("parent", "1", StringField.TYPE_UNSTORED));
    w.addDocuments(Arrays.asList(childDoc, parentDoc));
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    Query tq = new TermQuery(new Term("child", "1"));
    Filter parentFilter = new CachingWrapperFilter(
                            new QueryWrapperFilter(
                              new TermQuery(new Term("parent", "1"))));

    ToParentBlockJoinQuery q = new ToParentBlockJoinQuery(tq, parentFilter, ToParentBlockJoinQuery.ScoreMode.Avg);
    Weight weight = s.createNormalizedWeight(q);
    DocIdSetIterator disi = weight.scorer(s.getIndexReader().getTopReaderContext().leaves()[0], true, true, null);
    assertEquals(1, disi.advance(1));
    r.close();
    dir.close();
  }

  public void testAdvanceSingleParentNoChild() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(new LogDocMergePolicy()));
    Document parentDoc = new Document();
    parentDoc.add(newField("parent", "1", StringField.TYPE_UNSTORED));
    parentDoc.add(newField("isparent", "yes", StringField.TYPE_UNSTORED));
    w.addDocuments(Arrays.asList(parentDoc));

    // Add another doc so scorer is not null
    parentDoc = new Document();
    parentDoc.add(newField("parent", "2", StringField.TYPE_UNSTORED));
    parentDoc.add(newField("isparent", "yes", StringField.TYPE_UNSTORED));
    Document childDoc = new Document();
    childDoc.add(newField("child", "2", StringField.TYPE_UNSTORED));
    w.addDocuments(Arrays.asList(childDoc, parentDoc));

    // Need single seg:
    w.forceMerge(1);
    IndexReader r = w.getReader();
    w.close();
    IndexSearcher s = newSearcher(r);
    Query tq = new TermQuery(new Term("child", "2"));
    Filter parentFilter = new CachingWrapperFilter(
                            new QueryWrapperFilter(
                              new TermQuery(new Term("isparent", "yes"))));

    ToParentBlockJoinQuery q = new ToParentBlockJoinQuery(tq, parentFilter, ToParentBlockJoinQuery.ScoreMode.Avg);
    Weight weight = s.createNormalizedWeight(q);
    DocIdSetIterator disi = weight.scorer(s.getIndexReader().getTopReaderContext().leaves()[0], true, true, null);
    assertEquals(2, disi.advance(0));
    r.close();
    dir.close();
  }
}
