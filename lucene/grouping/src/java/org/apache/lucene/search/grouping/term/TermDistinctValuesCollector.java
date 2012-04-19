package org.apache.lucene.search.grouping.term;

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
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.DocTermsIndex; // javadocs
import org.apache.lucene.search.grouping.AbstractDistinctValuesCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

import java.io.IOException;
import java.util.*;

/**
 * A term based implementation of {@link org.apache.lucene.search.grouping.AbstractDistinctValuesCollector} that relies
 * on {@link DocTermsIndex} to count the distinct values per group.
 *
 * @lucene.experimental
 */
public class TermDistinctValuesCollector extends AbstractDistinctValuesCollector<TermDistinctValuesCollector.GroupCount> {

  private final String groupField;
  private final String countField;
  private final List<GroupCount> groups;
  private final SentinelIntSet ordSet;
  private final GroupCount groupCounts[];
  private final BytesRef spare = new BytesRef();

  private FieldCache.DocTermsIndex groupFieldTermIndex;
  private FieldCache.DocTermsIndex countFieldTermIndex;

  /**
   * Constructs {@link TermDistinctValuesCollector} instance.
   *
   * @param groupField The field to group by
   * @param countField The field to count distinct values for
   * @param groups The top N groups, collected during the first phase search
   */
  public TermDistinctValuesCollector(String groupField, String countField, Collection<SearchGroup<BytesRef>> groups) {
    this.groupField = groupField;
    this.countField = countField;
    this.groups = new ArrayList<GroupCount>(groups.size());
    for (SearchGroup<BytesRef> group : groups) {
      this.groups.add(new GroupCount(group.groupValue));
    }
    ordSet = new SentinelIntSet(groups.size(), -1);
    groupCounts = new GroupCount[ordSet.keys.length];
  }

  public void collect(int doc) throws IOException {
    int slot = ordSet.find(groupFieldTermIndex.getOrd(doc));
    if (slot < 0) {
      return;
    }

    GroupCount gc = groupCounts[slot];
    int countOrd = countFieldTermIndex.getOrd(doc);
    if (doesNotContainsOrd(countOrd, gc.ords)) {
      if (countOrd == 0) {
        gc.uniqueValues.add(null);
      } else {
        gc.uniqueValues.add(countFieldTermIndex.lookup(countOrd, new BytesRef()));
      }

      gc.ords = Arrays.copyOf(gc.ords, gc.ords.length + 1);
      gc.ords[gc.ords.length - 1] = countOrd;
      if (gc.ords.length > 1) {
        Arrays.sort(gc.ords);
      }
    }
  }

  private boolean doesNotContainsOrd(int ord, int[] ords) {
    if (ords.length == 0) {
      return true;
    } else if (ords.length == 1) {
      return ord != ords[0];
    }
    return Arrays.binarySearch(ords, ord) < 0;
  }

  public List<GroupCount> getGroups() {
    return groups;
  }

  public void setNextReader(AtomicReaderContext context) throws IOException {
    groupFieldTermIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), groupField);
    countFieldTermIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), countField);

    ordSet.clear();
    for (GroupCount group : groups) {
      int groupOrd = group.groupValue == null ? 0 : groupFieldTermIndex.binarySearchLookup(group.groupValue, spare);
      if (groupOrd < 0) {
        continue;
      }

      groupCounts[ordSet.put(groupOrd)] = group;
      group.ords = new int[group.uniqueValues.size()];
      Arrays.fill(group.ords, -1);
      int i = 0;
      for (BytesRef value : group.uniqueValues) {
        int countOrd = value == null ? 0 : countFieldTermIndex.binarySearchLookup(value, new BytesRef());
        if (countOrd >= 0) {
          group.ords[i++] = countOrd;
        }
      }
    }
  }

  static class GroupCount extends AbstractDistinctValuesCollector.GroupCount<BytesRef> {

    int[] ords;

    GroupCount(BytesRef groupValue) {
      super(groupValue);
    }
  }

}
