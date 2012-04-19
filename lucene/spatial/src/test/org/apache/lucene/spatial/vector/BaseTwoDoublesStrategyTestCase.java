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

package org.apache.lucene.spatial.vector;

import com.spatial4j.core.context.SpatialContext;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.util.NumericFieldInfo;
import org.junit.Test;

import java.io.IOException;


public abstract class BaseTwoDoublesStrategyTestCase extends StrategyTestCase<TwoDoublesFieldInfo> {

  protected abstract SpatialContext getSpatialContext();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.ctx = getSpatialContext();
    this.strategy = new TwoDoublesStrategy(ctx,
        new NumericFieldInfo(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER);
    this.fieldInfo = new TwoDoublesFieldInfo(getClass().getSimpleName());
  }

  @Test
  public void testCitiesWithinBBox() throws IOException {
    getAddAndVerifyIndexedDocuments(DATA_WORLD_CITIES_POINTS);
    executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_IsWithin_BBox);
  }
}
