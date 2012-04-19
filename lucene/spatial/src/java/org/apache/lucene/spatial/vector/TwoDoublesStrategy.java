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
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.exception.UnsupportedSpatialOperation;
import com.spatial4j.core.query.SpatialArgs;
import com.spatial4j.core.query.SpatialOperation;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.FieldCache.DoubleParser;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.util.CachingDoubleValueSource;
import org.apache.lucene.spatial.util.NumericFieldInfo;
import org.apache.lucene.spatial.util.ValueSourceFilter;

public class TwoDoublesStrategy extends SpatialStrategy<TwoDoublesFieldInfo> {

  private final NumericFieldInfo finfo;
  private final DoubleParser parser;

  public TwoDoublesStrategy(SpatialContext ctx, NumericFieldInfo finfo, DoubleParser parser) {
    super(ctx);
    this.finfo = finfo;
    this.parser = parser;
  }

  @Override
  public boolean isPolyField() {
    return true;
  }

  @Override
  public IndexableField[] createFields(TwoDoublesFieldInfo fieldInfo,
      Shape shape, boolean index, boolean store) {
    if( shape instanceof Point ) {
      Point point = (Point)shape;

      IndexableField[] f = new IndexableField[(index ? 2 : 0) + (store ? 1 : 0)];
      if (index) {
        f[0] = finfo.createDouble( fieldInfo.getFieldNameX(), point.getX() );
        f[1] = finfo.createDouble( fieldInfo.getFieldNameY(), point.getY() );
      }
      if(store) {
        FieldType customType = new FieldType();
        customType.setStored(true);
        f[f.length-1] = new Field( fieldInfo.getFieldName(), ctx.toString( shape ), customType );
      }
      return f;
    }
    if( !ignoreIncompatibleGeometry ) {
      throw new IllegalArgumentException( "TwoDoublesStrategy can not index: "+shape );
    }
    return new IndexableField[0]; // nothing (solr does not support null)
  }

  @Override
  public IndexableField createField(TwoDoublesFieldInfo indexInfo, Shape shape,
      boolean index, boolean store) {
    throw new UnsupportedOperationException("Point is poly field");
  }

  @Override
  public ValueSource makeValueSource(SpatialArgs args, TwoDoublesFieldInfo fieldInfo) {
    Point p = args.getShape().getCenter();
    return new DistanceValueSource(p, ctx.getDistCalc(), fieldInfo, parser);
  }

  @Override
  public Filter makeFilter(SpatialArgs args, TwoDoublesFieldInfo fieldInfo) {
    if( args.getShape() instanceof Circle) {
      if( SpatialOperation.is( args.getOperation(),
          SpatialOperation.Intersects,
          SpatialOperation.IsWithin )) {
        Circle circle = (Circle)args.getShape();
        Query bbox = makeWithin(circle.getBoundingBox(), fieldInfo);

        // Make the ValueSource
        ValueSource valueSource = makeValueSource(args, fieldInfo);

        return new ValueSourceFilter(
            new QueryWrapperFilter( bbox ), valueSource, 0, circle.getDistance() );
      }
    }
    return new QueryWrapperFilter( makeQuery(args, fieldInfo) );
  }

  @Override
  public Query makeQuery(SpatialArgs args, TwoDoublesFieldInfo fieldInfo) {
    // For starters, just limit the bbox
    Shape shape = args.getShape();
    if (!(shape instanceof Rectangle)) {
      throw new InvalidShapeException("A rectangle is the only supported shape (so far), not "+shape.getClass());//TODO
    }
    Rectangle bbox = (Rectangle) shape;
    if (bbox.getCrossesDateLine()) {
      throw new UnsupportedOperationException( "Crossing dateline not yet supported" );
    }

    ValueSource valueSource = null;

    Query spatial = null;
    SpatialOperation op = args.getOperation();

    if( SpatialOperation.is( op,
        SpatialOperation.BBoxWithin,
        SpatialOperation.BBoxIntersects ) ) {
        spatial = makeWithin(bbox, fieldInfo);
    }
    else if( SpatialOperation.is( op,
      SpatialOperation.Intersects,
      SpatialOperation.IsWithin ) ) {
      spatial = makeWithin(bbox, fieldInfo);
      if( args.getShape() instanceof Circle) {
        Circle circle = (Circle)args.getShape();

        // Make the ValueSource
        valueSource = makeValueSource(args, fieldInfo);

        ValueSourceFilter vsf = new ValueSourceFilter(
            new QueryWrapperFilter( spatial ), valueSource, 0, circle.getDistance() );

        spatial = new FilteredQuery( new MatchAllDocsQuery(), vsf );
      }
    }
    else if( op == SpatialOperation.IsDisjointTo ) {
      spatial =  makeDisjoint(bbox, fieldInfo);
    }

    if( spatial == null ) {
      throw new UnsupportedSpatialOperation(args.getOperation());
    }

    if( valueSource != null ) {
      valueSource = new CachingDoubleValueSource(valueSource);
    }
    else {
      valueSource = makeValueSource(args, fieldInfo);
    }
    Query spatialRankingQuery = new FunctionQuery(valueSource);
    BooleanQuery bq = new BooleanQuery();
    bq.add(spatial,BooleanClause.Occur.MUST);
    bq.add(spatialRankingQuery,BooleanClause.Occur.MUST);
    return bq;
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   * @return the spatial query
   */
  private Query makeWithin(Rectangle bbox, TwoDoublesFieldInfo fieldInfo) {
    Query qX = NumericRangeQuery.newDoubleRange(
      fieldInfo.getFieldNameX(),
      finfo.precisionStep,
      bbox.getMinX(),
      bbox.getMaxX(),
      true,
      true);
    Query qY = NumericRangeQuery.newDoubleRange(
      fieldInfo.getFieldNameY(),
      finfo.precisionStep,
      bbox.getMinY(),
      bbox.getMaxY(),
      true,
      true);

    BooleanQuery bq = new BooleanQuery();
    bq.add(qX,BooleanClause.Occur.MUST);
    bq.add(qY,BooleanClause.Occur.MUST);
    return bq;
  }

  /**
   * Constructs a query to retrieve documents that fully contain the input envelope.
   * @return the spatial query
   */
  Query makeDisjoint(Rectangle bbox, TwoDoublesFieldInfo fieldInfo) {
    Query qX = NumericRangeQuery.newDoubleRange(
      fieldInfo.getFieldNameX(),
      finfo.precisionStep,
      bbox.getMinX(),
      bbox.getMaxX(),
      true,
      true);
    Query qY = NumericRangeQuery.newDoubleRange(
      fieldInfo.getFieldNameY(),
      finfo.precisionStep,
      bbox.getMinY(),
      bbox.getMaxY(),
      true,
      true);

    BooleanQuery bq = new BooleanQuery();
    bq.add(qX,BooleanClause.Occur.MUST_NOT);
    bq.add(qY,BooleanClause.Occur.MUST_NOT);
    return bq;
  }
}




