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
package org.apache.solr.handler.dataimport;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TestThreaded extends AbstractDataImportHandlerTestCase {

@BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImport() throws Exception {
    List parentRow = new ArrayList();
//    parentRow.add(createMap("id", "1"));
    parentRow.add(createMap("id", "2"));
    parentRow.add(createMap("id", "3"));
    parentRow.add(createMap("id", "4"));
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());

    List childRow = new ArrayList();
    Map map = createMap("desc", "hello");
    childRow.add(map);

    MockDataSource.setIterator("select * from y where y.A=1", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=2", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=3", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=4", childRow.iterator());

    runFullImport(dataConfig);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("*:*"), "//*[@numFound='4']");
    assertQ(req("desc:hello"), "//*[@numFound='4']");
  }

  @Test
  public void testCachedSingleThread_FullImport() throws Exception {
      testCached(threads2replacement.replaceAll("threads=\"1\""));
  }
  
  @Test
  public void testCachedMultiThread_FullImport() throws Exception {
      testCached(dataCachedConfig);
  }
  
  @Test
  public void testCachedTenThreads_FullImport() throws Exception {
      testCached(threads2replacement.replaceAll("threads=\"10\""));
  }
  
  @Test
  public void testCachedThreadless_FullImport() throws Exception {
      testCached(threads2replacement.replaceAll(""));
  }
  
  @SuppressWarnings("unchecked")
  public void testCached(String config) throws Exception {
    List<Map> parentRow = new ArrayList();
//    parentRow.add(createMap("id", "1"));
    parentRow.add(createMap("id", "2"));
    parentRow.add(createMap("id", "3"));
    parentRow.add(createMap("id", "4"));
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x", (Iterator) parentRow.iterator());

    List childRow = new ArrayList();
    for(Map row : parentRow){
        for(int i=0;i<4;i++){
           childRow.add(createMap("xid", row.get("id"),
                   "desc", Integer.toString(i)));
        }
    }

    MockDataSource.setIterator("select * from y", childRow.iterator());

    runFullImport(config);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("*:*"), "//*[@numFound='4']");
    assertQ(req("desc:0"), "//*[@numFound='4']");
    assertQ(req("desc:1"), "//*[@numFound='4']");
    assertQ(req("desc:2"), "//*[@numFound='4']");
    assertQ(req("desc:3"), "//*[@numFound='4']");
  }
  
  private static String dataConfig = "<dataConfig>\n"
          +"<dataSource  type=\"MockDataSource\"/>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" threads=\"10\" query=\"select * from x\" deletedPkQuery=\"select id from x where last_modified > NOW AND deleted='true'\" deltaQuery=\"select id from x where last_modified > NOW\">\n"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>";

  private static final String threads2 = "threads=\"2\"";
  
  private static String dataCachedConfig = "<dataConfig>\n"
      +"<dataSource  type=\"MockDataSource\"/>\n"
      + "       <document>\n"
      + "               <entity name=\"x\" "+threads2+" query=\"select * from x\" deletedPkQuery=\"select id from x where last_modified > NOW AND deleted='true'\" deltaQuery=\"select id from x where last_modified > NOW\" " +
                         "processor=\"CachedSqlEntityProcessor\""+ ">\n"
      + "                       <field column=\"id\" />\n"
      + "                       <entity name=\"y\" query=\"select * from y\" where=\"xid=x.id\" " +
      "processor=\"CachedSqlEntityProcessor\">\n"
      + "                               <field column=\"desc\" />\n"
      + "                       </entity>\n" + "               </entity>\n"
      + "       </document>\n" + "</dataConfig>";

private static final Matcher threads2replacement = Pattern.compile(threads2).matcher(dataCachedConfig);
}
