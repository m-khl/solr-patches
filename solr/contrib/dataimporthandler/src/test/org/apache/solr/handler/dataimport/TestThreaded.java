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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TestThreaded extends AbstractDataImportHandlerTestCase {

  private static final String threads2 = "threads=\"2\"";
  
  private static String dataConfigNPulsOne = "<dataConfig>\n"
          +"<dataSource  type=\"MockDataSource\"/>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" "+threads2+" query=\"select * from x\" >"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>";

  
  private static String dataConfigCached = "<dataConfig>\n"
      +"<dataSource  type=\"MockDataSource\"/>\n"
      + "       <document>\n"
      + "               <entity name=\"x\" "+threads2+" query=\"select * from x\" " +
                         "processor=\"CachedSqlEntityProcessor\""+ ">\n"
      + "                       <field column=\"id\" />\n"
      + "                       <entity name=\"y\" query=\"select * from y\" where=\"xid=x.id\" " +
      "processor=\"CachedSqlEntityProcessor\">\n"
      + "                               <field column=\"desc\" />\n"
      + "                       </entity>\n" + "               </entity>\n"
      + "       </document>\n" + "</dataConfig>";

  private static final Matcher nPulsOne = Pattern.compile(threads2).matcher(dataConfigNPulsOne);
  private static final Matcher cached = Pattern.compile(threads2).matcher(dataConfigCached);
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @Before
  public void setupData(){
      List parentRow = new ArrayList();
  //  parentRow.add(createMap("id", "1"));
    parentRow.add(createMap("id", "2"));
    parentRow.add(createMap("id", "3"));
    parentRow.add(createMap("id", "4"));
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());
    // for every x we have four linked y-s
    List childRow = Arrays.asList(
            createMap("desc", "hello"),
            createMap("desc", "bye"),
            createMap("desc", "hi"),
            createMap("desc", "aurevoir"));
    // for N+1 scenario
    MockDataSource.setIterator("select * from y where y.A=1", shuffledIter(childRow));
    MockDataSource.setIterator("select * from y where y.A=2", shuffledIter(childRow));
    MockDataSource.setIterator("select * from y where y.A=3", shuffledIter(childRow));
    MockDataSource.setIterator("select * from y where y.A=4", shuffledIter(childRow));
    // for cached scenario
    
    List cartesianProduct = new ArrayList();
    for(Map parent : (List<Map>)parentRow){
      for(Iterator<Map> child = shuffledIter(childRow); child.hasNext();){
         Map tuple = createMap("xid", parent.get("id"));
         tuple.putAll(child.next());
         cartesianProduct.add(tuple);
      }
    }
    MockDataSource.setIterator("select * from y", cartesianProduct .iterator());
  }
  
  @After
  public void verify() {
    assertQ(req("id:"+ new String[]{"1","2","3","4"}[random.nextInt(4)]),
                      "//*[@numFound='1']");
    assertQ(req("*:*"), "//*[@numFound='4']");
    assertQ(req("desc:hello"), "//*[@numFound='4']");
    assertQ(req("desc:bye"), "//*[@numFound='4']");
    assertQ(req("desc:hi"), "//*[@numFound='4']");
    assertQ(req("desc:aurevoir"), "//*[@numFound='4']");
  }
  
  private Iterator shuffledIter(List rows){
    ArrayList shuffle = new ArrayList(rows);
    Collections.shuffle(shuffle, random);
    return shuffle.iterator();
  }
  
  @Test
  public void testCachedThreadless_FullImport() throws Exception {
    runFullImport(cached.replaceAll(""));
  }
  
  @Test
  public void testCachedSingleThread_FullImport() throws Exception {
    runFullImport(cached.replaceAll("threads=\"1\""));
  }
  
  @Test
  public void testCachedTwoThread_FullImport() throws Exception {
     runFullImport(cached.replaceAll("threads=\"2\""));
  }
  
  @Test
  public void testCachedTenThreads_FullImport() throws Exception {
    runFullImport(cached.replaceAll("threads=\"10\""));
  }
  
  @Test
  public void testNPlusOneThreadless_FullImport() throws Exception {
    runFullImport(nPulsOne.replaceAll(""));
  }
  @Test
  public void testNPlusOneSingleThread_FullImport() throws Exception {
    runFullImport(nPulsOne.replaceAll("threads=\"1\""));
  }
  
  @Test
  public void testNPlusOneTenThreads_FullImport() throws Exception {
    runFullImport(nPulsOne.replaceAll("threads=\"10\"" ));
  }
  
}
