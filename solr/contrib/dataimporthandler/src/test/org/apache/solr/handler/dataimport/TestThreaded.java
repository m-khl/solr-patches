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

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class TestThreaded extends AbstractDataImportHandlerTestCase {

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
    MockDataSource.setCollection("select * from x", new Once(parentRow));
    // for every x we have four linked y-s
    List childRow = Arrays.asList(
            createMap("desc", "hello"),
            createMap("desc", "bye"),
            createMap("desc", "hi"),
            createMap("desc", "aurevoir"));
    // for N+1 scenario
    MockDataSource.setCollection("select * from y where y.A=1", new Once(shuffled(childRow)));
    MockDataSource.setCollection("select * from y where y.A=2", new Once(shuffled(childRow)));
    MockDataSource.setCollection("select * from y where y.A=3", new Once(shuffled(childRow)));
    MockDataSource.setCollection("select * from y where y.A=4", new Once(shuffled(childRow)));
    // for cached scenario
    
    List cartesianProduct = new ArrayList();
    for(Map parent : (List<Map>)parentRow){
      for(Iterator<Map> child = shuffled(childRow).iterator(); child.hasNext();){
         Map tuple = createMap("xid", parent.get("id"));
         tuple.putAll(child.next());
         cartesianProduct.add(tuple);
      }
    }
    MockDataSource.setCollection("select * from y", new Once(cartesianProduct));
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
  
  private <T> List<T> shuffled(List<T> rows){
    ArrayList<T> shuffle = new ArrayList<T>(rows);
    Collections.shuffle(shuffle, random);
    return shuffle;
  }
  
  @Test
  public void testCachedThreadless_FullImport() throws Exception {
    runFullImport(getCachedConfig(random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), 0));
  }
  
  @Test
  public void testCachedSingleThread_FullImport() throws Exception {
    runFullImport(getCachedConfig(random.nextBoolean(), random.nextBoolean(), random.nextBoolean(), 1));
  }
  
  @Test
  public void testCachedThread_FullImport() throws Exception {
    int numThreads = random.nextInt(8) + 2; // between 2 and 10
    String config = getCachedConfig(random.nextBoolean(), random.nextBoolean(),  random.nextBoolean(), numThreads);
    runFullImport(config);
  }
  
  @Test
  public void testCachedThread_Total() throws Exception{
    List<String> cfgs = new ArrayList<String>();
    for(boolean a : new boolean[]{true,false}){
      for(boolean b : new boolean[]{true,false}){
        for(boolean c : new boolean[]{true,false}){
          for(int t : new int[]{0,1,3}){
            cfgs.add(
                getCachedConfig(a, b, c, t));
          }
        }
      }
    }
    Collections.shuffle(cfgs);
    for (String cfg:cfgs){
      setupData();
        runFullImport(cfg);
      verify();
    }
  }
    
  private String getCachedConfig(boolean parentCached, boolean childCached, boolean useWhereParam, int numThreads) {
    return "<dataConfig>\n"
      +"<dataSource  type=\"MockDataSource\"/>\n"
      + "       <document>\n"
      + "               <entity name=\"x\" "+(numThreads==0 ? "" : "threads=\"" + numThreads + "\"")
      + "                 query=\"select * from x\" " 
      + "                 processor=\""+(parentCached ? "Cached":"")+"SqlEntityProcessor\">\n"
      + "                       <field column=\"id\" />\n"
      + (useWhereParam
      ? "                       <entity name=\"y\" query=\"select * from y\" where=\"xid=x.id\" " 
      : "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\" " 
      )+"                         processor=\""+(childCached || useWhereParam ? "Cached":"")+"SqlEntityProcessor\">\n"
      + "                               <field column=\"desc\" />\n"
      + "                       </entity>\n" + "               </entity>\n"
      + "       </document>\n" + "</dataConfig>";
  }
  
  private static class Once<E> extends AbstractCollection<E> {    
    private final Collection<E> delegate;    
    private volatile boolean hit = false; 
    
    public Once(Collection<E> delegate) {
      this.delegate = delegate;
    }    
    @Override
    public Iterator<E> iterator() {
      if (!hit) {
        hit = true;
        return delegate.iterator();
      } else {
        throw new IllegalStateException(delegate
            + " is expected to be enumerated only once");
      }
    }    
    @Override
    public int size() {
      return delegate.size();
    }
  }  
}
