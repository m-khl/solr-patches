package org.apache.solr.response;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class TestDigits extends Assert{

  @Test
  public void testDigits(){
      assertEquals(Arrays.asList(1,4,3),  new Digits(341).dump());
      assertEquals(Arrays.asList(0),  new Digits(0).dump());
      assertEquals(Arrays.asList(1),  new Digits(1).dump());
      assertEquals(Arrays.asList(2),  new Digits(2).dump());

      assertEquals(Arrays.asList(0,1),  new Digits(10).dump());
      assertEquals(Arrays.asList(0,2),  new Digits(20).dump());        
      assertEquals(Arrays.asList(2,2),  new Digits(22).dump());
      assertEquals(Arrays.asList(1,0,1),  new Digits(101).dump());
      assertEquals(Arrays.asList(0,0,0,1),  new Digits(1000).dump());
      assertEquals(Arrays.asList(0,1,0,1),  new Digits(1010).dump());

  }
}
