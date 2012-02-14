package org.apache.lucene.util.junitcompat;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runners.model.Statement;

/**
 * Ensures proper functions of {@link LuceneTestCase#setUp()}
 * and {@link LuceneTestCase#tearDown()}.
 */
public class TestSorePointsReported extends WithNestedTests {
  public static SorePoint where;
  public static SoreType  type;
  
  public static class Nested extends AbstractNestedTest {
    @BeforeClass
    public static void beforeClass() {
      if (isRunningNested()) {
        triggerOn(SorePoint.BEFORE_CLASS);
      }
    }

    @Rule
    public TestRule rule = new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          public void evaluate() throws Throwable {
            if (isRunningNested()) {
              triggerOn(SorePoint.RULE);
            }
            base.evaluate();
          }
        };
      }
    };

    /** Class initializer block/ default constructor. */
    public Nested() {
      triggerOn(SorePoint.INITIALIZER);
    }

    @Before
    public void before() {
      if (isRunningNested()) {
        triggerOn(SorePoint.BEFORE);
      }
    }    

    @Test
    public void test() {
      triggerOn(SorePoint.TEST);
    }
    
    @After
    public void after() {
      if (isRunningNested()) {
        triggerOn(SorePoint.AFTER);
      }
    }    

    @AfterClass
    public static void afterClass() {
      if (isRunningNested()) {
        triggerOn(SorePoint.AFTER_CLASS);
      }
    }    

    /** */
    private static void triggerOn(SorePoint pt) {
      if (pt == where) {
        switch (type) {
          case ASSUMPTION:
            LuceneTestCase.assumeTrue(pt.toString(), false);
            throw new RuntimeException("unreachable");
          case ERROR:
            throw new RuntimeException(pt.toString());
          case FAILURE:
            Assert.assertTrue(pt.toString(), false);
            throw new RuntimeException("unreachable");
        }
      }
    }
  }

  /*
   * ASSUMPTIONS.
   */
  
  public TestSorePointsReported() {
    super(true);
  }

  @Test @Ignore
  public void testAssumeBeforeClass() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.BEFORE_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  @Test @Ignore
  public void testAssumeInitializer() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.INITIALIZER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  @Test
  public void testAssumeRule() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.RULE;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  @Test
  public void testAssumeBefore() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.BEFORE;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  @Test
  public void testAssumeTest() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.TEST;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  @Test
  public void testAssumeAfter() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.AFTER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  @Test @Ignore
  public void testAssumeAfterClass() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.AFTER_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: Assume failed in"));
  }

  /*
   * FAILURES
   */
  
  @Test @Ignore
  public void testFailureBeforeClass() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.BEFORE_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test @Ignore
  public void testFailureInitializer() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.INITIALIZER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureRule() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.RULE;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureBefore() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.BEFORE;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureTest() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.TEST;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureAfter() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.AFTER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test @Ignore
  public void testFailureAfterClass() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.AFTER_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  /*
   * ERRORS
   */
  
  @Test @Ignore
  public void testErrorBeforeClass() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.BEFORE_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test @Ignore
  public void testErrorInitializer() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.INITIALIZER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorRule() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.RULE;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorBefore() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.BEFORE;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorTest() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.TEST;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorAfter() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.AFTER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test @Ignore
  public void testErrorAfterClass() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.AFTER_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  private String runAndReturnSyserr() throws Exception {
    JUnitCore.runClasses(Nested.class);

    String err = getSysErr();
    // syserr.println("Type: " + type + ", point: " + where + " resulted in:\n" + err);
    return err;
  }
}
