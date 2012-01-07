package org.apache.solr.handler.dataimport;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestEphemeralCache extends AbstractDataImportHandlerTestCase {

	private final String dataConfig = loadDataConfig("dataimport-cache-ephemeral.xml");

    @BeforeClass
	public static void beforeClass() throws Exception {
		initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
	}

	@Before
	public void reset(){
	    DestroyCountCache.destroyed.clear();
	    
	    setupMockData();
	}
	
	
    @SuppressWarnings("unchecked")
    private void setupMockData() {
        List parentRows = new ArrayList();
        parentRows.add(createMap("id", new BigDecimal("1"), "parent_s", "one"));
        parentRows.add(createMap("id", new BigDecimal("2"), "parent_s", "two"));
        parentRows.add(createMap("id", new BigDecimal("3"), "parent_s", "three"));
        parentRows.add(createMap("id", new BigDecimal("4"), "parent_s", "four"));
        parentRows.add(createMap("id", new BigDecimal("5"), "parent_s", "five"));

        List child1Rows = new ArrayList();
        child1Rows.add(createMap("id", new BigDecimal("6"), "child1a_mult_s", "this is the number six."));
        child1Rows.add(createMap("id", new BigDecimal("5"), "child1a_mult_s", "this is the number five."));
        child1Rows.add(createMap("id", new BigDecimal("6"), "child1a_mult_s", "let's sing a song of six."));
        child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "three"));
        child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "III"));
        child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "3"));
        child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "|||"));
        child1Rows.add(createMap("id", new BigDecimal("1"), "child1a_mult_s", "one"));
        child1Rows.add(createMap("id", new BigDecimal("1"), "child1a_mult_s", "uno"));
        child1Rows.add(createMap("id", new BigDecimal("2"), "child1b_s", "CHILD1B", "child1a_mult_s", "this is the number two."));
        
        List child2Rows = new ArrayList();
        child2Rows.add(createMap("id", new BigDecimal("6"), "child2a_mult_s", "Child 2 says, 'this is the number six.'"));
        child2Rows.add(createMap("id", new BigDecimal("5"), "child2a_mult_s", "Child 2 says, 'this is the number five.'"));
        child2Rows.add(createMap("id", new BigDecimal("6"), "child2a_mult_s", "Child 2 says, 'let's sing a song of six.'"));
        child2Rows.add(createMap("id", new BigDecimal("3"), "child2a_mult_s", "Child 2 says, 'three'"));
        child2Rows.add(createMap("id", new BigDecimal("3"), "child2a_mult_s", "Child 2 says, 'III'"));
        child2Rows.add(createMap("id", new BigDecimal("3"), "child2b_s", "CHILD2B", "child2a_mult_s", "Child 2 says, '3'"));
        child2Rows.add(createMap("id", new BigDecimal("3"), "child2a_mult_s", "Child 2 says, '|||'"));
        child2Rows.add(createMap("id", new BigDecimal("1"), "child2a_mult_s", "Child 2 says, 'one'"));
        child2Rows.add(createMap("id", new BigDecimal("1"), "child2a_mult_s", "Child 2 says, 'uno'"));
        child2Rows.add(createMap("id", new BigDecimal("2"), "child2a_mult_s", "Child 2 says, 'this is the number two.'"));
        
        MockDataSource.setIterator("SELECT * FROM PARENT", parentRows.iterator());
        MockDataSource.setIterator("SELECT * FROM CHILD_1", child1Rows.iterator());
        MockDataSource.setIterator("SELECT * FROM CHILD_2", child2Rows.iterator());
        
    }

    @Ignore
    @Test
    public void testTenThreads() throws Exception {
        assertFullImport(dataConfig);
        
    }
    
    @Test
    public void testThreadless() throws Exception {
        assertFullImport(dataConfig.replaceAll("threads=\"10\"", ""));
    }

    @Test
    public void testSingleThread() throws Exception {
        assertFullImport(dataConfig.replaceAll("threads=\"10\"", "threads=\"1\""));
    }
    
    private void assertFullImport(String dataConfig) throws Exception {
        runFullImport(dataConfig);

        assertQ(req("*:*"),                                       "//*[@numFound='5']");
        assertQ(req("id:1"),                                      "//*[@numFound='1']");
        assertQ(req("id:6"),                                      "//*[@numFound='0']");
        assertQ(req("parent_s:four"),                             "//*[@numFound='1']");
        assertQ(req("child1a_mult_s:this\\ is\\ the\\ numbe*"),   "//*[@numFound='2']");
        assertQ(req("child2a_mult_s:Child\\ 2\\ say*"),           "//*[@numFound='4']");
        assertQ(req("child1b_s:CHILD1B"),                         "//*[@numFound='1']");
        assertQ(req("child2b_s:CHILD2B"),                         "//*[@numFound='1']");
        assertQ(req("child1a_mult_s:one"),                        "//*[@numFound='1']");
        assertQ(req("child1a_mult_s:uno"),                        "//*[@numFound='1']");
        assertQ(req("child1a_mult_s:(uno OR one)"),               "//*[@numFound='1']");
        
        assertThat(DestroyCountCache.destroyed.size(), is(3));
    }
}

class DestroyCountCache extends SortedMapBackedCache{
    static Map<DIHCache,DIHCache> destroyed = new IdentityHashMap<DIHCache, DIHCache>();
    
    @Override
    public void destroy() {
        super.destroy();
        Assert.assertThat(destroyed.put(this, this), nullValue());
    }
    
    public DestroyCountCache() {
    }
    
}
