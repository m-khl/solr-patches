package org.apache.solr.response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.LargeVolumeTestBase.DocThread;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResponseStreamingTest extends SolrJettyTestBase {
    
    static final class Digits implements Iterable<Integer> {
        private final int j;

        Digits(int j) {
            this.j = j;
        }

        @Override
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>(){
                int ord = 10;
                
                @Override
                public boolean hasNext() {
                    return j==0 ? ord/(1+j)<=10: ord/j<=10;
                }
                @Override
                public Integer next() {
                    int result = j % ord / (ord/10);
                    ord*=10;
                    return result;
                }
                @Override
                public void remove() {
                }
                
            };
        }
        
        List<Integer> dump(){
            List<Integer> l = new ArrayList<Integer>();
            for(Integer i: this){
                l.add(i);
            }
            return l; 
        }
        
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
       createJetty(new File(ExternalPaths.EXAMPLE_HOME, "../../core/src/test-files/solr").getAbsolutePath(), 
               "solrconfig-response-streaming.xml", null);
    }
    
    @Test
    public void testSample() throws SolrServerException, IOException{
        SolrServer gserver = this.getSolrServer();
        gserver.deleteByQuery( "*:*" ); // delete everything!
        
        ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        for(int i=1; i<100;i++){
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i );
            final int j = i;
            doc.addField("cat_s", new Digits(j).dump());
            docs.add(doc);
            
            if(!docs.isEmpty() && (i % 10 == 0 || !(i+1<100))){
                gserver.add(docs);
                docs.clear();
            }
        }
        
        // some of the commits could have failed because maxWarmingSearchers exceeded,
        // so do a final commit to make sure everything is visible.
        gserver.commit();
        
//        SolrQuery params = new SolrQuery("*:*");
//        params.set("wt","response-streaming");
//        params.set("qt","response-streaming");
        URL url = new URL("http://localhost:"+port+"/solr/select?wt=response-streaming&qt=response-streaming&q=*:*&response-streaming=true&fl=id");
        log.info("url {}", url);
        InputStream is = url.openConnection().getInputStream();
//        int b;
//        FileOutputStream out = new FileOutputStream("/tmp/out");
        
        byte buff[] = new byte[4]; 
        
        int pre=-1; 
        while(is.read(buff)==4){
            int i = ((buff[0] & 0xFF) << 24) | ((buff[1] & 0xFF) << 16)
            | ((buff[2] & 0xFF) <<  8) |  (buff[3] & 0xFF);
            if(pre==-1){
                assertEquals(1, i);
            }else{
                assertEquals(pre+1, i);
            }
            pre = i;
 //           out.write(buff);
        }
        
        assertEquals(pre, 100-1);
        
        is.close();
 //       out.close();
        
        log.info("file written");
    }
    
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
