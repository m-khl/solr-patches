/**
 * 
 */
package org.apache.solr.response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class Digits implements Iterable<Integer> {
    
    private int i;

    Digits(int j) {
        this.i = j;
    }

    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>(){
            final int j = i; 
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
    
    public void set(int k){
      i = k;
    }
}