package org.apache.lucene.search;

import java.io.IOException;

/** provides rewind() method */
public abstract class DocIdSetBackwardIterator extends DocIdSetIterator {

    /** just an accessor for prevSetBit() */
    public abstract int rewind(int target) throws IOException ;

}
