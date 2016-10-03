package PredictiveIndex;

import com.google.common.primitives.Ints;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;

import static PredictiveIndex.InvertedIndex.now;

/**
 * Created by aalto on 10/1/16.
 */
public class Selection extends WWW {
    static int maxBM25 = 367041008;
    static int minBM25 = 1;//80992396;
    static int maxLength = 0;
    static int[] lRanges = computelRanges(1.1);
    static int[] rRanges = computerRanges(1.4, minBM25, maxBM25);
    static long[] deltaRanges = diff(rRanges);
    static long[][][] QM = new long[lRanges.length][rRanges.length][2];
    static long postingCounter = 0;



        /*This functions returns buckets ranges given a length. The ranges-max is hardcoded.*/

    static int[] computelRanges(double lenRule) {
        lenRule = 1.1;
        LinkedList<Integer> lenBuckets = new LinkedList<>();
        for (int i = 4; i < 50220423; i += i * lenRule) {
            lenBuckets.addLast(i);
        }
        lenBuckets.addLast(50220423);
        System.out.println(lenBuckets);
        return Ints.toArray(lenBuckets);
    }

    /*This functions returns buckets ranges given a rank. The ranges-max is hardcoded*/

    static int[] computerRanges(double rankRule, int min, int max) {
        rankRule = 1.4;
        LinkedList<Integer> rankBuckets = new LinkedList<>();
        rankBuckets.add(0);
        for (int i = 11; i < max; i += i * rankRule) {
            rankBuckets.addLast(i);
        }
        rankBuckets.addLast(max);
        System.out.println(rankBuckets);
        return Ints.toArray(rankBuckets);
    }

    /*Getter for the bucket length*/

    static int getLenBucket(int len, int[] lenBuckets) {
        int i;
        //System.out.println(len);
        for (i = 0; lenBuckets[i] < len; i++) ;
        return i;
    }


    /* Since the the rank is monotonically WHAAAT?? we don't want to scan the array everytime
     * with the for loop. Instead we start immediately from the previous rank bucket in a way that
     * if at this iteration we did not change bucket range the operation is almost free O(1).
     *
     * OPEN ISSUES:
    * - */

    static int getRankBucket(int nowRank, int rank, int[] rankBuckets) {
        int i;
        //System.out.println(rank);
        for (i = nowRank; rankBuckets[i] < rank; i++) ;
        return i;
    }

    static long[] diff(int [] bcks){
        long [] bcksSize = new long [bcks.length-1];
        for (int i = 0; i < bcksSize.length ; i++) {
            bcksSize[i] = bcks[i+1]-bcks[i];
        }
        return  bcksSize;

    }

    static int[] getEntry(DataInputStream dataStream, int [] posting) throws IOException {
        try {
            for (int k = 0; k < posting.length; k++) posting[k] = dataStream.readInt();
            postingCounter++;
            if (postingCounter % 200000000 == 0) System.out.println("Up to postings #" + (postingCounter));
            return posting;
        } catch (EOFException exception) {
            System.out.println("Fetching Time: " + (System.currentTimeMillis() - now) + "ms");
            return null;
        }
    }

}
