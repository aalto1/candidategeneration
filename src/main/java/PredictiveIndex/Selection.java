package PredictiveIndex;

import com.google.common.primitives.Ints;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import static PredictiveIndex.InvertedIndex.now;

/**
 * Created by aalto on 10/1/16.
 */
public class Selection extends WWW {
    static int maxBM25 = 50220423;
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
        for (int i = 11; i < max; i += i * rankRule) {
            rankBuckets.addLast(i);
        }
        rankBuckets.addLast(max);
        System.out.println(rankBuckets);
        return Ints.toArray(rankBuckets);
    }

    /*Getter for the bucket length*/

    static int getLenBucket(int len) {
        int i;
        //System.out.println(len);
        for (i = 0; lRanges[i] < len; i++){
            //System.out.println(i + " " + lenBuckets[i] +" " + len);
        }
        return i;
    }


    /* Since the the rank is monotonically WHAAAT?? we don't want to scan the array everytime
     * with the for loop. Instead we start immediately from the previous rank bucket in a way that
     * if at this iteration we did not change bucket range the operation is almost free O(1).
     *
     * OPEN ISSUES:
    * - */

    static int getRankBucket(int nowRank, int rank) {
        int i;
        for (i = nowRank; rRanges[i] < rank; i++) ;
        return i;
    }

    static long[] diff(int [] bcks){
        long [] bcksSize = new long [bcks.length-1];
        for (int i = 0; i < bcksSize.length ; i++) {
            bcksSize[i] = bcks[i+1]-bcks[i];
        }
        return  bcksSize;

    }


    /*this method works even for three term inverted index since is based only on the
    lenght of the given array*/
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

    static void printQualityModel(String model) throws IOException, ClassNotFoundException {
        long[][][] qm = (long[][][]) deserialize(model);
        int [] rRange = computerRanges(1.4, 1, 400000000);
        float HP;
        float [][][] bufferQM = new float[qm.length][qm[0].length][2];
        float [][] finalQM = new float[qm.length][qm[0].length];
        long [][] bucketOrder = new long[bufferQM.length][bufferQM[0].length-1];
        System.out.println(bufferQM[0].length);


        int lowerBound =0;
        int upperBound =0;
        for (int i = 0; i < bufferQM.length; i++) {
            for (int j = 0; j < bufferQM[0].length-1; j++) {
                HP = (float) ((qm[i][j][0]*1.0)/(qm[i][j][1]));
                //System.out.print(HP+"\t\t");
                if(Float.isNaN(HP) | Float.isInfinite(HP))
                    bufferQM[i][j][0] =0;
                else
                    bufferQM[i][j][0] =  HP;
                bufferQM[i][j][1] = j;
                //System.out.print(bufferQM[i][j][0] +"\t");
            }


            Arrays.sort(bufferQM[i], new Comparator<float[]>() {
                @Override
                public int compare(float[] o1, float[] o2) {
                    return Float.compare(o2[0],o1[0]);
                }
            });

            /***/

            //System.out.println(Arrays.deepToString(bufferQM[i]));
            int end = 0;
            //System.out.println(rRange[0] +"," + rRange[1]);
            for (int j = 0; j < bufferQM[i].length-1; j++) {

                lowerBound = (int) bufferQM[i][j][1];
                upperBound = (int) bufferQM[i][j][1]+1;
                if(upperBound==22) end = 1;
                if(upperBound < 22) {
                    finalQM[i][j] = bufferQM[i][j][0];
                    bucketOrder[i][j - end] = getPair(rRange[lowerBound], rRange[upperBound]);
                    //System.out.println(rRange[lowerBound] +" , "+ rRange[upperBound]);
                }
            }
            //Those two print functions is useful to understand which buckets have more value
            //System.out.println(Arrays.toString(bucketOrder[i]));
            for (long a: bucketOrder[i]) System.out.print(Arrays.toString(getTerms(a)) +" ");
            System.out.println();
            //System.exit(1);

        }

        serialize(bucketOrder,  model+"_sortedRange");
        serialize(finalQM,      model+"_qualityModel");
    }


}
