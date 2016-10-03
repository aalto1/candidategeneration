package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import static PredictiveIndex.FastQueryTrace.getFQT;
import static PredictiveIndex.InvertedIndex.now;
import static PredictiveIndex.WWW.getDIStream;
import static PredictiveIndex.utilsClass.getPair;

/**
 * Created by aalto on 10/1/16.
 */
public class QualityModel extends Selection {
    static Long2IntOpenHashMap accMap = (Long2IntOpenHashMap) deserialize(accessMap);
    static Long2IntOpenHashMap dumped = (Long2IntOpenHashMap) deserialize(dumpMap);
    static long hit = 0;



    /*The quality model is a small 3D matrix:
    * 0) PairID
    * 1) Number of Varbytes to read
    * 2) Number of documents */

    public static long[][][] getQualityModel(int function) throws IOException, ClassNotFoundException {
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = getFQT(10);

        System.out.println("Fast Query Trace fetched!\nProcessing Inverted Index...");
        DataInputStream DIStream = getDIStream(sortedI2);
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int[] posting = new int[4];
        int[] currentPair = new int[]{-1, -1};
        long pair = -1;

        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            posting = getEntry(DIStream, posting);
            if (posting == null) break;

            if (posting[0] != currentPair[0] | posting[1] != currentPair[1]) {
                //processing Posting List

                pair = getPair(currentPair[0], currentPair[1]);
                switch (function){

                    case 1:
                        if (fastQueryTrace.containsKey(pair))
                            processPostingList(pair, currentPair[0], currentPair[1], Ints.toArray(auxPostingList), fastQueryTrace.get(pair));
                        break;

                    case 2:
                        //DOStream.writeLong(pair);
                        //DOStream.writeInt(auxPostingList.size() + dumped.get(pair));
                        break;
                }

                //DOStream.close();
                auxPostingList.clear();
                currentPair[0] = posting[0];
                currentPair[1] = posting[1];
            }
            auxPostingList.addLast(posting[3]); //docid

        }

        serialize(QM, partialModel);
        return QM;
    }

    private static void processPostingList(long pair, int t1, int t2, int[] postingList, Int2IntMap aggregatedTopK) {

        //access increment
        int increment = accMap.get(t1) + accMap.get(t2) + accMap.get(pair);
        int lbucket = getLenBucket((postingList.length) + dumped.get(pair), lRanges);
        int range = getRankBucket(0, (postingList.length), rRanges);

        for (int k = 0; k < range; k++) {
            QM[lbucket][k][1] += increment * deltaRanges[k];
        }

        //hit increment
        int rankBucket = 0;
        for (int i = 0; i < postingList.length - 1; i++) {
            increment = aggregatedTopK.get(postingList[i]);
            if (increment > 0) {
                rankBucket = getRankBucket(rankBucket, i, rRanges);
                QM[lbucket][rankBucket][0] += increment;
                System.out.println("Number of HITS: " + (hit+=increment));
            }

        }
    }

    static void printQualityModel() throws IOException, ClassNotFoundException {
        long[][][] qm = (long[][][]) deserialize(partialModel);
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

        serialize(bucketOrder,   sortedRange);
        serialize(finalQM,       qualityModel);
    }

}
