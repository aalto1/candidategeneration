package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;



/**
 * Created by aalto on 10/1/16.
 */
public class FastQueryTrace extends WWW {

    /** This class collects the methods to fetch from Clueweb09
     * 1) Filter Set
     * 2) Fast Query Trace
     * 3) Access Map
     */

    private static Long2ObjectOpenHashMap<Int2IntMap> FQT;

    public static Long2ObjectOpenHashMap<Int2IntMap> getFQT(int k) throws IOException {
        if (checkExistence(fastQT+k)) {
            return (Long2ObjectOpenHashMap<Int2IntMap>) deserialize(fastQT+k);
        } else {
            return buildFastQT(k);
        }
    }

    private static Long2ObjectOpenHashMap<Int2IntMap> buildFastQT(int k) throws IOException {
        int[][] topKMatrix = getTopKMatrix(new int[173800][], getBuffReader(complexRank), k);
        BufferedReader br= getBuffReader(trainQ);
        FQT = new Long2ObjectOpenHashMap<>();
        String line;
        String [] field;
        long[] queryBigrams;
        int[] topK;

        while ((line = br.readLine()) != null) {
            field = line.split(":");
            queryBigrams = getBigrams(field[1].split(" "));
            try {
                for (long bigram : queryBigrams) {
                    addTopK(bigram, topKMatrix[Integer.valueOf(field[0])]);
                }
            }catch (NullPointerException e){
                System.out.println(Integer.valueOf(field[0]));
            }
        }
        serialize(FQT, fastQT+k);
        return FQT;
    }

    private static void addTopK(long bigram, int[] topK) {
        Int2IntMap auxMap = FQT.get(bigram);
        if (auxMap != null) {
            for (int doc : topK) {
                if (auxMap.putIfAbsent(doc, 1) != null) {
                    auxMap.merge(doc, 1, Integer::sum);
                    FQT.put(bigram, auxMap);
                }
            }
        } else {
            auxMap = new Int2IntOpenHashMap();
            for (int doc : topK) {
                auxMap.put(doc, 1);
            }
            FQT.put(bigram, auxMap);
        }
    }


        /*This function parse the document and build an int[][] to get O(1) access to the top500 of each document*/

        private static int[][] getTopKMatrix(int[][] topMatrix, BufferedReader br, int k) throws IOException {
            System.out.println("Building TopK matrix...");
            getTermMap();
            LinkedList<Integer> auxTopK = new LinkedList<>();
            String line;
            String [] field;
            int perm = 0;
            int tmp;
            int topk = 0;

            while ((line = br.readLine()) != null) {
                field = line.split(" ");
                tmp = Integer.valueOf(field[0]);
                if (perm != tmp) {
                    topMatrix[perm] = Ints.toArray(auxTopK);
                    auxTopK.clear();
                    perm = tmp;
                    topk =0;
                }
                if(topk<k) {
                    auxTopK.addLast(Integer.valueOf(field[1]));
                    topk++;
                }
            }

            System.out.println("TopK matrix built.");
            return topMatrix;
        }





}
