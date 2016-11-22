package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.*;

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
    static Long2IntOpenHashMap accMap;
    static Long2LongOpenHashMap dumped;
    static LongOpenHashSet hitPairs = new LongOpenHashSet();
    static long hit = 0;



    /*The quality model is a small 3D matrix:
    * 0) PairID
    * 1) Number of Varbytes to read
    * 2) Number of documents */

    public static long[][][] getBigramQualityModel(int function, String index, String dumpMap, String model) throws IOException, ClassNotFoundException {
        accMap = (Long2IntOpenHashMap) deserialize(accessMap);
        dumped = (Long2LongOpenHashMap) deserialize(dumpMap);
        //Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = getFQT(10);
        Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap>> fastQueryTrace = (Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap>>) deserialize(fastQT+"102");
        System.out.println(fastQueryTrace.size());


        System.out.println("Fast Query Trace fetched!\nProcessing Inverted Index...");
        System.out.println(index);
        DataInputStream DIStream = getDIStream(index);
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int[] posting = new int[4];
        int[] currentPair = new int[]{-1, -1};
        long pair = -1;

        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            posting = getEntry(DIStream, posting);
            if (posting == null) break;
            //System.out.println(Arrays.toString(posting));
            if (posting[0] != currentPair[0] | posting[1] != currentPair[1]) {
                //System.out.println(posting[0] +" "+ currentPair[0] +" - "+posting[1] +" "+ currentPair[1]);

                pair = getPair(currentPair[0], currentPair[1]);

                if (fastQueryTrace.containsKey(pair)){
                    //hitPairs.add(pair);
                    System.out.println(auxPostingList.size());
                    //System.out.println(Arrays.toString(getTerms(pair)));
                    //processBigramPostingList(pair, currentPair[0], currentPair[1], Ints.toArray(auxPostingList), fastQueryTrace.get(pair));
                    fastQueryTrace.put(pair, processPostingListNew(fastQueryTrace.get(pair), Ints.toArray(auxPostingList)));
                }
                //DOStream.close();
                auxPostingList.clear();
                currentPair[0] = posting[0];
                currentPair[1] = posting[1];
            }
            auxPostingList.addLast(posting[3]); //docid

        }
        System.out.println(hitPairs.size());

        serialize(QM, model);
        printQualityModel(model);
        return QM;
    }

    private static Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap> processPostingListNew(Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap> qt, int [] postingList){
        for(int i = 0; i < postingList.length; i++){
            for(Int2IntLinkedOpenHashMap map : qt.values()){
                if(map.containsKey(postingList[i])) map.put(postingList[i], i+1);
            }
        }
        return qt;
    }

    private static void processBigramPostingList(long pair, int t1, int t2, int[] postingList, Int2IntMap aggregatedTopK) {

        //access increment
        int increment = accMap.get(t1) + accMap.get(t2) + accMap.get(pair);
        int lbucket = getLenBucket((postingList.length) +  getTerms(dumped.get(pair))[1], lRanges);
        int range = getRankBucket(0, (postingList.length), rRanges);

        for (int k = 0; k < range; k++) {
            QM[lbucket][k][1] += increment * deltaRanges[k];
        }

        //hit increment
        int rankBucket = 0;
        for (int i = 0; i < postingList.length - 1; i++) {
            increment = aggregatedTopK.get(postingList[i]);
            if (increment > 0) {
                hitPairs.add(pair);
                rankBucket = getRankBucket(rankBucket, i, rRanges);
                QM[lbucket][rankBucket][0] += increment;
                hit+=increment;
                System.out.println("Number of HITS: " + (hit));
            }

        }
    }


}
