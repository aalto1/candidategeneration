package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.function.BiFunction;

import static PredictiveIndex.FastQueryTrace.getFQT;

/**
 * Created by aalto on 11/12/16.
 */
public class UnigramQualityModel extends Selection {
    static Long2IntOpenHashMap accMap;
    static Int2LongOpenHashMap dumped;
    static LongOpenHashSet hitPairs = new LongOpenHashSet();
    static long hit = 0;

    public static long[][][] getUnigramQualityModel(int function, String index, String dumpMap,  String model) throws IOException, ClassNotFoundException {
        accMap = (Long2IntOpenHashMap) deserialize(accessMap);
        dumped = (Int2LongOpenHashMap) deserialize(dumpMap);
        //Long2ObjectOpenHashMap<Int2IntMap> fastUnigramQueryTrace = getFQT(10);
        Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap>> fastUnigramQueryTrace = (Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap>>) deserialize(fastQT+"102");

        System.out.println(fastUnigramQueryTrace.size());


        System.out.println("Fast Query Trace fetched!\n Processing Inverted Index...");
        DataInputStream DIStream = getDIStream(index);
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int[] posting = new int[3];                         //
        int currentTerm = -1;

        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            posting = getEntry(DIStream, posting);
            if (posting == null) break;

            if (posting[0] != currentTerm) {
                //processing Posting List

                if (fastUnigramQueryTrace.containsKey(posting[0])) {
                    //hitPairs.add(pair);
                    //System.out.println(Arrays.toString(posting));
                    //processUnigramPostingList(posting[0], Ints.toArray(auxPostingList), fastUnigramQueryTrace.get(posting[0]));
                    fastUnigramQueryTrace.put(posting[0], processPostingListNew(fastUnigramQueryTrace.get(posting[0]), Ints.toArray(auxPostingList)));
                }
                //DOStream.close();
                currentTerm = posting[0];
                auxPostingList.clear();
            }
            auxPostingList.addLast(posting[2]); //docid

        }
        System.out.println(hitPairs.size());

        serialize(QM, model);
        restructResults(fastUnigramQueryTrace);
        printQualityModel(model);
        return QM;
    }

    private static BiFunction<LinkedList<long[]>, long[], LinkedList<long[]>> mergeList = (x,y) -> {
        x.addLast(y);
        return x;
    };

    private static LinkedList<long[]> map2longarray(Int2IntMap map){
        long [] array = new long[map.size()];
        LinkedList<long[]> a = new LinkedList<>();
        int k = 0;
        for (int key : map.keySet()) {
            array[k++] = getPair(key,map.get(key));
        }
        a.add(array);
        return a;
    }

    private static void restructResults(Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap>> fqt) throws IOException {
        Int2ObjectOpenHashMap<LinkedList<long[]>> toPrint = new Int2ObjectOpenHashMap<>();
        Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap> pairQID;

        for(long pair: fqt.keySet()){
            pairQID = fqt.get(pair);
            for(int qID: pairQID.keySet()) {
                toPrint.merge(qID, map2longarray(pairQID.get(qID)), UnigramQualityModel::mergeList);
            }
        }

        BufferedWriter bw = getBuffWriter(results+"modelResult");
        for(int k : toPrint.keySet()){
            bw.write("############################################\n");
            bw.write("Document: " + k);
            bw.write("############################################\n");
            for(long [] a : toPrint.get(k)){
                for (long elem: a) {
                    bw.write(Arrays.toString(getTerms(elem)));
                }
                bw.newLine();
                bw.newLine();
            }

        }
    bw.close();
    }




    private static LinkedList<long[]> mergeList(LinkedList<long[]> a, LinkedList<long[]> longs) {
        longs.addLast(a.getFirst());
        return longs;
    }


    private static Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap> processPostingListNew(Int2ObjectOpenHashMap<Int2IntLinkedOpenHashMap> qt, int [] postingList){
        int counter = 0;
        for(int i = 0; i < postingList.length & counter<10; i++){
            for(Int2IntLinkedOpenHashMap map : qt.values()){
                if(map.containsKey(postingList[i])){
                    map.put(postingList[i], i+1);
                    counter++;
                }
            }
        }
        return qt;
    }

    private static void processUnigramPostingList(int term, int[] postingList, Int2IntMap aggregatedTopK) {

        //access increment
        int increment = accMap.get(term);
        int lbucket = getLenBucket(getTerms(dumped.get(term))[0], lRanges);
        int range = getRankBucket(0, (postingList.length), rRanges);

        for (int k = 0; k < range; k++) {
            QM[lbucket][k][1] += increment * deltaRanges[k];
        }

        //hit increment
        int rankBucket = 0;
        for (int i = 0; i < postingList.length - 1; i++){
            increment = aggregatedTopK.get(postingList[i]);
            if (increment > 0) {
                rankBucket = getRankBucket(rankBucket, i, rRanges);
                QM[lbucket][rankBucket][0] += increment;
                hit+=increment;
                System.out.println("Number of HITS: " + (hit));
            }

        }
    }

}
