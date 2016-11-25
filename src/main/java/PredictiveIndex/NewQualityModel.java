package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static PredictiveIndex.utilsClass.getDIDMap;

/**
 * Created by aalto on 11/23/16.
 */
public class NewQualityModel extends Selection {
    static Long2IntOpenHashMap accMap;
    static Int2LongOpenHashMap dumped;
    static LongOpenHashSet hitPairs = new LongOpenHashSet();
    static long hit = 0;

    /**Once we process a porocess a posting list the corrispondent array is completly filled up. The only
     * thing that could change this is a line that you did not process the whole corpora
     *
     * negative number => full index can have negative bm25
     * check score of the first one
     * inverted index not sorted
     * */

    public static long[][][] getModel(String index, String output, String model) throws IOException, ClassNotFoundException {
        System.out.println("Fast Query Trace fetched!\n Processing Inverted Index...");
        Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>> fastUnigramQueryTrace = (Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>>) deserialize(fastQT2);
        Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>> emptymodel = (Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>>) deserialize(unigramEmptyModel);

        DataInputStream DIStream = getDIStream(finalSingle);
        int[] posting = new int[3];                         //
        long currentTerm = -1;
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> documentsToFind = new Int2ObjectOpenHashMap();
        Int2IntOpenHashMap scores;
        int counter = 0;
        int postingNumber = 0;
        int previousBM25 = 0;
        int newBM25 =0;
        LinkedList<Integer> a = new LinkedList<>();
        //Int2IntOpenHashMap DM = getDIDMap();


        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            if ((posting = Selection.getEntry(DIStream, posting)) == null) break;
            previousBM25 = newBM25;
            newBM25 = posting[1];
            //posting[2] = DM.get(posting[2]);


            if(newBM25 > previousBM25 & posting[0] == currentTerm & posting[1]>0){
                /** While I scan the posting list I the value of the bm25 should decrease with new<old */
                //System.err.println(posting[0] +" - "+ currentTerm);
                System.out.println(previousBM25+"-"+newBM25);
            }

            if (posting[0] != currentTerm) {
                currentTerm = posting[0];
                //System.out.println(currentTerm);
                documentsToFind = fastUnigramQueryTrace.get(posting[0]);
                postingNumber += counter;
                counter = 0;
            }
            try {
                if (documentsToFind.size() > 0 & (scores = documentsToFind.remove(posting[2])) != null) {
                    for (int qID : scores.keySet()) {
                        try {
                            emptymodel.get(qID).get(currentTerm)[scores.get(qID)] = getPair(posting[2], counter);
                            if(qID==1753) printResult(emptymodel.get(qID), qID, 0, posting[1], scores.get(qID));
                        }catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                }
            }catch(Exception e){
                //System.out.println(Arrays.toString(posting));
            }
            counter++;

        }
        System.out.println(hitPairs.size());
        System.out.println(postingNumber);
        //printQualityModel(model);
        return QM;
    }

    public static void printResult(Long2ObjectOpenHashMap<long[]> m, int qID, int topBm25, int bm25, int pos){
        System.out.println(qID);
        for(long [] a : m.values()) {
            for (long i : a) {
                System.out.print(Arrays.toString(getTerms(i)));
            }
            System.out.println();
            System.err.println("top: "+topBm25+" elem: "+bm25+ " diff: " + (topBm25-bm25) + " position: " + pos);
            //break;
        }
    }


    public static void deepOperation(Map m, long[] keys, long lastKey, Object value, BiFunction operation){
        LinkedList<Map> maze = new LinkedList<>();
        maze.add(m);
        for (int depth = 0; depth < keys.length; depth++) {
            maze.addFirst((Map) maze.getFirst().get(keys[depth]));
        }
        maze.getFirst().merge(lastKey, value, operation);

        for (int depth = keys.length-1; depth <= 0; depth--) {
            maze.get(1).put(keys[depth], maze.removeFirst());
        }
    }

}
