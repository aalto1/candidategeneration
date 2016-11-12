package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;

import static PredictiveIndex.FastQueryTrace.getFQT;

/**
 * Created by aalto on 11/12/16.
 */
public class UnigramQualityModel extends Selection {
    static Int2IntOpenHashMap accMap;
    static Int2IntOpenHashMap dumped;
    static LongOpenHashSet hitPairs = new LongOpenHashSet();
    static long hit = 0;

    public static long[][][] getUnigramQualityModel(int function, String index) throws IOException, ClassNotFoundException {
        accMap = (Int2IntOpenHashMap) deserialize(accessMap);
        dumped = (Int2IntOpenHashMap) deserialize(dumpMap);
        Long2ObjectOpenHashMap<Int2IntMap> fastUnigramQueryTrace = getFQT(10);
        System.out.println(fastUnigramQueryTrace.size());


        System.out.println("Fast Query Trace fetched!\nProcessing Inverted Index...");
        DataInputStream DIStream = getDIStream(index);
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int[] posting = new int[3];
        int currentTerm = -1;

        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            posting = getEntry(DIStream, posting);
            if (posting == null) break;

            if (posting[0] != currentTerm) {
                //processing Posting List

                switch (function){

                    case 1:
                        if (fastUnigramQueryTrace.containsKey(posting[0]))
                            //hitPairs.add(pair);
                            processUnigramPostingList(dumped.get(posting[0]), Ints.toArray(auxPostingList), fastUnigramQueryTrace.get(posting[0]));
                        break;

                    case 2:
                        //DOStream.writeLong(pair);
                        //DOStream.writeInt(auxPostingList.size() + dumped.get(pair));
                        break;
                }

                //DOStream.close();
                currentTerm = posting[0];
                auxPostingList.clear();
            }
            auxPostingList.addLast(posting[3]); //docid

        }
        System.out.println(hitPairs.size());

        serialize(QM, partialModel);
        return QM;
    }

    private static void processUnigramPostingList(int increment, int[] postingList, Int2IntMap aggregatedTopK) {

        //access increment
        int lbucket = getLenBucket((postingList.length) + increment, lRanges);
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
                hit+=increment;
                System.out.println("Number of HITS: " + (hit));
            }

        }
    }

}
