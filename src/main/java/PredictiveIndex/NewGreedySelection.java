package PredictiveIndex;

import it.unimi.dsi.fastutil.doubles.Double2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.*;

import java.util.HashSet;

/**
 * Created by aalto on 12/7/16.
 */
public class NewGreedySelection extends Selection {
    static Double2ObjectRBTreeMap<long[]> heap;
    static Long2IntOpenHashMap counterMap;


    public static void greedySelection(int budget, String input, String output){

        double [][][] model = (double[][][]) deserialize(input);
        Long2DoubleOpenHashMap probMap = (Long2DoubleOpenHashMap) deserialize(lanModel);
        Long2IntOpenHashMap bukMap  = (Long2IntOpenHashMap) deserialize(bucketMap);
        initCounters();
        heap = new Double2ObjectRBTreeMap<>();
        int x, y;
        double score, last;
        long range;
        boolean change = true;

        while(heap.size()<counterMap.size() | change) {
            change = false;
            for (long aguTerm : counterMap.keySet()) {
                x = counterMap.merge(aguTerm, 1, Integer::sum);
                if(x<model[0].length) {
                    y = bukMap.get(aguTerm);
                    score = probMap.get(aguTerm) * model[y][x][0];
                    range = deltaRanges[(int) model[y][x][1]];

                    if (heap.size() < counterMap.size()) {
                        heap.put(score, new long[]{aguTerm, range});
                    } else if ((last = heap.lastDoubleKey()) < score) {
                        heap.remove(last);
                        heap.put(score, new long[]{aguTerm, range});
                        change = true;
                    }
                }
            }
        }
        serialize(getSubMap(budget, heap), output);
    }


    /** */
    private static Long2LongOpenHashMap getSubMap(int budget, Double2ObjectRBTreeMap<long[]> heap){
        Long2LongOpenHashMap result = new Long2LongOpenHashMap();
        for (long [] value: heap.values()) {
            result.put(value[0], value[1]);
            if(result.size()>budget) break;
        }
        return result;
    }

    //the greedy selection select up to the moment when it doesn't find anything new, than take the top-budget
    public static void initCounters(){
        counterMap = new Long2IntOpenHashMap();
        LongOpenHashSet trainAguTerms = (LongOpenHashSet) deserialize("set");
        for (long aguTerm : trainAguTerms) {
            counterMap.put(aguTerm, 0);
        }
    }

}

