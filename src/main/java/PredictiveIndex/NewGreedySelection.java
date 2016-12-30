package PredictiveIndex;

import it.unimi.dsi.fastutil.doubles.Double2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by aalto on 12/7/16.
 */
public class NewGreedySelection extends Selection {
    static Double2ObjectRBTreeMap<long[]> heap;
    static Long2IntOpenHashMap counterMap;



    public static void greedySelection(String modelName, String output, String lanMap, String bucketmap, int limitBudget){

        double [][][] model = (double[][][]) deserialize(modelName);
        Long2DoubleOpenHashMap probMap = (Long2DoubleOpenHashMap) deserialize(lanMap);
        Long2IntOpenHashMap bukMap  = (Long2IntOpenHashMap) deserialize(bucketmap);
        initCounters();
        heap = new Double2ObjectRBTreeMap<>();
        int x, y;
        double score, last;
        long range;
        boolean change = true;
        int budget =0;
        int counter =0;

        while(budget < limitBudget | change) {
            change = false;
            for (long aguTerm : counterMap.keySet()) {
                x = counterMap.merge(aguTerm, 1, Integer::sum);
                System.out.println(model);
                if(x<model[0].length) {
                    y = bukMap.get(aguTerm);
                    score = -probMap.get(aguTerm) * model[y][x][0];
                    range = deltaRanges[(int) model[y][x][1]];

                    if (budget < limitBudget) {
                        heap.put(score, new long[]{aguTerm, range});
                        budget+=range;
                    } else if ((last = heap.lastDoubleKey()) > score) {
                        heap.remove(last);
                        heap.put(score, new long[]{aguTerm, range});
                        budget+=range;
                        change = true;
                    }
                    if(++counter%10000==0){
                        System.out.println(counter);
                        System.out.println(heap.lastDoubleKey());
                    }
                }
            }
        }
        System.out.println(heap.size());
        System.out.println(Arrays.toString(deltaRanges));
        serialize(getSubMap(limitBudget, heap), output);
    }


    /** */
    private static Long2LongOpenHashMap getSubMap(int limitBudget, Double2ObjectRBTreeMap<long[]> heap){
        Long2LongOpenHashMap result = new Long2LongOpenHashMap();
        LinkedList<Long> list = new LinkedList<>();
        int budget = 0;
        for (long [] value: heap.values()){
            result.put(value[0], value[1]);
            budget+=value[1];
            list.add(value[1]);
            if(budget>limitBudget) break;
        }
        System.out.println(Arrays.toString(getTerms(list.getFirst())));
        //System.out.println(result.toString());
        return result;
    }

    //the greedy selection select up to the moment when it doesn't find anything new, than take the top-limitBudget
    public static void initCounters(){
        counterMap = new Long2IntOpenHashMap();
        LongOpenHashSet trainAguTerms = (LongOpenHashSet) deserialize(UNIGRAM_SMALL_FILTER_SET);
        for (long aguTerm : trainAguTerms) {
            counterMap.put(aguTerm, 0);
        }
    }

    public static void getBucketMaps(){
        generateBucketMap(UNILENGTHS, UNIBUCKET);
        generateBucketMap(HITLENGTHS, HITBUCKET);
        generateBucketMap(BILENGTHS, BIBUCKET);
        generateBucketMap(DBILENGTHS, DBIBUCKET);
    }


    public static void generateBucketMap(String input, String output){
        Long2IntOpenHashMap lenMap = (Long2IntOpenHashMap) deserialize(input);
        Long2IntOpenHashMap bucketMap = new Long2IntOpenHashMap();
        for (long term: lenMap.keySet()) {
            bucketMap.put(term, getLenBucket(lenMap.get(term)));
        }
        serialize(bucketMap, output);
    }

    public static void generateBucketMap2(String input, String output){
        Int2LongOpenHashMap lenMap = (Int2LongOpenHashMap) deserialize(input);
        Long2IntOpenHashMap bucketMap = new Long2IntOpenHashMap();
        for (int term: lenMap.keySet()) {
            bucketMap.put(term, getLenBucket((int)lenMap.get(term)));
        }
        serialize(bucketMap, output);
    }

}

