package PredictiveIndex;

import it.unimi.dsi.fastutil.doubles.Double2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by aalto on 12/7/16.
 */
public class NewGreedySelection extends Selection {
    static Double2ObjectRBTreeMap<long[]> heap;
    static Long2IntOpenHashMap counterMap;



    public static void greedySelection(String modelName, String output, String lanMap, String bucketmap, String inputMeta, int limitBudget) throws IOException, ClassNotFoundException {

        double [][][] model = (double[][][]) deserialize(modelName);
        Long2DoubleOpenHashMap probMap = (Long2DoubleOpenHashMap) deserialize(lanMap);

        Long2IntOpenHashMap bukMap  = (Long2IntOpenHashMap) deserialize(bucketmap);
        probMap.defaultReturnValue(0);
        initCounters(inputMeta);
        heap = new Double2ObjectRBTreeMap<>();
        int x, y;
        double score, last;
        long range;
        boolean change = true;
        int budget =0;
        int counter =0;
        //NewQualityModel.buildQualityMatrix(FILLEDUNIGRAM, UNIGRAMQUALITYMODEL);

        while(budget < limitBudget | change) {
            change = false;
            for (long aguTerm : counterMap.keySet()) {
                x = counterMap.merge(aguTerm, 1, Integer::sum);
                if (x < model[0].length) {
                    y = bukMap.get(aguTerm);
                    score = -probMap.get(aguTerm) * model[y][x][0];
                    //System.out.println(model[y][x][0] + " " + aguTerm);
                    range = deltaRanges[(int) model[y][x][1]];


                    if (budget < limitBudget) {
                        if (heap.containsKey(score))
                            System.out.println("azz..." + (score));
                        heap.put(score, new long[]{aguTerm, range});
                        budget += (getTerms(range)[1] - getTerms(range)[0]);
                    } else if ((last = heap.lastDoubleKey()) > score) {
                        heap.remove(last);
                        heap.put(score, new long[]{aguTerm, range});
                        budget += (getTerms(range)[1] - getTerms(range)[0]);
                        change = true;
                    }



                    //System.out.println(budget);
                    if (++counter % 10000 == 0) {
                        System.out.println(counter);
                        System.out.println(heap.lastDoubleKey());
                        System.out.println("Heap size: " + heap.size());

                    }
                }
            }

        }
        System.out.println("budgettone : " +  budget);
        System.out.println("mappettone : " +  heap.size());

        System.out.println(Arrays.toString(deltaRanges));
        serialize(getSubMap(limitBudget, heap), output);
    }


    /** */
    private static Long2ObjectOpenHashMap getSubMap(int limitBudget, Double2ObjectRBTreeMap<long[]> heap){
        Long2ObjectOpenHashMap<ArrayList<Long>> result = new Long2ObjectOpenHashMap<>();
        ArrayList<Long> list;
        int budget = 0;
        System.out.println("Building stuff..." + heap.size());
        for (long [] value: heap.values()){
            if((list=result.get(value[0]))==null)
                list = new ArrayList<>();
            list.add(value[1]);
            result.put(value[0],list);
            System.out.println(budget);
            budget+= (getTerms(value[1])[1] - getTerms(value[1])[0]);

            list.add(value[1]);
            if(budget>limitBudget){
                System.out.println("final: " + budget);
                break;
            }
        }
        //System.out.println(Arrays.toString(getTerms(list.getFirst())));
        //System.out.println(result.toString());
        return result;
    }

    //the greedy selection select up to the moment when it doesn't find anything new, than take the top-limitBudget
    public static void initCounters(String inputMeta) throws IOException {
        counterMap = new Long2IntOpenHashMap();
        String line;
        BufferedReader br = getBuffReader(inputMeta);
        while((line=br.readLine())!=null){
            counterMap.put(Double.valueOf(line.split(" ")[0]).longValue(), 0);
        }
        br.close();
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

