package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


import static PredictiveIndex.Extra.getDocIDMap;
import static PredictiveIndex.Extra.getFilterSet;
import static PredictiveIndex.Extra.uniquePairs;
import static PredictiveIndex.FastQueryTrace.getFQT;
import static PredictiveIndex.QualityModel.getQualityModel;
import static PredictiveIndex.QualityModel.printQualityModel;
import static PredictiveIndex.utilsClass.splitCollection;

/**
 * Created by aalto on 10/1/16.
 */
public class WWWMain extends WWW {


    /*/home/aalto/IdeaProjects/PredictiveIndex/aux/sort/bin/binsort --size 16 --length 12 --block-size=900000000  ./InvertedIndex.dat ./sortedInvertedIndex.dat*/
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        checkExtraData();
        //splitCollection();
        //tryTry();
        //System.exit(1);

        InvertedIndex i2;
        int distance = 5;
        int numThreads = 4;
        if (!checkExistence(freqMap)) {
            i2 = new InvertedIndex(distance, numThreads);
            startBatteria(i2, 0, numThreads);
            serialize(i2.globalFreqMap, freqMap);
            serialize(i2.globalStats,   gStats);
        }
        if(!checkExistence(rawI2+20)){
            i2 = new InvertedIndex((int[]) deserialize(freqMap), (long[]) deserialize(gStats), distance, numThreads);
            startBatteria(i2, 1, numThreads);
            serialize(i2.dMap, dumpMap);
        }
        if(!checkExistence(sortedI2)) ExternalSort.massiveBinaryMerge(new File(rawI2), sortedI2);
        if(!checkExistence(partialModel)) getQualityModel(1);
        printQualityModel();


    }

    private static void startBatteria(InvertedIndex i2, int phase, int numThreads) throws InterruptedException {
        System.out.println("Starting Batteria. Phase: " + phase);
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new MultiThread(i, i2, phase));
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }


    private static void checkExtraData() throws IOException {
        if(!checkExistence(filterSet))  getFilterSet();
        //if(!checkExistence(fastQT))     getFQT(10)  ;
        if(!checkExistence(accessMap))  uniquePairs();
        //if(!checkExistence())  ;

    }

    private static void tryTry(){
        Long2ObjectOpenHashMap<Int2IntMap> h = (Long2ObjectOpenHashMap<Int2IntMap>) deserialize(fastQT);
        for (long key: h.keySet()) {
            for(int m : h.get(key).keySet()){
                System.out.println(Arrays.toString(getTerms(key)) + " " + m +" "+ h.get(key).get(m));
            }
        }
    }










    private static class MultiThread implements Runnable {
        private int threadNum;
        private InvertedIndex i2;
        private int phase;

        MultiThread(int threadNum, InvertedIndex i2, int phase) {
            this.threadNum = threadNum;
            this.i2 = i2;
            this.phase = phase;
        }

        @Override
        public void run() {
            System.out.println("Thread " + threadNum + "Started");
            try {
                if (this.phase == 0)
                    i2.getClueWebMetadata(threadNum);
                else
                    i2.buildDBigramInvertedIndex(threadNum);

            } catch (IOException | ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    //splitCollection("/home/aalto/dio/docInfo");
    //uniquePairs();
    //mergeDumps();

    //massiveBinaryMerge(new File(globalFold + "rawInvertedIndex/"),globalFold+"invertedIndex.bin");
    //testMassiveBinaryMerge2(new File(globalFold + "rawInvertedIndex/"));
    //sortSmallInvertedIndex();



    //fetchTermMap();
    //buildFastQueryTrace();

    //computelRanges(1);
    //computerRanges(1.4, 1, 400000000);
    //getQualityModel(globalFold+"invertedIndex.bin");
    //printQualityModel(metadata+"qualityModel");

    //convertProbabilities();
    //getFinalModel(globalFold+"invertedIndex.bin",metadata+"qualityModelfinal");
    //finalModel();

    //greedySelection();


       // System.exit(1);




}