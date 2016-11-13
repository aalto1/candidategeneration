package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


import static PredictiveIndex.Extra.*;
import static PredictiveIndex.FastQueryTrace.getFQT;
import static PredictiveIndex.QualityModel.getBigramQualityModel;
import static PredictiveIndex.QualityModel.printQualityModel;
import static PredictiveIndex.utilsClass.*;

/**
 * Created by aalto on 10/1/16.
 *
 * Minimum Distance is 1: this value takes into account just the prox term.
 */
public class WWWMain extends WWW {

        //14k

    //30k
    /*/home/aalto/IdeaProjects/PredictiveIndex/aux/sort/bin/binsort --size 16 --length 12 --block-size=900000000  ./InvertedIndex.dat ./sortedInvertedIndex.dat*/
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //checkExtraData();
        //getDocIDMap();
        //splitCollection();
        november13();
        //tryTry();
        //printQualityModel();
        //ExternalSort.massiveBinaryMerge(new File(dBigramIndex+rawI2),dBigramIndex+sortedI2);
        //sortComplexRanking();
        //getFQT(10);
        //System.exit(1);
        //getId2TermMap();

        InvertedIndex i2;
        int distance = 5;
        int numThreads = 4;
        if (!checkExistence(localFreqMap)) {
            i2 = new InvertedIndex(distance, numThreads);
            startBatteria(i2, 0, numThreads);
            getLocFreqMap(i2.termFreqArray, i2.uniTerms); //no need
            //serialize(i2.termFreqArray, termFrequencyArray);
            serialize(i2.globalStats,   gStats);
        }
        if(false/*!checkExistence(dumpMap)*/){
            //Single + HIT
            i2 = new InvertedIndex((Int2IntOpenHashMap) deserialize(localFreqMap), (int[]) deserialize(hitScores), (long[]) deserialize(gStats), 1, false, singleIndex, numThreads);
            buildStructure(i2, numThreads);
            //D-Bigram
            i2 = new InvertedIndex((Int2IntOpenHashMap) deserialize(localFreqMap), null, (long[]) deserialize(gStats), distance, true, dBigramIndex, numThreads);
            buildStructure(i2, numThreads);
        }
        buildFinalStructures();
        //if(!checkExistence(partialModel)) getBigramQualityModel();
        printQualityModel();


    }

    private static void buildStructure(InvertedIndex i2, int numThreads) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("");
        startBatteria(i2, 1, numThreads);
        serialize(Arrays.stream(i2.dmpPost).sum(), dumpedPostings);
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

    private static void buildFinalStructures() throws IOException {
        if(!checkExistence(finalSingle))
            ExternalSort.massiveBinaryMerge(new File(singleIndex+rawI2), finalSingle, false);
        if(!checkExistence(finalHIT))
            ExternalSort.massiveBinaryMerge(new File(HITIndex+rawI2), finalHIT, false);
        if(!checkExistence(finalDBigram))
            ExternalSort.massiveBinaryMerge(new File(dBigramIndex+rawI2), finalDBigram, true);
    }


    private static void checkExtraData() throws IOException {
        if(!checkExistence(hitScores))  getHitScore2();
        if(!checkExistence(bigFilterSet))  getBigFilterSet();
        if(!checkExistence(smallFilterSet)) getSmallFilterSet();
        if(!checkExistence(fastQT))     getFQT(10)  ;
        if(!checkExistence(accessMap))  uniquePairs();
        if(!checkExistence(uniqueTerms)) getUniqueTermsSet();

    }

    private static void tryTry() throws IOException {

        if(true) {
            int[][] decodingCheck = new int[30][];
            BufferedReader br = getBuffReader(trentaDoc);
            String line;
            int[] termID;
            String[] auxBuff;
            line = br.readLine();
            line = br.readLine();
            for (int k = 0; (line = br.readLine()) != null; k++) {
                auxBuff = line.split(" ");
                termID = new int[auxBuff.length];
                for (int i = 0; i < auxBuff.length; i++) {
                    termID[i] = Integer.valueOf(auxBuff[i]);
                }
                decodingCheck[k] = termID;
                line = br.readLine();
            }
            for (int [] a : decodingCheck) System.out.println(Arrays.toString(a));
            serialize(decodingCheck,array30);


        }

        if(false){
            Long2ObjectOpenHashMap<Int2IntMap> h = (Long2ObjectOpenHashMap<Int2IntMap>) deserialize(fastQT);
            for (long key: h.keySet()) {
                for(int m : h.get(key).keySet()){
                    System.out.println(Arrays.toString(getTerms(key)) + " " + m +" "+ h.get(key).get(m));
                }
            }
        }
        System.exit(1);

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
