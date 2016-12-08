package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.*;
import java.nio.Buffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;


import static PredictiveIndex.Extra.*;
import static PredictiveIndex.FastQueryTrace.buildFastQT2;
import static PredictiveIndex.FastQueryTrace.getFQT;
import static PredictiveIndex.NewQualityModel.buildQualityMatrix;
import static PredictiveIndex.QualityModel.getBigramQualityModel;
import static PredictiveIndex.QualityModel.printQualityModel;
import static PredictiveIndex.Selection.getLenBucketMap;
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
        //NewQualityModel.getModel(finalSingle, unigramModel, fastQT2);
        buildQualityMatrix(model1);
        getLenBucketMap();
        NewGreedySelection.greedySelection(1000, model1, "chunk");
        System.exit(1);

        InvertedIndex i2;
        int distance = 5;
        int numThreads = 4;
        if (!checkExistence(localFreqMap)){
            i2 = new InvertedIndex(distance, numThreads);
            startBatteria(i2, 0, numThreads);
            getLocFreqMap(i2.termFreqArray, i2.uniTerms); //no need
            //serialize(i2.termFreqArray, termFrequencyArray);
            serialize(i2.globalStats,   gStats);
        }
        if(true/*!checkExistence(dumpMap)*/){
            //D-Bigram
            //i2 = new InvertedIndex((Int2IntOpenHashMap) deserialize(localFreqMap), null, (long[]) deserialize(gStats), distance, true, dBigramIndex, numThreads);
            //buildStructure(i2, numThreads);
            //Single + HIT
            i2 = new InvertedIndex((Int2IntOpenHashMap) deserialize(localFreqMap), (int[]) deserialize(hitScores), (long[]) deserialize(gStats), 1, false, singleIndex, numThreads);
            buildStructure(i2, numThreads);
            //BigramIndex.getBigramIndex(finalSingle);
        }
        //i2 = new InvertedIndex((Int2IntOpenHashMap) deserialize(localFreqMap), (int[]) deserialize(hitScores), (long[]) deserialize(gStats), 1, false, singleIndex, numThreads);
        //startBatteria(i2, 0, numThreads);
        //serialize(i2.missingWords,results+"trueMissingSet");
        buildFinalStructures();
        //buildQualityModels();
        //printQualityModel();


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

    private static void buildQualityModels() throws IOException, ClassNotFoundException {
        if(checkExistence(dBiModel)) {
            getBigramQualityModel(1, finalDBigram, dBigramDumpMap, dBiModel);
        }
        if(!checkExistence(hitModel)){
            UnigramQualityModel.getUnigramQualityModel(1, finalHIT, hitDumpMap, hitModel);
        }
        if(!checkExistence(unigramModel)) {
            UnigramQualityModel.getUnigramQualityModel(1, finalSingle, unigramDumpMap, unigramModel);
        }
    }

    private static void printModels() throws IOException, ClassNotFoundException {
        printQualityModel(dBiModel);
        printQualityModel(hitModel);
        printQualityModel(unigramModel);
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

    private static void finda() throws IOException {
        int currentTerm = -1;
        int [] previousBM25 = new int[3];
        int [] newBM25 = new int[3];
        int [] posting = new int[3];
        int max = -99;
        DataInputStream DIStream = getDIStream(finalSingle);

        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            if ((posting = Selection.getEntry(DIStream, posting)) == null) break;

            newBM25 = posting;
            //posting[2] = DM.get(posting[2]);

            //System.out.println(Arrays.toString(newBM25));
            //System.out.println(Arrays.toString(previousBM25) +"d");
            if(posting[1]>max) max = posting[1];
            if(newBM25[1] > previousBM25[1] & newBM25[0] == previousBM25[0]){
                /** While I scan the posting list I the value of the bm25 should decrease with new<old */
                //System.err.println(posting[0] +" - "+ currentTerm);
                System.out.println(Arrays.toString(previousBM25));
                System.out.println(Arrays.toString(newBM25));
            }
            previousBM25 = newBM25.clone();

        }
        System.out.println(max);
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

    public static void x(){
        IntOpenHashSet a = (IntOpenHashSet) deserialize(uniqueTerms);
        System.out.println(a.size());
        System.out.println(Collections.max(a));
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

    public static void elaborateMe(String modelPath) throws IOException {
        Int2LongOpenHashMap posTListLength = (Int2LongOpenHashMap) deserialize(unigramDumpMap);
        BufferedWriter bw = getBuffWriter(results+"me.csv");
        double value = 0;
        Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>> model = (Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>>) deserialize(modelPath);
        for (Long2ObjectOpenHashMap<long[]> aguTerms: model.values()) {
            for(long aguTerm: aguTerms.keySet()){
                for (long score: aguTerms.get(aguTerm)) {
                    value = (score*100.0)/posTListLength.get((int)aguTerm);
                    System.out.println(value);
                    bw.write(value+",");
                }
            }
        }
        bw.close();
    }

    public static void elaborateQi() throws IOException {
        BufferedReader br = getBuffReader("/home/aalto/dio/query/QM/unigram_training_posting_model");
        String line;
        int [] field;
        StringBuffer sb = new StringBuffer();
        while((line=br.readLine())!=null){
            field = string2IntArray(line, " ");
            for (int i = 0; i < field.length ; i+=2) {
                sb.append(field[i+1]*100.0/field[i] + ",");
            }
        }
        BufferedWriter bw = getBuffWriter(metadata+"qiresults.csv");
        bw.write(sb.toString());
        bw.close();
        br.close();


    }



}
