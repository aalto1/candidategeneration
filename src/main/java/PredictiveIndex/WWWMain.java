package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.*;
import java.util.Arrays;
import static PredictiveIndex.Extra.*;
import static PredictiveIndex.Selection.printQualityModel;
import static PredictiveIndex.utilsClass.*;
import static PredictiveIndex.NewQualityModel.*;
import static PredictiveIndex.ExternalSort.*;
import static PredictiveIndex.NestedQueryTrace.*;
import static PredictiveIndex.Metadata.*;
import static PredictiveIndex.NewGreedySelection.*;

/**
 * Created by aalto on 10/1/16.
 *
 * Minimum Distance is 1: this value takes into account just the prox term.
 */
public class WWWMain extends WWW {
    static InvertedIndex i2;
    static int distance = 5;
    static int numThreads = 4;
    static int budget = 1000;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //PHASE0_CollectMetadata();
        //PHASE1_CollectGobalStatistics();
        PHASE2_CollectQualityModel();
        //PHASE3_CollectBestChunks();
    }

    private static void PHASE0_CollectMetadata() throws IOException {
        //getUnigramLanguageModel();
        //getBigramLanguageModel();
        //getBigFilterSet(new String[]{UNIGRAMLANGUAGEMODELCONVERTED}, BIG_FILTER_SET);

        convertANDcleanQueryTrace(Q, QCONVERTED);
        agumentedQueryTrace(true);
        agumentedQueryTrace(false);

        getEmptyModel(QAGUMENTED, EMPTYMODEL);
        buildReference(QAGUMENTED, REFERENCEMODEL);

        getSmallFilterSet(QCONVERTED, UNIGRAM_SMALL_FILTER_SET);
        getSmallFilterSet(QBIGRAM, BIGRAM_SMALL_FILTER_SET);
        getAccessMap(QAGUMENTED, ACCESSMAP);
        System.exit(1);

    }

    private static void PHASE1_CollectGobalStatistics() throws IOException, ClassNotFoundException, InterruptedException {
        i2 = new InvertedIndex(numThreads);
        startBatteria(i2, 0, numThreads);
        getLocFreqMap(i2.termFreqArray, (LongOpenHashSet) deserialize(UNIGRAM_SMALL_FILTER_SET));
        //serialize(i2.termFreqArray, termFrequencyArray);
        serialize(i2.globalStats,  GLOBALSTATS);
        //System.exit(1);
    }

    private static void PHASE2_CollectQualityModel() throws InterruptedException, IOException, ClassNotFoundException {
        if(!(checkExistence(FILLEDHIT) & checkExistence(FILLEDUNIGRAM)))
            PHASE21_CollectUnigramHitModel();
        PHASE23_CollectDBigramModel();
        //PHASE22_CollectBigramModel();

    }

    private static void PHASE21_CollectUnigramHitModel() throws IOException, ClassNotFoundException, InterruptedException {
        i2 = new InvertedIndex(
                (Int2IntOpenHashMap) deserialize(LOCALTERMFREQ),
                (int[]) deserialize(HITSCORES),
                (long[]) deserialize(GLOBALSTATS),
                1,
                false,
                UNIGRAMRAW,
                UNIGRAM_SMALL_FILTER_SET,
                numThreads);
        //buildStructure(i2, numThreads, UNIGRAMRAW);

        if(!checkExistence(UNIGRAMINDEX))
            massiveBinaryMerge(new File(UNIGRAMRAW), UNIGRAMINDEX, false, UNIGRAMMETA);
        getModel(UNIGRAMINDEX,FILLEDUNIGRAM, UNIGRAMMETA);

        if(!checkExistence(HITINDEX))
            massiveBinaryMerge(new File(HITRAW), HITINDEX, false, HITMETA);
        getModel(HITINDEX, FILLEDHIT, HITMETA);

    }

    private static void PHASE22_CollectBigramModel() throws IOException, ClassNotFoundException {
        BigramIndex.getBigramIndex(UNIGRAMINDEX, 1000);
        massiveBinaryMerge(new File(BIGRAMRAW), BIGRAMINDEX, true, BIGRAMMETA);
        getModel(BIGRAMINDEX, FILLEDBIGRAM, DBIGRAMMETA);
    }

    private static void PHASE23_CollectDBigramModel() throws InterruptedException, IOException, ClassNotFoundException {
        i2 = new InvertedIndex(
                (Int2IntOpenHashMap) deserialize(LOCALTERMFREQ),
                null,
                (long[]) deserialize(GLOBALSTATS),
                distance,
                true,
                DBIGRAMRAW,
                BIGRAM_SMALL_FILTER_SET,
                numThreads);
        //buildStructure(i2, numThreads, DBIGRAMRAW);
        //massiveBinaryMerge(new File(DBIGRAMRAW), DBIGRAMINDEX, true, DBIGRAMMETA);
        getModel(DBIGRAMINDEX, FILLEDBIGRAM, BIGRAMMETA);
    }


    private static void PHASE3_CollectBestChunks(){
        //greedySelection(budget, input, output, );
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

    private static void buildStructure(InvertedIndex i2, int numThreads, String dumpedPostings) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("");
        startBatteria(i2, 1, numThreads);
        serialize(Arrays.stream(i2.dmpPost).sum(), dumpedPostings);
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

    private static void finda() throws IOException {
        int currentTerm = -1;
        int [] previousBM25 = new int[3];
        int [] newBM25 = new int[3];
        int [] posting = new int[3];
        int max = -99;
        DataInputStream DIStream = getDIStream(UNIGRAMINDEX);

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

    public static void elaborateMe(String modelPath) throws IOException {
        Int2LongOpenHashMap posTListLength = (Int2LongOpenHashMap) deserialize(ACCESSMAP);
        BufferedWriter bw = getBuffWriter(METADATA+"me.csv");
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
        BufferedWriter bw = getBuffWriter(METADATA+"QI.CSV");
        bw.write(sb.toString());
        bw.close();
        br.close();


    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////

    private static void buildQualityModels() throws IOException, ClassNotFoundException {
        if(checkExistence(DBIGRAMQUALITYMODEL)) {
            //NewQualityModel.getModel(1, DBIGRAMINDEX, DBILENGTHS, DBIGRAMQUALITYMODEL);
            //NewQualityModel.getModel(DBIGRAMINDEX, DBILENGTHS, DBIGRAMQUALITYMODEL,"dw");
        }
        if(!checkExistence(HITQUALITYMODEL)){
            UnigramQualityModel.getUnigramQualityModel(1, HITINDEX, HITLENGTHS, HITQUALITYMODEL);
        }
        if(!checkExistence(UNIGRAMQUALITYMODEL)) {
            UnigramQualityModel.getUnigramQualityModel(1, UNIGRAMINDEX, UNILENGTHS, UNIGRAMQUALITYMODEL);
        }
    }

    private static void printModels() throws IOException, ClassNotFoundException {
        printQualityModel(DBIGRAMQUALITYMODEL);
        printQualityModel(HITQUALITYMODEL);
        printQualityModel(UNIGRAMQUALITYMODEL);
    }



    private static void checkExtraData() throws IOException {
        if(!checkExistence(HITSCORES))  getHitScore2();
        if(!checkExistence(BIG_FILTER_SET))  getBigFilterSet();
        if(!checkExistence(ACCESSMAP))  uniquePairs();

    }


//14k

    //30k
    /*/home/aalto/IdeaProjects/PredictiveIndex/aux/sort/bin/binsort --size 16 --length 12 --block-size=900000000  ./InvertedIndex.dat ./sortedInvertedIndex.dat*/
}
