package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.doubles.Double2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.lemurproject.kstem.KrovetzStemmer;

import static PredictiveIndex.ExternalSort.massiveBinaryMerge;
import static PredictiveIndex.ExternalSort.sortSmallInvertedIndex;
import static PredictiveIndex.InvertedIndex.*;


import static PredictiveIndex.utilsClass.*;



/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {
    static long counter = 1;
    static int counter2 = 1;
    private static String metadata = "/home/aalto/IdeaProjects/PredictiveIndex/data/metadata/";
    private static String qi = "/home/aalto/dio/";
    static final String clueweb09 = "/home/aalto/IdeaProjects/PredictiveIndex/data/clueweb/";
    static final String dataFold = "/home/aalto/IdeaProjects/PredictiveIndex/data/";
    static final String globalFold = dataFold + "global/";


    static int hit = 0;

    private static Object2IntMap<String> termMap;
    private static Int2ObjectOpenHashMap<String> termMap2;

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
            String threadFold = dataFold + threadNum + "/";
            System.out.println("Thread " + threadNum + "Started");
            try {
                if (this.phase == 0)
                    i2.getClueWebMetadata(threadFold);
                else
                    i2.buildDBigramInvertedIndex(threadFold, threadNum);

            } catch (IOException | ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /*/home/aalto/IdeaProjects/PredictiveIndex/aux/sort/bin/binsort --size 16 --length 12 --block-size=900000000  ./InvertedIndex.dat ./sortedInvertedIndex.dat*/
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
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
        getQualityModel(globalFold+"invertedIndex.bin");
        //printQualityModel(metadata+"qualityModel");

        //convertProbabilities();
        //getFinalModel(globalFold+"invertedIndex.bin",metadata+"qualityModelfinal");


        System.exit(1);


        InvertedIndex i2;
        if (Files.exists(Paths.get(globalFold + "freqMap.bin"))) {
            i2 = new InvertedIndex((int[]) deserialize(globalFold + "freqMap"), (long[]) deserialize(globalFold + "stats"), globalFold + "rawInvertedIndex/");
        } else {
            i2 = new InvertedIndex(globalFold + "rawInvertedIndex/");
            startBatteria(i2, 0);
            serialize(i2.globalFreqMap, globalFold + "freqMap");
            serialize(i2.globalStats, globalFold + "stats");
        }
        startBatteria(i2, 1);
        serialize(i2.dumpMap, globalFold + "/dumped" );

    }

    private static void printQualityModel(String s) throws IOException, ClassNotFoundException {
        int[][][] qm = (int[][][]) getOIStream(s, false).readObject();
        int [] rRange = computerRanges(1.4, 1, 400000000);
        float HP;
        float [][][] bufferQM = new float[qm.length][qm[0].length][2];
        float [][] finalQM = new float[qm.length][qm[0].length];
        long [][] bucketOrder = new long[bufferQM.length][bufferQM[0].length-1];
        System.out.println(bufferQM[0].length);


        int lowerBound =0;
        int upperBound =0;
        for (int i = 0; i < bufferQM.length; i++) {
            for (int j = 0; j < bufferQM[0].length-1; j++) {
                HP = (float) ((qm[i][j][0]*1.0)/(qm[i][j][1]));
                //System.out.print(HP+"\t\t");
                if(Float.isNaN(HP) | Float.isInfinite(HP))
                    bufferQM[i][j][0] =0;
                else
                    bufferQM[i][j][0] =  HP;
                bufferQM[i][j][1] = j;
                //System.out.print(bufferQM[i][j][0] +"\t");
            }


            Arrays.sort(bufferQM[i], new Comparator<float[]>() {
                @Override
                public int compare(float[] o1, float[] o2) {
                    return Float.compare(o2[0],o1[0]);
                }
            });

            /***/

            //System.out.println(Arrays.deepToString(bufferQM[i]));
            int end = 0;
            //System.out.println(rRange[0] +"," + rRange[1]);
            for (int j = 0; j < bufferQM[i].length-1; j++) {

                lowerBound = (int) bufferQM[i][j][1];
                upperBound = (int) bufferQM[i][j][1]+1;
                if(upperBound==22) end = 1;
                if(upperBound < 22) {
                    finalQM[i][j] = bufferQM[i][j][0];
                    bucketOrder[i][j - end] = getPair(rRange[lowerBound], rRange[upperBound]);
                    //System.out.println(rRange[lowerBound] +" , "+ rRange[upperBound]);
                }
            }
            //System.out.println(Arrays.toString(bucketOrder[i]));
            for (long a: bucketOrder[i]) System.out.print(Arrays.toString(getTerms(a)) +" ");
            System.out.println();
            //System.exit(1);

        }

        //serialize(bucketOrder,   s+"bucketOrder");
        //serialize(finalQM,       s+"final");
    }

    static void startBatteria(InvertedIndex i2, int phase) throws InterruptedException {
        System.out.println("Starting Batteria. Phase: " + phase);
        Thread[] threads = new Thread[4];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new MultiThread(i, i2, phase));
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }


    public static void uniquePairs() throws IOException {
        Long2IntOpenHashMap queryOccurences = new Long2IntOpenHashMap();
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/query/Q/million09_training"));
        String line;
        String[] stringTerms;
        Integer term;
        LinkedList<Integer> terms = new LinkedList<>();
        KrovetzStemmer stemmer = new KrovetzStemmer();
        for (line = br.readLine(); line != null; line = br.readLine()) {
            stringTerms = line.split(":")[1].split(" ");
            for (int i = 0; i < stringTerms.length; i++) {
                term = termMap.get(stringTerms[i]);
                if (term != null) {
                    terms.addLast(term);
                    if (queryOccurences.putIfAbsent(Long.valueOf(term), 1) != null)
                        queryOccurences.merge(Long.valueOf(term), 1, Integer::sum);
                }
            }
            for (long i : getCombinations(terms, 2)) {
                if (queryOccurences.putIfAbsent(i, 1) != null) queryOccurences.merge(i, 1, Integer::sum);
            }
            terms.clear();
            //System.out.println(queryOccurences.size());
        }
        serialize(queryOccurences, metadata + "queryOccurences");


    }

    /*Fetching termID-term Map*/

    private static void fetchTermMap2() throws IOException {
        System.out.println("Fetching TermID-Term map...");
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/termIDs"));
        //BiMap termMap = HashBiMap.create();
        //Map<String, Integer> termMap = HashObjIntMaps.newMutableMap();
        termMap2 = new Int2ObjectOpenHashMap<>();
        String line;
        String[] splittedLine;
        int k = 1;
        while ((line = br.readLine()) != null) {
            splittedLine = line.split(" ");
            termMap2.put(k, splittedLine[1]);
            k++;
            if (k % 10000000 == 0) System.out.println(k);
        }
        System.out.println("Map Completed!");
    }

    /*Fetching term-termID Map*/

    private static void fetchTermMap() throws IOException {
        System.out.println("Fetching Term-TermID map...");
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/IdeaProjects/PredictiveIndex/data/source/termIDs"));
        //BiMap termMap = HashBiMap.create();
        //Map<String, Integer> termMap = HashObjIntMaps.newMutableMap();
        termMap = new Object2IntOpenHashMap<>();

        String line;
        String[] splittedLine;
        int k = 1;
        while ((line = br.readLine()) != null) {
            splittedLine = line.split(" ");
            termMap.put(splittedLine[1], k);
            if (k % 10000000 == 0) System.out.println(k);
            k++;
        }
        System.out.println("Map fetched!");
    }

    private static void getTermMap() throws IOException, ClassNotFoundException {
        if (Files.exists(Paths.get(metadata + "termMap.bin"))) {
            ObjectInputStream iStream = new ObjectInputStream(new FileInputStream(metadata + "termMap.bin"));
            //return (Map<String, Integer>) iStream.readObject();
        } else {
            fetchTermMap();
        }
    }

    /*This function parse the document and build an int[][] to get O(1) access to the top500 of each document*/

    private static int[][] getTopKMatrix(int[][] topMatrix, BufferedReader br) throws IOException {
        System.out.println("Building TopK matrix...");
        String line;
        String[] split;
        int query = 0;
        int pointer;
        int top10 = 0;
        LinkedList<Integer> auxTopK = new LinkedList<>();
        while ((line = br.readLine()) != null) {
            split = line.split(" ");
            pointer = Integer.valueOf(split[0]);
            if (query != pointer) {
                topMatrix[query] = Ints.toArray(auxTopK);
                auxTopK.clear();
                query = pointer;
                top10 =0;
            }
            if(top10<10) {
                auxTopK.addLast(Integer.valueOf(split[1]));
                top10++;
            }
        }
        System.out.println("TopK matrix built.");
        return topMatrix;
    }


    private static void getSubsets(List<Integer> superSet, int k, int idx, Set<Integer> current, List<Set<Integer>> solution) {
        //successful stop clause
        if (current.size() == k) {
            solution.add(new HashSet<>(current));
            return;
        }
        //unseccessful stop clause
        if (idx == superSet.size()) return;
        Integer x = superSet.get(idx);
        current.add(x);
        //"guess" x is in the subset
        getSubsets(superSet, k, idx + 1, current, solution);
        current.remove(x);
        //"guess" x is not in the subset
        getSubsets(superSet, k, idx + 1, current, solution);
    }

    private static long[] getCombinations(List<Integer> superSet, int k) {
        List<Set<Integer>> res = new ArrayList<>();
        superSet.removeIf(Objects::isNull);
        getSubsets(superSet, k, 0, new HashSet<>(), res);
        long[] combo = new long[res.size()];
        int[] pair;
        int p = 0;
        //System.out.println(res);
        for (Set set : res) {
            pair = Ints.toArray(set);
            java.util.Arrays.parallelSort(pair);
            combo[p] = getPair(pair[0], pair[1]);
            p++;
        }
        return combo;
    }


    private static long[] getQueryBigrams(String[] queryTerms) {
        //this method return all the combination of the docID in the document

        LinkedList<Integer> queryInt = new LinkedList<>();
        int termID;
        String stemmedTerm;
        KrovetzStemmer stemmer = new KrovetzStemmer();

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i < queryTerms.length; i++) {
            //stemmedTerm = stemmer.stem(queryTerms[i]);
            //termID = termMap.get(stemmedTerm);
            //queryInt.add(termID);
            //if(termMap.get(stemmedTerm)!=null) queryInt.add(termMap.get(stemmedTerm));//System.out.println(stemmedTerm+"-"+queryTerms[i]);
            queryInt.add(termMap.get(queryTerms[i]));
        }
        //We take every combination of our query terms. We save them in a long array using bit-shifting
        return getCombinations(queryInt, 2);
    }

    private static int[] getQueryInts(String[] queryTerms) {
        int[] queryInt = new int[queryTerms.length];
        for (int i = 0; i < queryTerms.length; i++) queryInt[i] = termMap.get(queryTerms[i]);
        return queryInt;
    }

    private static int[] getQueryTopkDocIDs(String[] topk) {
        //declare a new object every time
        int[] topkInt = new int[topk.length];

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i < topk.length; i++) {
            topkInt[i] = 1; //docIDmap.get(String);
        }
        return topkInt;
    }

    private static Long2ObjectOpenHashMap<Int2IntMap> addTopK(Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace, long bigram, int[] topK) {
        Int2IntMap auxMap = fastQueryTrace.get(bigram);
        if (auxMap != null) {
            for (int doc : topK) {
                if (auxMap.putIfAbsent(doc, 1) != null) {
                    auxMap.merge(doc, 1, Integer::sum);
                    fastQueryTrace.put(bigram, auxMap);
                    //System.out.println(auxMap.get(doc));
                }
            }
        } else {
            auxMap = new Int2IntOpenHashMap();
            for (int doc : topK) {
                auxMap.put(doc, 1);
            }
            fastQueryTrace.put(bigram, auxMap);
        }
        return fastQueryTrace;
    }

    /*We reformat the Query trace in a way that we can perform fast lookup while scanning the
    * compressed inverted index.
    *
    * OPEN ISSUES:
    * - Check the format of the query trace in a way that you can see if the split is ok*/
    public static Long2ObjectOpenHashMap<Int2IntMap> buildFastQueryTrace() throws IOException {
        int[][] topKMatrix = new int[173800][];
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/query2/complexRankerResultsTraining"));
        getTopKMatrix(topKMatrix, br);
        br = new BufferedReader(new FileReader("/home/aalto/dio/query2/complexRankerResultsTesting"));
        getTopKMatrix(topKMatrix, br);
        br = new BufferedReader(new FileReader("/home/aalto/dio/query/Q/million09_training"));
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = new Long2ObjectOpenHashMap<>();
        String line;
        String[] splittedLine;
        long[] queryBigrams;
        int[] topK;
        while ((line = br.readLine()) != null) {
            splittedLine = line.split(":");
            queryBigrams = getQueryBigrams(splittedLine[1].split(" "));     //**
            topK = topKMatrix[Integer.valueOf(splittedLine[0])];          //**
            //System.out.println(topK+"-"+Integer.valueOf(line[0]));
            if (topK != null) {
                for (long bigram : queryBigrams) {
                    fastQueryTrace = addTopK(fastQueryTrace, bigram, topK);
                }
            }
        }
        /*HashMap<Integer,Integer> mappa;
        for ( long x: fastQueryTrace.keySet()) {
            mappa = fastQueryTrace.get(x);
            for(long y : mappa.keySet()) System.out.println("Pair: "+ x +". Key: " + y + ". Value: " + mappa.get(y));
        }*/
        serialize(fastQueryTrace, metadata + "fastQueryTrace");
        return fastQueryTrace;
    }


    /*This functions returns buckets ranges given a length. The ranges-max is hardcoded.*/

    private static int[] computelRanges(double lenRule) {
        lenRule = 1.1;
        LinkedList<Integer> lenBuckets = new LinkedList<>();
        for (int i = 4; i < 50220423; i += i * lenRule) {
            lenBuckets.addLast(i);
        }
        System.out.println(lenBuckets);
        return Ints.toArray(lenBuckets);
    }

    /*This functions returns buckets ranges given a rank. The ranges-max is hardcoded*/

    private static int[] computerRanges(double rankRule, int min, int max) {
        rankRule = 1.4;
        LinkedList<Integer> rankBuckets = new LinkedList<>();
        rankBuckets.add(0);
        for (int i = 11; i < max; i += i * rankRule) {
            rankBuckets.addLast(i);
        }
        rankBuckets.addLast(max);
        System.out.println(rankBuckets);
        return Ints.toArray(rankBuckets);
    }

    /*Getter for the bucket length*/

    private static int getLenBucket(int len, int[] lenBuckets) {
        int i;
        for (i = 0; lenBuckets[i] < len; i++) ;
        return i;
    }


    /* Since the the rank is monotonically WHAAAT?? we don't want to scan the array everytime
     * with the for loop. Instead we start immediately from the previous rank bucket in a way that
     * if at this iteration we did not change bucket range the operation is almost free O(1).
     *
     * OPEN ISSUES:
    * - */

    private static int getRankBucket(int nowRank, int rank, int[] rankBuckets) {
        int i;
        //System.out.println(rank);
        for (i = nowRank; rankBuckets[i] < rank; i++) ;
        return i;
    }

    private static long[] diff(int [] bcks){
        long [] bcksSize = new long [bcks.length-1];
        for (int i = 0; i < bcksSize.length ; i++) {
            bcksSize[i] = bcks[i+1]-bcks[i];
        }
        return  bcksSize;

    }

    /*The quality model is a small 3D matrix:
    * X = Rows      = length buckets
    * Y = Columns   = rank buckets
    * Z = Counters  = [0] hit counter - [1] touch counter (fixed size of 2)
    *
    * 0) PairID
    * 1) Number of Varbytes to read
    * 2) Number of documents
    *
    * OPEN ISSUES:
    * - Make it cleaner */
    public static long[][][] getQualityModel(String I2) throws IOException, ClassNotFoundException {
        int maxBM25 = 367041008;
        int minBM25 = 1;//80992396;
        int maxLength = 0;
        int[] lRanges = computelRanges(1.1);
        int[] rRanges = computerRanges(1.4, minBM25, maxBM25);
        long[] deltaRanges = diff(rRanges);
        long[][][] qualityModel = new long[lRanges.length][rRanges.length][2];
        //BufferedReader br = new BufferedReader(new FileReader("readIndexInfo"));
        final Long2IntOpenHashMap UBOcc = (Long2IntOpenHashMap) getOIStream(metadata + "queryOccurences", true).readObject();
        final Long2IntOpenHashMap dumped = (Long2IntOpenHashMap) getOIStream(globalFold+"/dumped", true).readObject();

        DataInputStream inStream = new DataInputStream(new BufferedInputStream(new FileInputStream(I2)));
        ObjectInputStream obInStream = getOIStream(metadata + "fastQueryTrace", true);
        System.out.println("Fast Query Trace fetched!\nProcessing Inverted Index...");
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = (Long2ObjectOpenHashMap<Int2IntMap>) obInStream.readObject(); //conversion seems to work
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        auxPostingList.add(3);
        LongSet duplicate = new LongOpenHashSet();
        long numberOfPostingLists = 0;
        int[] posting;
        int[] currentPair = new int[]{-1, -1};
        long pair = -1;
        int increment;
        int lbucket;
        int range;
        DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        while (true) {
            posting = getEntry(inStream);
            if (posting[0] == -1) break;

            if (posting[0] != currentPair[0] | posting[1] != currentPair[1]) {
                pair = getPair(currentPair[0], currentPair[1]);
                //System.out.println(auxPostingList.size());

                /*if (fastQueryTrace.containsKey(pair)) {
                    lbucket = getLenBucket(auxPostingList.size() / 2 + dumped.get(pair), lRanges);
                    increment = UBOcc.get(currentPair[0]) + UBOcc.get(currentPair[1]) + UBOcc.get(currentPair[0]);
                    range = getRankBucket(0, auxPostingList.size() / 2, rRanges);
                    for (int k = 0; k < range; k++) {
                        //System.out.println(size);
                        qualityModel[lbucket][k][1] += increment*deltaRanges[k];
                    }
                    qualityModel = processPostingList(Ints.toArray(auxPostingList), qualityModel, fastQueryTrace.get(pair), rRanges, lbucket);

                }*/
                DOStream.writeLong(pair);
                DOStream.writeInt(auxPostingList.size() / 2 + dumped.get(pair));
                auxPostingList.clear();
                currentPair[0] = posting[0];
                currentPair[1] = posting[1];
            }/*
            if(posting[2]< minBM25)
                minBM25 = posting[2];
            else if(posting[2]>maxBM25) {
                maxBM25 = posting[2];
            }
            */

            auxPostingList.addLast(posting[2]); //BM25
            auxPostingList.addLast(posting[3]); //docid

        }
        DOStream.close();
        System.out.println("Posting List: " + numberOfPostingLists);
        System.out.println("max: " + maxBM25 + ". min: " + minBM25 + ". len: " + maxLength);
        serialize(qualityModel, metadata+"qualityModel");
        return qualityModel;
    }

    private static long[][][] processPostingList(int[] postingList, long[][][] qualityModel, Int2IntMap aggregatedTopK, int[] rRanges, int lbucket) {
        int rankBucket = 0;
        int score;
        int term;
        for (int i = 0; i < postingList.length - 1; i += 2) {
            score = postingList[i];
            term = postingList[i + 1];
            int increment = aggregatedTopK.get(term);
            if (increment > 0) {
                System.out.println(hit);
                hit += increment;
                if (hit % 10000 == 0 & hit % 100000 != 0) System.out.print(hit + " ");
                if (hit % 100000 == 0) System.out.println(hit);
                rankBucket = getRankBucket(rankBucket, i / 2, rRanges);
                //bucket hit by this posting
                qualityModel[lbucket][rankBucket][0] += increment;
            }

        }
        return qualityModel;
    }

    private static int[] getEntry(DataInputStream dataStream) throws IOException {
        //OK
        int[] aux = new int[4];
        try {
            for (int k = 0; k < aux.length; k++) aux[k] = dataStream.readInt();
            counter += 1;
            if (counter % 200000000 == 0) System.out.println("Up to postings #" + (counter));
            return aux;
        } catch (EOFException exception) {
            System.out.println("Fetching Time: " + (System.currentTimeMillis() - now) + "ms");
            aux[0] = -1;
            return aux;
        }
    }

    static void convertProbabilities() throws IOException {
        String line;
        long pair =0;
        String [] records;
        LongOpenHashSet relevantPairs = new LongOpenHashSet();
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/query/LM/bigram_info_million09"));
        BufferedWriter bw = new BufferedWriter(new FileWriter(metadata+"pm.csv"));
        fetchTermMap();
        for (line = br.readLine(); line != null; line = br.readLine()){
            records = line.split(" ");
            try{
                pair = getPair(termMap.get(records[0]),termMap.get(records[1]));
                bw.write(pair+","+records[3]);
                bw.newLine();
                relevantPairs.add(pair);
            }catch (NullPointerException e){
                System.out.println(termMap.get(records[0])+","+termMap.get(records[1]));
            }

        }
        serialize(relevantPairs, metadata+"relevantPairs");
        bw.close();
        br.close();
    }

    static Long2FloatOpenHashMap fetchBigramProbabilities() throws IOException {
        Long2FloatOpenHashMap probabilityModel = new Long2FloatOpenHashMap();
        BufferedReader br = new BufferedReader(new FileReader(metadata+"pm.csv"));
        String line;
        String [] records;
        long pair;
        for(line = br.readLine(); line!=null; line = br.readLine()){
            records = line.split(",");
            //System.out.println(line);
            probabilityModel.put(Long.valueOf(records[0]).longValue(), Float.valueOf(records[1]).longValue());
        }
        return probabilityModel;
    }

    static void sortLM(){

    }

    static void pairLengthProbability() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/query/LM/bigram_info_million09"));
        BufferedWriter bw = new BufferedWriter(new FileWriter(metadata+"pm.csv"));
        String line;
        String [] records;
        long pair;
        for (line = br.readLine(); line != null; line = br.readLine()){
            records = line.split(" ");

        }
    }



    /*static Long2ObjectOpenHashMap<float[]> getFinalModel(String I2, String qualityModel) throws IOException, ClassNotFoundException {
        int [] posting;
        int pointer = 0;
        int [] currentPair= new int[2];
        float [][] qm = (float[][]) deserialize(qualityModel);
        long [][] finalModel = new long[11050140][10];

        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int [] lRanges = computelRanges(1.4);
        long pair;
        float [] quality = new float[22];

        DataInputStream inStream = new DataInputStream( new BufferedInputStream(new FileInputStream(I2)));
        final Long2IntOpenHashMap dumped = (Long2IntOpenHashMap) getOIStream(globalFold+"/dumped/finalDump", true).readObject();

        Long2LongRBTreeMap model =
        Long2FloatOpenHashMap pMap= fetchBigramProbabilities();

        while(true){
            posting = getEntry(inStream);
            if(posting[0] ==-1) break;

            if(posting[0] != currentPair[0] | posting[1] != currentPair[1]){
                pair = getPair(currentPair[0], currentPair[1]);

                quality = scalarPerArray(pMap.get(pair), qm[getLenBucket(auxPostingList.size() / 2 + dumped.get(pair), lRanges)]);
                finalModel[pointer][0] = pair;

                //for (int i = 1; i < quality.length ; i++) finalModel[pointer][i] = quality[i-1];


                }

                auxPostingList.clear();
                currentPair[0]= posting[0];
                currentPair[1]= posting[1];
            }
        System.out.println(model.size());
        serialize(model, globalFold+"finalmodel");
        return model;
    }*/

    static void greedySelection(Long2ObjectOpenHashMap finalModel){
        Long2ObjectOpenHashMap<float[]> model = (Long2ObjectOpenHashMap<float[]>) deserialize("f");

    }

    static void externalSort(){

    }




}





/*We get the global statistics of the collection (fetch from memory if present, compute them if the opposite)
        * and than build the pair-distance from memory.*/
//fetchTermMap();
//buildFastQueryTrace();
//aggregateHITS("/home/aalto/dio/hit/ben/cw09b.100K_AND2.csv.tar.gz");
//getQualityModel();
//binaryMassiveSort(dPath + "/InvertedIndex.bin", dPath + "/tmp/", (int) (5*Math.pow(10,9)));
//readFiles(new File("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/tmp/"), "/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.bin");
//massiveBinaryMerge(new File("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/tmp/"), "/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.bin");
//testMassiveBinaryMerge();
//testMassiveBinaryMerge2(new File("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/tmp/"));
//tryMap();
//splitCollection(info);
//System.exit(1);









        /*
        if (Files.exists(Paths.get(fPath+".bin"))) {
            ps = new InvertedIndex((Int2IntMap) deserialize(fPath), (int[]) deserialize(sPath));
        }else {
            ps = new InvertedIndex();
            ps.getClueWebMetadata(info);
        }
        //ps.threads();
        ps.doc = 0;
        ps.buildDBigramInvertedIndex(info);
        //fetchTermMap();
        //buildFastQueryTrace();*/