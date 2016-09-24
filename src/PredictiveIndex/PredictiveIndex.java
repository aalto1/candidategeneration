package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RunnableFuture;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.lemurproject.kstem.KrovetzStemmer;
import static PredictiveIndex.InvertedIndex.*;
import com.google.code.externalsorting.*;
import static PredictiveIndex.VariableByteCode.encodeInterpolate;

import static PredictiveIndex.utilsClass.*;
import static PredictiveIndex.ExternalSort.*;

import org.lemurproject.kstem.Stemmer;
import sun.nio.cs.Surrogate;
import org.lemurproject.kstem.KrovetzStemmer.*;
import com.koloboke.collect.map.hash.HashObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import javax.sound.sampled.Line;
import com.koloboke.collect.map.hash.HashObjIntMaps.*;
import static PredictiveIndex.VariableByteCode.decodeInterpolate;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;



/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {
    static int counter = 1;
    static int counter2 = 1;
    private static String metadata = "/home/aalto/IdeaProjects/PredictiveIndex/data/metadata/";
    private static String qi = "/home/aalto/dio/";
    static final String clueweb09 = "/home/aalto/IdeaProjects/PredictiveIndex/data/clueweb/";
    static final String dataFold = "/home/aalto/IdeaProjects/PredictiveIndex/data/";
    static final String globalFold = dataFold+"global/";


    static int hit = 0;

    private static Object2IntMap<String> termMap;
    private static Int2ObjectOpenHashMap<String> termMap2;

    private static class MultiThread implements Runnable{
        private int threadNum;
        private InvertedIndex i2;
        private int phase;

        MultiThread(int threadNum, InvertedIndex i2, int phase){
            this.threadNum = threadNum;
            this.i2 = i2;
            this.phase = phase;
        }

        @Override
        public void run() {
            String threadFold = dataFold+threadNum+"/";
            System.out.println("Thread " + threadNum + "Started");
            try {
                if(this.phase==0)
                    i2.getClueWebMetadata(threadFold);
                else
                    i2.buildDBigramInvertedIndex(threadFold);

            }catch (IOException | ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /*/home/aalto/IdeaProjects/PredictiveIndex/aux/sort/bin/binsort --size 16 --length 12 --block-size=900000000  ./InvertedIndex.dat ./sortedInvertedIndex.dat*/
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        InvertedIndex i2;
        if (Files.exists(Paths.get(globalFold + "freqMap.bin"))) {
            i2 = new InvertedIndex((short[]) deserialize(globalFold + "freqMap"), (int[]) deserialize(globalFold + "stats"), globalFold + "rawInvertedIndex/");
        }else {
            i2 = new InvertedIndex(globalFold + "rawInvertedIndex/");
            startBatteria(i2,0);
            serialize(i2.globalFreqMap, globalFold+"freqMap");
            serialize(i2.globalStats, globalFold+"stats");
        }
        startBatteria(i2,1);
    }

    static void startBatteria(InvertedIndex i2, int phase) throws InterruptedException {
        System.out.println("Starting Batteria. Phase: " + phase);
        Thread [] threads = new Thread[4];
        for(int i = 0; i < threads.length; i++){
            threads[i] = new Thread(new MultiThread(i,i2,phase));
            threads[i].start();
        }

        for(int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
    }


    /* TEST CLASS
    Test class to cheeck if is possible to serialize and deserialize an Hashmap of Hashmaps*/

    private static void provaSer() throws IOException, ClassNotFoundException {
        Long2ObjectOpenHashMap<Int2IntMap> mappa = new Long2ObjectOpenHashMap<>();
        Int2IntMap mappetta1 = new Int2IntOpenHashMap();
        mappetta1.put(10, 10);
        mappetta1.put(20, 23);
        Int2IntMap mappetta2 = new Int2IntOpenHashMap();
        mappetta2.put(-114, 13);
        mappetta2.put(-87, -423);
        mappa.put((long) 1, mappetta1);
        mappa.put((long) 2, mappetta2);
        ObjectOutputStream OOStream = getOOStream("out.bin", true);
        OOStream.writeObject(mappa);
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = (Long2ObjectOpenHashMap<Int2IntMap>) (new ObjectInputStream(new FileInputStream("out.bin"))).readObject(); //**
        //System.out.println(fastQueryTrace.get((long) 1).get(10));
        System.out.println(fastQueryTrace.get((long) 2).get(-87));
        System.exit(1);
    }

    public static void uniquePairs() throws IOException{
        Long2IntMap map = new Long2IntOpenHashMap();
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/query/Q/million09_training"));
        String[] line = br.readLine().split(":")[1].split(" ");
        Integer [] terms ;
        int unique=0;
        while(line[0] != null){
            terms = new Integer[line.length];
            for(int i = 0; i< terms.length; i++) terms[i] = termMap.get(line[i]);
            for (long i : getCombinations(new LinkedList<Integer>(java.util.Arrays.asList(terms)),2)) {
                if(map.putIfAbsent(i,1)==null) unique++;
            }
            System.out.println(unique);
            line = br.readLine().split(":")[1].split(" ");
        }

    }

    /*Fetching termID-term Map*/

    private static void fetchTermMap2() throws IOException {
        System.out.println("Fetching TermID-Term map...");
        BufferedReader br = new BufferedReader( new FileReader("/home/aalto/dio/termIDs"));
        //BiMap termMap = HashBiMap.create();
        //Map<String, Integer> termMap = HashObjIntMaps.newMutableMap();
        termMap2 = new Int2ObjectOpenHashMap<>();
        String line;
        String [] splittedLine;
        int k =1;
        while((line = br.readLine()) != null){
            splittedLine = line.split(" ");
            termMap2.put(k, splittedLine[1]);
            k++;
            if(k % 10000000 == 0) System.out.println(k);
        }
        System.out.println("Map Completed!");
    }

    /*Fetching term-termID Map*/

    private static void fetchTermMap() throws IOException {
        System.out.println("Fetching Term-TermID map...");
        BufferedReader br = new BufferedReader( new FileReader("/home/aalto/dio/termIDs"));
        //BiMap termMap = HashBiMap.create();
        //Map<String, Integer> termMap = HashObjIntMaps.newMutableMap();
        termMap = new Object2IntOpenHashMap<>();

        String line;
        String [] splittedLine;
        int k =1;
        while((line = br.readLine()) != null){
            splittedLine = line.split(" ");
            termMap.put(splittedLine[1], k);
            if(k % 10000000 == 0) System.out.println(k);
            k++;
        }
        System.out.println("Map fetched!");
    }

    private static void getTermMap() throws IOException, ClassNotFoundException {
        if(Files.exists(Paths.get(metadata+"termMap.bin"))){
            ObjectInputStream iStream = new ObjectInputStream(new FileInputStream(metadata+"termMap.bin"));
            //return (Map<String, Integer>) iStream.readObject();
        }else{
            fetchTermMap();
        }
    }

    /*This function parse the document and build an int[][] to get O(1) access to the top500 of each document*/

    private static int[][] getTopKMatrix(int[][] topMatrix, BufferedReader br) throws IOException {
        System.out.println("Building TopK matrix...");
        String line;
        String [] split;
        int query = 0;
        int pointer;
        LinkedList<Integer> auxTopK = new LinkedList<>();
        while((line = br.readLine()) != null){
            split = line.split(" ");
            pointer = Integer.valueOf(split[0]);
            if(query!=pointer) {
                topMatrix[query] = Ints.toArray(auxTopK);
                auxTopK.clear();
                query = pointer;
            }
            auxTopK.addLast(Integer.valueOf(split[1]));
        }
        System.out.println("TopK matrix built.");
        return topMatrix;
    }


    private static void getSubsets(List<Integer> superSet, int k, int idx, Set<Integer> current,List<Set<Integer>> solution) {
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
        getSubsets(superSet, k, idx+1, current, solution);
        current.remove(x);
        //"guess" x is not in the subset
        getSubsets(superSet, k, idx+1, current, solution);
    }

    private static long[] getCombinations(List<Integer> superSet, int k) {
        List<Set<Integer>> res = new ArrayList<>();
        superSet.removeIf(Objects::isNull);
        getSubsets(superSet, k, 0, new HashSet<>(), res);
        long [] combo = new long[res.size()];
        int [] pair;
        int p = 0;
        //System.out.println(res);
        for(Set set : res){
            pair = Ints.toArray(set);
            java.util.Arrays.parallelSort(pair);
            combo[p] = getPair(pair[0],pair[1]);
            p++;
        }
        return combo;
    }


    private static long[] getQueryBigrams(String [] queryTerms){
        //this method return all the combination of the docID in the document

        LinkedList<Integer> queryInt = new LinkedList<>();
        int termID;
        String stemmedTerm;
        KrovetzStemmer stemmer = new KrovetzStemmer();

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i <  queryTerms.length; i++) {
            stemmedTerm = stemmer.stem(queryTerms[i]);
            //termID = termMap.get(stemmedTerm);
            //queryInt.add(termID);
            if(termMap.get(stemmedTerm)!=null) queryInt.add(termMap.get(stemmedTerm));//System.out.println(stemmedTerm+"-"+queryTerms[i]);
        }
        //We take every combination of our query terms. We save them in a long array using bit-shifting
        return getCombinations(queryInt,2);
    }

    private static int[] getQueryTopkDocIDs(String [] topk){
        //declare a new object every time
        int [] topkInt = new int[topk.length];

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i <  topk.length; i++) {
            topkInt[i] = 1; //docIDmap.get(String);
        }
        return topkInt;
    }

    private static void addTopK(Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace, long bigram, int[] topK){
        Int2IntMap auxMap = fastQueryTrace.get(bigram);
        if(auxMap != null){
            for (int doc: topK){
                if(auxMap.putIfAbsent(doc,1) != null){
                    auxMap.merge(doc,1,Integer::sum);
                    //System.out.println(auxMap.get(doc));
                }
            }
        }else{
            auxMap = new Int2IntOpenHashMap();
            for (int doc: topK){
                auxMap.put(doc,1);
            }
            fastQueryTrace.put(bigram, auxMap);
        }
    }

    /*We reformat the Query trace in a way that we can perform fast lookup while scanning the
    * compressed inverted index.
    *
    * OPEN ISSUES:
    * - Check the format of the query trace in a way that you can see if the split is ok*/
    public static Long2ObjectOpenHashMap<Int2IntMap> buildFastQueryTrace() throws IOException {
        int [][] topKMatrix = new int[173800][];
        BufferedReader br = new BufferedReader( new FileReader("/home/aalto/dio/query2/complexRankerResultsTraining"));
        getTopKMatrix(topKMatrix, br);
        br =  new BufferedReader( new FileReader("/home/aalto/dio/query2/complexRankerResultsTesting"));
        getTopKMatrix(topKMatrix, br);
        br = new BufferedReader(new FileReader("/home/aalto/dio/query/Q/million09_training"));
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = new Long2ObjectOpenHashMap<>();
        String line;
        String [] splittedLine;
        long [] queryBigrams;
        int [] topK;
        while((line = br.readLine()) != null) {
            splittedLine = line.split(":");
            queryBigrams = getQueryBigrams(splittedLine[1].split(" "));     //**
            topK = topKMatrix[Integer.valueOf(splittedLine[0])];          //**
            //System.out.println(topK+"-"+Integer.valueOf(line[0]));
            if(topK!=null){
                for (long bigram : queryBigrams) {
                    addTopK(fastQueryTrace, bigram, topK);
                }
            }
        }
        /*HashMap<Integer,Integer> mappa;
        for ( long x: fastQueryTrace.keySet()) {
            mappa = fastQueryTrace.get(x);
            for(long y : mappa.keySet()) System.out.println("Pair: "+ x +". Key: " + y + ". Value: " + mappa.get(y));
        }*/
        serialize(fastQueryTrace, metadata+"fastQueryTrace");
        return fastQueryTrace;
    }


    /*This functions returns buckets ranges given a length. The ranges-max is hardcoded.*/

    private static int[] computelRanges(double lenRule){
        lenRule = 1.1;
        LinkedList<Integer> lenBuckets = new LinkedList<>();
        for (int i = 4; i < 50220423; i += i*lenRule) {
            lenBuckets.addLast(i);
        }
        return Ints.toArray(lenBuckets);
    }

    /*This functions returns buckets ranges given a rank. The ranges-max is hardcoded*/

    private static int[] computerRanges(double rankRule){
        rankRule = 1.4;
        LinkedList<Integer> rankBuckets = new LinkedList<>();
        for (int i = 11; i < Integer.MAX_VALUE ; i += i*rankRule) {
            rankBuckets.addLast(i);
        }
        rankBuckets.addLast(Integer.MAX_VALUE);
        return Ints.toArray(rankBuckets);
    }

    /*Getter for the bucket length*/

    private static int getLenBucket(int len, int[] lenBuckets){
        int i;
        for (i = 0; lenBuckets[i] < len; i++);
        return i;
    }


    /* Since the the rank is monotonically WHAAAT?? we don't want to scan the array everytime
     * with the for loop. Instead we start immediately from the previous rank bucket in a way that
     * if at this iteration we did not change bucket range the operation is almost free O(1).
     *
     * OPEN ISSUES:
    * - */

    private static int getRankBucket(int nowRank, int rank, int[] rankBuckets){
        int i;
        //System.out.println(rank);
        for (i = nowRank; rankBuckets[i] < rank; i++);
        return i;
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
    public static int[][][] getQualityModel() throws IOException, ClassNotFoundException {
        int maxBM25 =0;
        int minBM25=0;
        int maxLength=0;
        int [] lRanges = computelRanges(1.1);
        int [] rRanges = computerRanges(1.4);
        int [][][] qualityModel = new int[lRanges.length][rRanges.length][2];
        //BufferedReader br = new BufferedReader(new FileReader("readIndexInfo"));
        DataInputStream inStream = new DataInputStream( new BufferedInputStream(new FileInputStream(dPath + "/sortedInvertedIndex.bin")));
        ObjectInputStream obInStream = getOIStream(metadata+"fastQueryTrace", true);
        System.out.println("Fast Query Trace fetched!\nProcessing Inverted Index...");
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = (Long2ObjectOpenHashMap<Int2IntMap>) obInStream.readObject(); //conversion seems to work
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        auxPostingList.add(3);
        LongSet duplicate= new LongOpenHashSet();
        long numberOfPostingLists = 0;
        int [] posting;
        int [] currentPair = new int[]{-1,-1};
        while(true){
            posting = getEntry(inStream);
            if(posting[0] ==-1) break;
            if(posting[0] != currentPair[0] | posting[1] != currentPair[1]){
                //System.out.println(auxPostingList.size());
                if(duplicate.contains(getPair(currentPair[0], currentPair[1]))) System.out.println(getPair(currentPair[0], currentPair[1]));
                else duplicate.add(getPair(currentPair[0], currentPair[1]));
                if(fastQueryTrace.get(getPair(currentPair[0], currentPair[1]))!=null){
                    //System.out.println(getPair(currentPair[0], currentPair[1]));
                    //qualityModel = processPostingList(Ints.toArray(auxPostingList), qualityModel, fastQueryTrace.get(getPair(currentPair[0], currentPair[1])), rRanges, lRanges);
                    //System.out.println(fastQueryTrace.get(getPair(currentPair[0], currentPair[1])));
                }
                numberOfPostingLists++;
                auxPostingList.clear();
                currentPair[0]= posting[0];
                currentPair[1]= posting[1];
            }
            auxPostingList.addLast(posting[2]); //BM25
            auxPostingList.addLast(posting[3]); //docid
        }
        System.out.println("Posting List: " + numberOfPostingLists);
        System.out.println("max: " + maxBM25 + ". min: " + minBM25 + ". len: " + maxLength);
        ObjectOutputStream oStream = getOOStream(metadata+"qualityModel.bin",true);
        oStream.writeObject(qualityModel);
        return qualityModel;
    }

    private static int[][][] processPostingList(int [] postingList, int[][][] qualityModel, Int2IntMap aggregatedTopK, int [] rRanges, int [] lRanges){
        int lenBucket = getLenBucket(postingList.length, lRanges);
        int rankBucket = 0;
        int score;
        int term;
        for (int i = 0; i < postingList.length ; i += 2) {
            score = postingList[i];
            term = postingList[i+1];
            int increment = aggregatedTopK.get(term);
            if(increment>0){
                System.out.println(aggregatedTopK.get(term));
                hit+=increment;
                rankBucket = getRankBucket(rankBucket, score, rRanges);
                //bucket hit by this posting
                qualityModel[lenBucket][rankBucket][0] += increment;
                for (int j = 0; j < rankBucket + 1; j++) {
                    //previous buckets hit by this posting
                    qualityModel[lenBucket][j][1] += increment;
                }
            }

        }
        return qualityModel;
    }

    private static int[] getEntry(DataInputStream dataStream) throws IOException {
        //OK
        int [] aux = new int[4];
        try{
            for(int k = 0; k<aux.length; k++) aux[k] = dataStream.readInt();
            counter += 1;
            if(counter % 200000000 == 0) System.out.println("Up to postings #" + (counter));
            return aux;
        }catch(EOFException exception){
            System.out.println("Fetching Time: " + (System.currentTimeMillis() - now) + "ms");
            aux[0] = -1;
            return aux;
        }
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