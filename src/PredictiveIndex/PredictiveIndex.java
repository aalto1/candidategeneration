package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.primitives.Ints;
import com.koloboke.collect.map.hash.HashObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import sun.nio.cs.Surrogate;

import javax.sound.sampled.Line;
import com.koloboke.collect.map.hash.HashObjIntMaps.*;

import static PredictiveIndex.InvertedIndex.*;
import static PredictiveIndex.VariableByteCode.decodeInterpolate;
import static PredictiveIndex.VariableByteCode.encodeInterpolate;


/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {
    static int counter = 1;
    static int counter2 = 1;
    private static String metadata = "/home/aalto/IdeaProjects/PredictiveIndex/data/metadata/";
    private static String qi = "/home/aalto/dio/";

    private static Object2IntMap<String> termMap;

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*We get the global statistics of the collection (fetch from memory if present, compute them if the opposite)
        * and than build the pair-distance from memory.*/
        //superMagic();
        //readLinez();
        //s2();
        //read();
        //fetchInvertedIndex();
        //getBucketsRanges(1.1,1.4);
        fetchTermMap();
        buildFastQueryTrace();
        //metodo();
        System.exit(1);

        String data = "/home/aalto/dio/docInfo";
        InvertedIndex ps;


        if (Files.exists(Paths.get(tPath+ser))) {
            System.out.println("Deserializing Predictive Inverted...");
            ps = new InvertedIndex((HashMap<Integer, Integer>) deserialize(fPath+ser), (int[]) deserialize(sPath+ser));
            System.out.println("Predictive Index Deserialized");
        }else {
            ps = new InvertedIndex();
            ps.readClueWeb(data,0);
        }
        //ps.threads();
        ps.readClueWeb(data,1);
    }

    /* TEST CLASS
    Test class to cheeck if is possible to serialize and deserialize an Hashmap of Hashmaps*/

    private static void provaSer() throws IOException, ClassNotFoundException {
        HashMap<Long, HashMap<Integer, Integer>> mappa = new HashMap<>();
        HashMap<Integer, Integer> mappetta1 = new HashMap<>();
        mappetta1.put(10, 10);
        mappetta1.put(20, 23);
        HashMap<Integer, Integer> mappetta2 = new HashMap<>();
        mappetta2.put(-114, 13);
        mappetta2.put(-87, -423);
        mappa.put((long) 1, mappetta1);
        mappa.put((long) 2, mappetta2);
        ObjectOutputStream oStream = new ObjectOutputStream(new FileOutputStream("out.bin"));
        oStream.writeObject(mappa);
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = (HashMap<Long, HashMap<Integer, Integer>>) (new ObjectInputStream(new FileInputStream("out.bin"))).readObject(); //**
        //System.out.println(fastQueryTrace.get((long) 1).get(10));
        System.out.println(fastQueryTrace.get((long) 2).get(-87));
        System.exit(1);
    }

    private static void fetchTermMap() throws IOException {
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
        System.out.println("Map Completed.");
        //DataInputStream dStream = new DataInputStream( new BufferedInputStream( new FileInputStream("/home/aalto/dio/compressedIndex")));

        /*ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"termMap.bin")));
        for(String key : termMap.keySet()){
            out.writeObject(key);
            out.writeInt(termMap.get(key));
        }
        out.close();*/
        //return termMap;
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

    private static int[][] getTopKMatrix() throws IOException {
        System.out.println("Building TopK matrix...");
        BufferedReader br = new BufferedReader( new FileReader("/home/aalto/dio/query2/complexRankerResultsTraining"));
        int [][] topMatrix = new int[173800][];
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
        getSubsets(superSet, k, 0, new HashSet<>(), res);
        long [] combo = new long[res.size()];
        int [] pair;
        int p = 0;
        //System.out.println(res);
        for(Set set : res){
            pair = Ints.toArray(set);
            java.util.Arrays.parallelSort(pair);
            combo[p] = getPair(pair[0],pair[1]);
        }
        return combo;
    }


    private static long[] getQueryBigrams(String [] queryTerms){
        //this method return all the combination of the docID in the document

        LinkedList<Integer> queryInt = new LinkedList<>();
        int termID;

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i <  queryTerms.length; i++) {
            try{
                termID = termMap.get(queryTerms[i]);
                queryInt.add(termID);
            }catch (NullPointerException e){
                e.getStackTrace();
            }
           //System.out.println(termMap.get(queryTerms[i]));
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


    private static void addTopK(HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace, long bigram, int[] topK){
        HashMap<Integer, Integer> auxMap = fastQueryTrace.get(bigram);
        if(auxMap != null){
            for (int doc: topK){
                if(auxMap.putIfAbsent(doc,1) != null){
                    auxMap.merge(doc,1,Integer::sum);
                    //System.out.println(auxMap.get(doc));
                }
            }
        }else{
            auxMap = new HashMap<>();
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

    public static HashMap<Long, HashMap<Integer, Integer>> buildFastQueryTrace() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/query/Q/million09_training"));
        int [][] topKmatrix = getTopKMatrix();
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = new HashMap<>();
        HashMap<Integer, Integer> auxMap;
        String line;
        String [] splittedLine;
        long [] queryBigrams;
        int [] topK;
        while((line = br.readLine()) != null) {
            splittedLine = line.split(":");
            queryBigrams = getQueryBigrams(splittedLine[1].split(" "));     //**
            topK = topKmatrix[Integer.valueOf(splittedLine[0])];          //**
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
        ObjectOutputStream oStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"fastQueryTrace.bin")));
        oStream.writeObject(fastQueryTrace);
        oStream.close();
        return fastQueryTrace;
    }


    /*This functions returns buckets ranges given a length. The ranges-max is hardcoded.*/

    private static int[] computeLenRanges(double lenRule){
        lenRule = 1.1;
        LinkedList<Integer> lenBuckets = new LinkedList<>();
        for (int i = 4; i < 50220423; i += i*lenRule) {
            lenBuckets.addLast(i);
        }
        return Ints.toArray(lenBuckets);
    }

    /*This functions returns buckets ranges given a rank. The ranges-max is hardcoded*/

    private static int[] computeRankRanges(double rankRule){
        rankRule = 1.4;
        LinkedList<Integer> rankBuckets = new LinkedList<>();
        for (int i = 11; i < 100220423 ; i += i*rankRule) {
            rankBuckets.addLast(i);
        }
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
        int [] lenRanges = computeLenRanges(1.1);
        int [] rankRanges = computeRankRanges(1.4);
        int [][][] qualityModel = new int[lenRanges.length][rankRanges.length][2];
        BufferedReader br = new BufferedReader(new FileReader("readIndexInfo"));
        DataInputStream inStream = new DataInputStream( new BufferedInputStream(new FileInputStream("compressedSortedInvertedIndex")));
        ObjectInputStream obInStream = new ObjectInputStream(new FileInputStream(metadata+"fastQueryTrace.bin"));
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = (HashMap<Long, HashMap<Integer, Integer>>) obInStream.readObject(); //conversion seems to work
        String [] line;
        byte [] byteStream;
        int [] postingList;
        long pair;
        int lenBucket;
        int rankBucket = 0;
        HashMap<Integer, Integer> aggregatedTopK;
        int increment;
        while((line = br.readLine().split(","))[0] != null){ //**
            pair = Integer.valueOf(line[0]);
            byteStream = new byte[Integer.valueOf(line[1])];
            inStream.read(byteStream);
            postingList = decodeInterpolate(byteStream);
            lenBucket = getLenBucket(Integer.valueOf(line[2]), lenRanges);

            aggregatedTopK = fastQueryTrace.get(pair);
            for (int i = 0; i < postingList.length ; i += 2) {
                increment = aggregatedTopK.get(postingList[i]);
                rankBucket = getRankBucket(rankBucket, postingList[i+1], rankRanges);

                //bucket hit by this posting
                qualityModel[lenBucket][rankBucket][0] += increment ;
                for (int j = 0; j < rankBucket+1 ; j++) {
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
            if(counter % 200000000 == 0) System.out.println("Up to record #" + (counter));
            return aux;
        }catch(EOFException exception){
            System.out.println("Fetching Time: " + (System.currentTimeMillis() - now) + "ms");
            aux[0] = -1;
            return aux;
        }
    }


    /* Once the inverted index is sorted we compress it using variable byte encoding.
     * In this way we achive a compression of x3 and achiving a computation speed up of x10
     *
     * OPEN ISSUES:
     * - THIS IS NOT GOOD*/

    public static int [][] compressInvertedIndex() throws IOException {
        int p = 0;
        int maxLength = 0;
        PrintWriter totalIndexStats = new PrintWriter(path+"/totalStatsLength.csv", "UTF-8");
        int [] lengthStats = new int[5000000];
        long now = System.currentTimeMillis();
        long percentage;
        DataInputStream dataStream = new DataInputStream( new BufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.dat")));
        DataOutputStream outStream = new DataOutputStream( new BufferedOutputStream((new FileOutputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/compressedSortedInvertedIndex.dat"))));
        int [][] invertedIndex = new int[528184109][];
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        getEntry(dataStream);
        int [] nowPair = new int[]{-1, -1};
        int [] aux;
        byte [] byteSteam;
        while(true){
            aux = getEntry(dataStream);
            if(aux[0] != nowPair[0] | aux[1] != nowPair[1]){
                if(p % 10000000 == 0){
                    percentage = (long) (p*100.0)/528184109;
                    System.out.println("Work in progress: " + percentage+ "% completed.");
                    //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
                }
                if(auxPostingList.size()>maxLength) maxLength = auxPostingList.size();
                //lengthStats[auxPostingList.size()]++;
                //invertedIndex[p] = Ints.toArray(auxPostingList);
                byteSteam = encodeInterpolate(auxPostingList);
                //need to add the docInfo file for the compressed inverted index
                for (byte b: byteSteam) {
                    outStream.writeByte(b);
                }
                p++;
                auxPostingList.clear();
                nowPair[0]= aux[0];
                nowPair[1]= aux[1];
                auxPostingList.addLast(aux[3]);

            }else if(aux[0]==-1){
                break;
            }else{
                auxPostingList.addLast(aux[2]);
                auxPostingList.addLast(aux[3]);
            }
        }
        for(int k =0; k<lengthStats.length ; k++) totalIndexStats.print((lengthStats[k]*k)+",");
        System.out.println(maxLength);
        System.exit(1);
        return invertedIndex;
    }


}






