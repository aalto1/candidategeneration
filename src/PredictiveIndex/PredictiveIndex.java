package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import sun.nio.cs.Surrogate;

import javax.sound.sampled.Line;

import static PredictiveIndex.InvertedIndex.*;
import static PredictiveIndex.VariableByteCode.decodeInterpolate;
import static PredictiveIndex.VariableByteCode.encodeInterpolate;


/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {
    static int counter = 1;
    static int counter2 = 1;

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*We get the global statistics of the collection (fetch from memory if present, compute them if the opposite)
        * and than build the pair-distance from memory.*/
        //superMagic();
        //readLinez();
        //s2();
        //read();
        //fetchInvertedIndex();
        getBucketsRanges(1.1,1.4);

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
        getSubsets(superSet, k, 0, new HashSet<Integer>(), res);
        long [] combo = new long[res.size()];
        int [] pair;
        int p = 0;
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

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i <  queryTerms.length; i++) {
           queryInt.add(i); //termsMap.get(String);
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


    private void addTopK(HashMap<Integer, Integer> auxMap, int[] topK){
        if(auxMap != null){
            for (int doc: topK){
                if(auxMap.putIfAbsent(doc,1) != null){
                    auxMap.merge(doc,1,Integer::sum);
                }
            }
        }else{
            auxMap = new HashMap<>();
            for (int doc: topK) auxMap.put(doc,1);
        }
        addTopK(auxMap, topK);
    }

    /*We reformat the Query trace in a way that we can perform fast lookup while scanning the
    * compressed inverted index.
    *
    * OPEN ISSUES:
    * - Save the fastQueryTrace and check if the conversion is possible while fetching it
    * - Check the format of the query trace in a way that you can see if the split is ok*/

    public HashMap<Long, HashMap<Integer, Integer>> getfastQueryTrace() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("f"));
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = new HashMap<>();
        HashMap<Integer, Integer> auxMap;
        String [] line;
        long [] queryBigrams;
        int [] topK;
        while((line = br.readLine().split("-"))[0] != null) {
            queryBigrams = getQueryBigrams(line[0].split(","));     //**
            topK = getQueryTopkDocIDs(line[1].split(","));          //**
            for (long bigram : queryBigrams) {
                auxMap = fastQueryTrace.get(bigram);
                addTopK(auxMap, topK);
            }
        }
        //serialize fastQueryTrace to disk
        return fastQueryTrace;
    }


    /*This functions returns 2 arrays which are the buckets ranges given a length and rank rule.
    * Default values are:
    * 1) L-rule = 1.1
    * 2) R-rule = 1.4
    *
    * The ranges-max is hardcoded.
    *
    * OPEN ISSUES:
    * - Load the fastQueryTrace in an appropriate way */

    public static int[][] getBucketsRanges(double lenRule, double rankRule){
        lenRule = 1.1;
        rankRule = 1.4;
        LinkedList<Integer> lenBuckets = new LinkedList<>();
        LinkedList<Integer> rankBuckets = new LinkedList<>();
        for (int i = 4; i < 50220423; i += i*lenRule) {
            lenBuckets.addLast(i);
        }
        for (int i = 11; i < 100220423 ; i += i*rankRule) {
            rankBuckets.addLast(i);
        }
        return new int[][]{Ints.toArray(lenBuckets),Ints.toArray(rankBuckets)};
    }

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
    * - Load the fastQueryTrace in an appropriate way */

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
    * OPEN ISSUES:
    * - Load the fastQueryTrace in an appropriate way
    * - Check if the while condition is always true*/

    public static int[][][] getQualityModel() throws IOException, ClassNotFoundException {
        int [][] bucketsRanges = getBucketsRanges(1.1,1.4);         //bucketsRanges[0] = Length Ranges - bucketsRanges[1] = Rank Ranges
        int [][][] qualityModel = new int[bucketsRanges[0].length][bucketsRanges[1].length][2];
        BufferedReader br = new BufferedReader(new FileReader("readIndexInfo"));
        DataInputStream inStream = new DataInputStream( new BufferedInputStream(new FileInputStream("compressedSortedInvertedIndex")));
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = (HashMap<Long, HashMap<Integer, Integer>>) (new ObjectInputStream(new FileInputStream(" fee"))).readObject(); //**
        String [] line;
        byte [] byteStream;
        int [] postingList;
        long pair;
        int lenBucket;
        int rankBucket = 0;
        HashMap<Integer, Integer> aggregatedTopK;
        int increment;
        while((line = br.readLine().split(","))[0] != null){ //**
            byteStream = new byte[Integer.valueOf(line[1])];
            inStream.read(byteStream);
            postingList = decodeInterpolate(byteStream);
            lenBucket = getLenBucket(Integer.valueOf(line[2]), bucketsRanges[0]);
            pair = Integer.valueOf(line[1]);
            aggregatedTopK = fastQueryTrace.get(pair);
            for (int i = 0; i < postingList.length ; i += 2) {
                increment = aggregatedTopK.get(postingList[i]);
                rankBucket = getRankBucket(rankBucket, postingList[i+1], bucketsRanges[1]);

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





}






