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



    public static void superMagic(){
        LinkedList<int[]> prova = new LinkedList<>();
        prova.add(new int[]{1,2});
        for (int i : prova.getFirst()) {
            System.out.print(i);
        }
        double[][] kickerNumbers = new double[50000000][2];
        for (int i = 0; i < kickerNumbers.length; i++) {
            for (int j = 0; j < kickerNumbers[0].length ; j++) {
                kickerNumbers[i][j] = Math.random();
            }
        }
        long now = System.currentTimeMillis();
        java.util.Arrays.sort(kickerNumbers, new Comparator<double[]>() {
            @Override
            public int compare(double[] int1, double[] int2) {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0] == int2[0]) {
                    return Double.compare(int1[1], int2[1]) * -1;
                } else return Double.compare(int1[0], int2[0]);
            }
        });
        System.out.print(System.currentTimeMillis() - now);
        System.exit(1);
    }

    public static void readLinez() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/docInfo"));
        long k=0;
        String line = br.readLine();
        while(line != null){
          line = br.readLine();
            k++;

        }
        System.out.print(k);
        System.exit(1);
    }

    public static int[] getEntry(DataInputStream dataStream) throws IOException {
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

    public static int [][] compressInvertedIndex() throws IOException {
        /* it is not necessary load the whole inverted index in main memory because that was exactly what we are trying to avoid.
        * */


        int IIPointer = 0;
        int maxLength = 0;
        PrintWriter totalIndexStats = new PrintWriter(path+"/totalStatsLength.csv", "UTF-8");
        int [] lengthStats = new int[5000000];
        long now = System.currentTimeMillis();
        long percentage;
        DataInputStream dataStream = new DataInputStream( new BufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.dat")));
        int [][] invertedIndex = new int[528184109][];
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        getEntry(dataStream);
        int [] nowPair = new int[]{-1, -1};
        int [] aux;
        while(true){
            aux = getEntry(dataStream);
            if(aux[0] != nowPair[0] | aux[1] != nowPair[1]){
               if(IIPointer++ % 10000000 == 0){
                   percentage = (long) (IIPointer*100.0)/528184109;
                   System.out.println("Work in progress: " + percentage+ "% completed.");
                   //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
               }
               if(auxPostingList.size()>maxLength) maxLength = auxPostingList.size();
               //lengthStats[auxPostingList.size()]++;
               //invertedIndex[IIPointer] = Ints.toArray(auxPostingList);
               IIPointer++;
               auxPostingList.clear();
               nowPair[0]= aux[0];
               nowPair[1]= aux[1];
               auxPostingList.addLast(aux[0]);
               auxPostingList.addLast(aux[1]);
               auxPostingList.addLast(aux[3]);

           }else if(aux[0]==-1){
                break;
           }else{
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

    public static long[] getCombinations(List<Integer> superSet, int k) {
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


    public static long[] getQueryBigrams(String [] queryTerms){
        //this method return all the combination of the docID in the document

        LinkedList<Integer> queryInt = new LinkedList<>();

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i <  queryTerms.length; i++) {
           queryInt.add(i); //termsMap.get(String);
        }
        //We take every combination of our query terms. We save them in a long array using bit-shifting
        return getCombinations(queryInt,2);
    }

    public static int[] getQueryTopkDocIDs(String [] topk){
        //declare a new object every time
        int [] topkInt = new int[topk.length];

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i <  topk.length; i++) {
            topkInt[i] = 1; //docIDmap.get(String);
        }
        return topkInt;
    }


    public void addTopK(HashMap<Integer, Integer> auxMap, int[] topK){
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

    public HashMap<Long, HashMap<Integer, Integer>> getfastQueryTrace() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("f"));
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = new HashMap<>();
        HashMap<Integer, Integer> auxMap;
        String [] line;
        long [] queryBigrams;
        int [] topK;
        while((line = br.readLine().split("-")) != null) {
            queryBigrams = getQueryBigrams(line[0].split(","));
            topK = getQueryTopkDocIDs(line[1].split(","));
            for (long bigram : queryBigrams) {
                auxMap = fastQueryTrace.get(bigram);
                addTopK(auxMap, topK);
            }
        }
        //serialize fastQueryTrace to disk
        return fastQueryTrace;
    }

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

    public static int getLenBucket(int len, int[] lenBuckets){
        int i;
        for (i = 0; lenBuckets[i] < len; i++);
        return i;
    }





}






