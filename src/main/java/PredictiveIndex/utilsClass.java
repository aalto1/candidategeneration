package PredictiveIndex;

import it.unimi.dsi.fastutil.floats.Float2BooleanArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import me.lemire.integercompression.differential.*;

import java.lang.System;
import it.unimi.dsi.fastutil.longs.*;

import java.util.*;
import it.unimi.dsi.fastutil.ints.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipFile.*;
import java.util.zip.ZipInputStream;

import static PredictiveIndex.PredictiveIndex.globalFold;
import static java.lang.System.exit;
import static java.lang.System.out;

/**
 * Created by aalto on 9/11/16.
 */
class utilsClass extends WWW {

    static int[] readClueWebDocument(String [] line, DataInputStream stream, int [] document) throws IOException {
        byte [] rawDoc = new byte[Integer.parseInt(line[3])];
        int [] aux;
        for (int i = 0; i < rawDoc.length; i++) {
            rawDoc[i] = stream.readByte();
        }
        //aux = Arrays.copyOf(decodeRawDoc(rawDoc, Integer.parseInt(line[4]), document),Integer.parseInt(line[4]));
        //System.out.println(aux[0]);
        //if(Arrays.asList(aux).contains(0)) System.out.println(Arrays.asList(aux));
        //return aux;
        return decodeRawDoc(rawDoc, Integer.parseInt(line[4]), document);
    }

    /*  This function decompress the forward index using opposite variable byte decoding
    0xff = 255 = 11111111 = u-byte */

    static int[] decodeRawDoc(byte[] rawDoc, int docLen, int[] document) {
        int k = 0;
        int n = 0;
        int a;
        for (byte b : rawDoc) {
            if ((b & 0xff) >= 128) {
                a = (b << 25);
                n = n * 128 + (a >>> 25);
            } else {
                int num = n * 128 + b;
                document[k] = num;
                //if(document[k]<1)System.out.println(","+document[k]+","+ k);
                k++;
                n = 0;
            }
        }
        //if(docLen==k) System.out.println("expected: " + docLen + " Found: " + k + " Array Length: " + document.length);
        return document;
    }




    static void storeHashMap(Int2IntMap map, DataOutputStream DOS, int len) throws IOException {
        DOS.writeInt(len);
        //int k = 0;
        for(int key : map.keySet()){
            if(map.get(key)>1){
                DOS.writeInt(key);
                DOS.writeInt(map.get(key));
                //k++;
            }
        }
        //if(k!=len)System.out.println(k +"-" + len);
    }

    static Int2IntMap fetchHashMap(Int2IntMap map, DataInputStream DIS, int tn) throws IOException {
        int a = DIS.readInt();
        for (int i = 0; i < a; i++) {
            map.put(DIS.readInt(),DIS.readInt());
        }
        System.out.println(tn+ "-" + map.size() + "-" + a);
        return map;
    }




    protected static int getBM25(long [] globalStats, int docLen, int termFreq , int localMaxFreq, int n) {
        /*global statistics for BM25*/
        long N = globalStats[0];
        double avg = globalStats[1] / N;
        double k = 1.6;
        double b = 0.75;
        double normalizedFreq = 0.5 + (0.5*termFreq/localMaxFreq);
        double IDF = java.lang.Math.log((N - n + 0.5 )/( n + 0.5));
        double BM25 = (IDF * normalizedFreq * (k + 1)) / (normalizedFreq + k * (1 - b + (b* docLen / avg)));
        //System.out.println(localMaxFreq);
        //if(BM25<0) System.out.println(N + " " + n);
        //System.out.println(BM25*Math.pow(10, 7));
        return (int) (BM25*Math.pow(10, 7));
    }

    protected static int getBM25basic(long [] globalStats, int docLen, int termFreq , int localMaxFreq, int n) {
        /*global statistics for BM25*/
        long N = globalStats[0];
        double avg = globalStats[1] / N;
        double k = 1.6;
        double b = 0.75;
        double normalizedFreq = termFreq/localMaxFreq;
        double IDF = java.lang.Math.log((N - n + 0.5 )/( n + 0.5));
        double BM25 = (IDF * normalizedFreq * (k + 1)) / (normalizedFreq + k * (1 - b + (b* docLen / avg)));
        //if(BM25<0) System.out.println(N + " " + n);
        //System.out.println(BM25*Math.pow(10, 7));
        return (int) (BM25*Math.pow(10, 7));
    }




    static int [] hashMapToArray(Int2IntMap map, int [] array){
        /*This function convert an HashMap to an 1d array with the dimension doubled respect to the key-value pairs
        * with a value bigger than one*/

        int value;
        int k = 0;
        for(int key : map.keySet()){
            value = map.get(key);
            if((value)>1){                          //becasue we want to reduce the overhead (20% space saved per dump)
                array[k] = key;
                array[k+1] = value;
                k+=2;
            }
        }
        return Arrays.copyOf(array, k);
    }

    static void arrayToHashMap(int [] array, Int2IntMap map){
        /*This function converst an 1d array to an hashmap. We use this function to get the term freq. for a specific
        * term within a doc*/

        for(int k = 0; k<array.length-1; k+=2){
            map.put(array[k], array[k+1]);
        }
    }




    /*528184109
    * #documnets: 50220423
    * rate: 10000000*/
    static boolean checkProgress(long now, int tot, int rate, double start, int testLimit){
        if(now % rate == 0){
            long percentage = (long) (now*100.0)/testLimit;
            double value = (((System.currentTimeMillis() - start)/percentage)*(100-percentage)/60000);
            out.println("Work in progress: " + percentage+ "%\tProcessing Time: " + (now / (System.currentTimeMillis() - start)) * 1000 + "doc/s. \tProcessed: " +now);
            out.print("Expected Remaining Time: "+ ((int) value) + " minutes " + (int) ((value - ((int) value))*60) + " seconds");
            memoryStatistics();
        }
        if(now>testLimit) return false;
        else return true;
    }


    //79274,78218,77516,77060,76525,76073,75829,75549,75225,74945,738756,1434403,2757305,5236694,9776414,17872156,31812253,38568859,13752205,268435456,0,0
    static void aggregateHITS(String file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        GZIPInputStream gis = new GZIPInputStream(new BufferedInputStream(fis));
        InputStreamReader isr = new InputStreamReader(gis);
        //BufferedReader br = new BufferedReader(isr);
        //System.out.println(br.readLine());
        CSVParser reader = (new CSVParser(isr, CSVFormat.EXCEL));
        Iterator<CSVRecord> it = reader.iterator();
        CSVRecord line = it.next();
        float [][] score = new float[50025796][2];
        out.println("Computing Scores...\t");
        for(int k=0; it.hasNext(); line = it.next(), k++){
            score[k][0]=k;
            for (int i = 1; i < line.size() ; i++) {
                score[k][1] += (Integer.parseInt(line.get(i))*1.0)/(Math.log(i)/Math.log(2)+1);
                //score[k][1] += Integer.parseInt(line.get(i));
            }
            if(k % Math.pow(10,6)==0){
                if(k % Math.pow(10,7)==0 && k!=0){
                    out.println(" " + k);
                }else out.print(" " + k);
            }

        }
        out.println("\tdone.");
        out.print("Sorting...");
        Arrays.parallelSort(score, new Comparator<float[]>() {
            @Override
            public int compare(float[] o1, float[] o2) {
                return Float.compare(o2[1],o1[1]);
            }
        });
        out.println("\tdone.");
        out.print("Writing on disk...");
        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream("globalscore.csv")));
        for (float [] a: score) {
            ps.println(((int)a[0])+","+a[1]);
        }
        out.print("\tdone.");

    }

    public static double[] getHitScore() throws IOException {
        int numbeOfDocs = 50025479;
        BufferedReader br = getBuffReader(hitScores);
        String line = br.readLine();
        double [] hitScores = new double[numbeOfDocs];
        double [] aux;
        while(line!=null){
            aux = Arrays.stream(line.split(",")).mapToDouble(Double::parseDouble).toArray();
            hitScores[(int) aux[0]] = aux[1];
            br.readLine();
        }
        return hitScores;
    }

    public static void getHitScore2() throws IOException {
        System.out.println("Getting HITS score");
        int numbeOfDocs = 50222043; //50025479; attention!
        BufferedReader br = getBuffReader(hitScoresCSV);
        String line = br.readLine();
        int [] HITS = new int[numbeOfDocs];
        for(int i = 0; line!= null; i++){
            HITS[Integer.parseInt(line.split(",")[0])] = i;
            line = br.readLine();
            if(i%1000000==0) System.out.print(i+",");
        }
        serialize(HITS, hitScores);
    }

    /*public static void tryMap(){
        Int2IntMap auxMap = new Int2IntOpenHashMap();
        Pump a = Pump.INSTANCE;
        DB db = DBMaker.fileDB("termFreq.db").fileMmapEnable().cleanerHackEnable().fileMmapPreclearDisable().make();
        ConcurrentMap<Integer,Integer> map = db.hashMap("map", Serializer.INTEGER_DELTA, Serializer.INTEGER_DELTA).createOrOpen();
        for(int i =0; i<Integer.MAX_VALUE;i++){
            if(i%5000000==0){
                //auxMap.put(i,i);
                out.println(map.size());
                map.putAll(auxMap);
                //map.putAll(auxMap);
                //auxMap.clear();
                //System.out.println(auxMap.size());
            }
            //map.put(i, i);
            auxMap.put(i,i);
        }
        db.close();
    }*/

    /*fefe*/

    public static void memoryStatistics() {

        int mb = 1024*1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        //Print used memory
        out.println("\tUsed Memory: "
                + (runtime.totalMemory() - runtime.freeMemory()) / mb + " mb");
    }

    static void splitCollection() throws IOException {
        //long stemmedQuarter = 13760033936L;
        long nonStemmedQuarter = 14257479996L;
        System.out.println("Splitting ClueWeb09...");
        int doc = 0;
        int split = 0;
        long bytes = 0;
        int flag = 0;


        DataInputStream DIS = new DataInputStream(new BufferedInputStream( new FileInputStream(nonStemClue)));
        BufferedReader br = new BufferedReader(new FileReader(finalDocInfo));

        DataOutputStream DOS = getDOStream(folder[split] + "clueweb");
        BufferedWriter bw = getBuffWriter(folder[split] + "docInfo.csv");

        String line = br.readLine();
        String [] record;
        while(line != null){
            record = line.split(" ");
            if (flag==1) {

                out.println(doc);
                DOS.close();
                bw.close();
                //System.exit(1);
                split++;
                flag=0;
                bw = getBuffWriter(folder[split] + "docInfo.csv");
                DOS = getDOStream(folder[split] + "clueweb");
            }
            for (int i = 0; i < Integer.parseInt(record[3]); i++) {
                DOS.writeByte(DIS.readByte());
                bytes++;
                if(bytes % nonStemmedQuarter==0 & doc!=0 & split!=3) flag = 1;
            }
            bw.write(line + "\n");
            line = br.readLine();
            doc++;
        }
        bw.close();                 //if you don't close the buffer you will lost stuff.
        DOS.close();
        DIS.close();
        System.out.println("ClueWeb09 Splitted! " + doc);
    }


   static void mergePairDumps(Long2IntOpenHashMap [] maps) throws IOException, ClassNotFoundException { /***NO-Need***/
       Long2IntOpenHashMap mergedMap = new Long2IntOpenHashMap();
       int k = 0;
       for(Long2IntOpenHashMap map: maps) {
           for (long key: map.keySet()) {
               if(mergedMap.putIfAbsent(key, map.get(key)) != null) mergedMap.merge(key, map.get(key), Integer::sum);
           }
       }
       serialize(mergedMap, dBigramDumpMap);
   }

    static void getLocFreqMap(int [] locFreqArr, IntOpenHashSet uniTerms) throws IOException, ClassNotFoundException { /***NO-Need***/
        Int2IntOpenHashMap mergedMap = new Int2IntOpenHashMap();
        int k = 0;
        for (int i = 0; i < locFreqArr.length ; i++) {
            if(uniTerms.contains(i)) mergedMap.put(i, locFreqArr[i]);
            k++;
        }
        System.out.println(k);
        serialize(mergedMap, localFreqMap);
    }

   static float[] scalarPerArray(float scalar, float[] array){
       float [] result = new float[array.length];
       for (int i = 0; i < result.length; i++) {
           result[i] = array[i]*scalar;
       }
       return result;
   }

   static void sortComplexRanking() throws IOException {
       BufferedReader br = getBuffReader(complexRank);
       BufferedWriter bw = getBuffWriter(complexRankN);
       double [][] ranks = new double[85796224][4];
       int k = 0;
       String line;
       while((line = br.readLine()) != null){
               ranks[k++] = string2DoubleArray(line, " ");
       }
       br.close();

       Arrays.parallelSort(ranks, rankerComparator);
       double tmp = -1;
       int counter = 0;
       for (double [] aux : ranks) {
           if(tmp!=aux[0]){
               counter = 0;
               tmp = aux[0];
           }
           if(counter < 10){
               if(counter == 0) bw.write(((int) aux[0])+",");
               bw.write((((int) aux[1])+","));
               counter++;
               if(counter == 10) bw.newLine();
           }
       }
       bw.close();
   }

    public static List<Integer> string2IntList(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());

    }

    public static int[] string2IntArray(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToInt(Integer::parseInt).toArray();

    }

    public static List<Long> string2LongList(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToLong(Long::parseLong).boxed().collect(Collectors.toList());

    }

    public static long[] string2LongArray(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToLong(Long::parseLong).toArray();

    }

    public static List<Double> string2DoubleList(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToDouble(Double::parseDouble).boxed().collect(Collectors.toList());

    }

    public static double[] string2DoubleArray(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToDouble(Double::parseDouble).toArray();

    }

    public static void buildDocIDMap() throws IOException {
        BufferedReader br1 = getBuffReader(didNameMap);
        BufferedReader br2 = getBuffReader(oldDocInfo);
        BufferedWriter bw = getBuffWriter(didMap);
        bw.write("old, new\n");
        String line;
        String [] fields;
        Object2IntOpenHashMap<String> map = new Object2IntOpenHashMap<>();
        while((line= br1.readLine())!=null){
            fields = line.split(" ");
            map.put(fields[1], Integer.valueOf(fields[0]).intValue());
        }
        while((line= br2.readLine())!=null){
            fields = line.split(" ");
            bw.write(fields[1]+","+map.getInt(fields[0]));
            bw.newLine();
        }
        br1.close();
        br2.close();
        bw.close();
    }

    public static Int2IntOpenHashMap getDIDMap() throws IOException {
        Int2IntOpenHashMap map;
        if(!checkExistence(serDIDMap)) {
            BufferedReader br = getBuffReader(didMap);
            br.readLine();
            String line;
            int[] entry;
            map = new Int2IntOpenHashMap();
            while ((line = br.readLine()) != null) {
                entry = string2IntArray(line, ",");
                map.put(entry[0], entry[1]);
            }
            serialize(map, serDIDMap);
        }else
            map = (Int2IntOpenHashMap) deserialize(serDIDMap);
        return map;
    }


}
