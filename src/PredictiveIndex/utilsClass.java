package PredictiveIndex;

import it.unimi.dsi.fastutil.floats.Float2BooleanArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPInputStream;
import me.lemire.integercompression.differential.*;

import org.bouncycastle.asn1.dvcs.Data;
import org.mapdb.*;
import java.lang.System;
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
class utilsClass {

    static int[] readClueWebDocument(String [] line, DataInputStream stream, int [] document) throws IOException {
        byte [] rawDoc = new byte[Integer.parseInt(line[3])];
        for (int i = 0; i < rawDoc.length; i++) {
            rawDoc[i] = stream.readByte();
        }
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
                k++;
                n = 0;
            }
        }
        return document;
    }



    static void storeHashMap(Int2IntMap map, DataOutputStream DOS, int len) throws IOException {
        DOS.writeInt(len*2);
        for(int key : map.keySet()){
            if(map.get(key)>1){
                DOS.writeInt(key);
                DOS.writeInt(map.get(key));
            }
        }
    }

    static Int2IntMap fetchHashMap(Int2IntMap map, DataInputStream DIS) throws IOException {
        for (int i = 0; i <DIS.readInt()/2; i++) {
            map.put(DIS.readInt(),DIS.readInt());
        }
        return map;
    }




    protected static int getBM25(long [] globalStats, int docLen, int f, int n) {
        /*global statistics for BM25*/
        long N = globalStats[0];
        double avg = globalStats[1] / N;
        double k = 1.6;
        double b = 0.75;
        double IDF = java.lang.Math.log((N - n + 0.5 )/( n + 0.5));
        double BM25 = (IDF * f * k + 1) / (f + k * (1 - b + (b* docLen / avg)));
        //System.out.println(BM25);
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

    static long getPair(int a , int b){
        return (long)a << 32 | b & 0xFFFFFFFFL;
    }

    static int[] getTerms(long c){
        int aBack = (int)(c >> 32);
        int bBack = (int)c;
        return new int[]{aBack,bBack};
    }

    static ObjectInputStream getOIStream(String filename, boolean buffered) throws IOException {
        if(buffered) return new ObjectInputStream(new FileInputStream(filename+".bin"));
        else return new ObjectInputStream( new BufferedInputStream(new FileInputStream(filename+".bin")));

    }

    static ObjectOutputStream getOOStream(String filename, boolean buffered) throws IOException {
        if(buffered) return new ObjectOutputStream(new FileOutputStream(filename+".bin"));
        else return new ObjectOutputStream( new BufferedOutputStream(new FileOutputStream(filename+".bin")));
    }

    static void serialize(Object e, String filename) {
        try {
            ObjectOutputStream OOStream = getOOStream(filename, true);
            OOStream.writeObject(e);
            OOStream.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    static Object deserialize(String file) {
        out.print("Fetching: " + file + "...");
        Object e = null;
        try {
            ObjectInputStream OIStream = getOIStream(file, true);
            e = OIStream.readObject();
            OIStream.close();
            out.println("\tfetched!");
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }
    /*528184109
    * #documnets: 50220423
    * rate: 10000000*/
    static boolean checkProgress(long now, int tot, int rate, double start, int testLimit){
        if(now % rate == 0){
            long percentage = (long) (now*100.0)/testLimit;
            out.println("Work in progress: " + percentage+ "%\tProcessing Time: " + (now / (System.currentTimeMillis() - start)) * 1000 + "doc/s. \tProcessed: " +now);
            out.print("Expected Remaining Time: "+ (((System.currentTimeMillis() - start)/percentage)*(100-percentage)/60000) + " minutes");
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

    public static void tryMap(){
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
    }

    public static void memoryStatistics() {

        int mb = 1024*1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        //Print used memory
        out.println("\tUsed Memory: "
                + (runtime.totalMemory() - runtime.freeMemory()) / mb + " mb");
    }

    static void splitCollection(String info) throws IOException {
        long stemmedQuarter = 13760033936L;
        long nonStemmedQuarter = 14257479996L;
        System.out.println("Splitting ClueWeb09...");
        int doc = 0;
        int split = 0;
        long bytes = 0;
        int flag = 0;

        String docInfo = "/home/aalto/IdeaProjects/PredictiveIndex/data/source/docInfo";
        String compressedIndex = "/home/aalto/IdeaProjects/PredictiveIndex/data/source/noStemmerIndex";
        String folder = "/home/aalto/IdeaProjects/PredictiveIndex/data/";

        DataInputStream DIS = new DataInputStream(new BufferedInputStream( new FileInputStream(compressedIndex)));
        BufferedReader br = new BufferedReader(new FileReader(docInfo));

        DataOutputStream DOS = new DataOutputStream(new BufferedOutputStream( new FileOutputStream(folder + split + "/clueweb.bin")));;
        BufferedWriter bw = new BufferedWriter(new FileWriter(folder + split + "/"+ "docInfo.csv"));

        String line = br.readLine();
        String [] record;
        while(line != null){
            record = line.split(" ");
            if (flag==1) {
                out.println(doc);
                DOS.close();
                bw.close();
                System.exit(1);
                split++;
                flag=0;
                bw = new BufferedWriter(new FileWriter(folder + split + "/"+ "docInfo.csv"));
                DOS = new DataOutputStream(new BufferedOutputStream( new FileOutputStream(folder + split + "/clueweb.bin")));
            }
            for (int i = 0; i < Integer.parseInt(record[3]); i++) {
                DOS.writeByte(DIS.readByte());
                bytes++;
                if(bytes % nonStemmedQuarter==0 & doc!=0) flag = 1;
            }
            bw.write(line);
            bw.newLine();
            line = br.readLine();
            doc++;
        }
        DOS.close();
        DIS.close();
        System.out.println("ClueWeb09 Splitted! " + doc);
    }

   static void greedySelection(){
        Int2IntOpenHashMap model = new Int2IntOpenHashMap();

   }

   static void mergeDumps() throws IOException, ClassNotFoundException {
       Long2IntOpenHashMap mergedMap = new Long2IntOpenHashMap();
       Long2IntOpenHashMap aux;
       int k = 0;
       for(File f: new File(globalFold+"/dumped/").listFiles()) {
           aux = (Long2IntOpenHashMap) new ObjectInputStream( new BufferedInputStream(new FileInputStream(f.getAbsoluteFile()))).readObject();
           for (long key: aux.keySet()) {
               if(mergedMap.putIfAbsent(key, aux.get(key)) != null) mergedMap.merge(key, aux.get(key), Integer::sum);
           }
       }
       serialize(mergedMap, globalFold+"/dumped/finalDump");
   }

   static float[] scalarPerArray(float scalar, float[][] array){
       float [] result = new float[array.length];
       for (int i = 0; i < result.length; i++) {
           result[i] = (array[i][0]/array[i][1])*scalar;
       }
       return result;
   }

}
