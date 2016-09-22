package PredictiveIndex;

import it.unimi.dsi.fastutil.floats.Float2BooleanArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPInputStream;
import org.mapdb.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipFile.*;
import java.util.zip.ZipInputStream;

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
        for (byte b : rawDoc) {
            if ((b & 0xff) >= 128) {
                n = 128 * n + (b & 0xff);
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                document[k] = num;
                //if(num>87262395) System.out.println(num);
                k++;
                n = 0;
            }
        }
        return document;
    }

    protected static int getBM25(int [] globalStats, int docLen, int f, int n) {
        /*global statistics for BM25*/
        int N = globalStats[0];
        double avg = globalStats[1] / N;
        double k = 1.6;
        double b = 0.75;
        double IDF = java.lang.Math.log((N - n + 0.5 )/( n + 0.5));
        double BM25 = (IDF * f * k + 1) / (f + k * (1 - b + (b* docLen / avg)));
        //if(BM25>100) System.out.println(BM25);
        return (int) (BM25*Math.pow(10, 7));
    }


    static int [] hashMapToArray(Int2IntMap map, int multipleOccurence){
        /*This function convert an HashMap to an 1d array with the dimension doubled respect to the key-value pairs
        * with a value bigger than one*/

        int [] array = new int[multipleOccurence*2];
        int value;
        int k = 0;
        for(int key : map.keySet()){
            value = map.get(key);
            if((value)>1){                          //becasue we want to reduce the overhead (20% space saved per dump)
                array[k*2] = key;
                array[k*2+1] = value;
                k++;
            }
        }
        return Arrays.copyOf(array, k*2+1);
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
        System.out.print("Fetching: " + file + "...");
        Object e = null;
        try {
            ObjectInputStream OIStream = getOIStream(file, true);
            e = OIStream.readObject();
            OIStream.close();
            System.out.println("\tfetched!");
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }
    /*528184109
    * #documnets: 50220423
    * rate: 10000000*/
    static boolean checkProgress(long p, int max, int rate, double start, int limit){
        if(p % rate == 0){
            int percentage = (int) (p*100.0)/max;
            System.out.println("Work in progress: " + percentage+ "%\tProcessing Time: " + (p / (System.currentTimeMillis() - start)) * 1000 + "doc/s. \tProcessed: " +p);
            System.out.print("Expected Remaining Time: "+ (((System.currentTimeMillis() - start)/percentage)*(100-percentage)/60000) + " minutes");
            memoryStatistics();
            //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
        }
        if(p>limit) return false;
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
        System.out.println("Computing Scores...\t");
        for(int k=0; it.hasNext(); line = it.next(), k++){
            score[k][0]=k;
            for (int i = 1; i < line.size() ; i++) {
                score[k][1] += (Integer.parseInt(line.get(i))*1.0)/(Math.log(i)/Math.log(2)+1);
                //score[k][1] += Integer.parseInt(line.get(i));
            }
            if(k % Math.pow(10,6)==0){
                if(k % Math.pow(10,7)==0 && k!=0){
                    System.out.println(" " + k);
                }else System.out.print(" " + k);
            }

        }
        System.out.println("\tdone.");
        System.out.print("Sorting...");
        Arrays.parallelSort(score, new Comparator<float[]>() {
            @Override
            public int compare(float[] o1, float[] o2) {
                return Float.compare(o2[1],o1[1]);
            }
        });
        System.out.println("\tdone.");
        System.out.print("Writing on disk...");
        PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream("globalscore.csv")));
        for (float [] a: score) {
            ps.println(((int)a[0])+","+a[1]);
        }
        System.out.print("\tdone.");




    }

    public static void tryMap(){
        Int2IntMap auxMap = new Int2IntOpenHashMap();
        Pump a = Pump.INSTANCE;
        DB db = DBMaker.fileDB("termFreq.db").fileMmapEnable().cleanerHackEnable().fileMmapPreclearDisable().make();
        ConcurrentMap<Integer,Integer> map = db.hashMap("map", Serializer.INTEGER_DELTA, Serializer.INTEGER_DELTA).createOrOpen();
        for(int i =0; i<Integer.MAX_VALUE;i++){
            if(i%5000000==0){
                //auxMap.put(i,i);
                System.out.println(map.size());
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
        System.out.println("\tUsed Memory: "
                + (runtime.totalMemory() - runtime.freeMemory()) / mb + " mb");
    }





}
