package PredictiveIndex;



import com.mchange.v2.async.ThreadPoolAsynchronousRunner;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.*;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static PredictiveIndex.PredictiveIndex.dataFold;
import static PredictiveIndex.PredictiveIndex.globalFold;
import static PredictiveIndex.utilsClass.*;

/**
 * Created by aalto on 6/24/16.
 *
 */

public class InvertedIndex extends WWW {
    static final int testLimit = (int) (51*Math.pow(10,6));
    static final int bufferSize = (int) (3*Math.pow(10,7));
    final private int distance;
    final static int threadNum = 4;

    static LongOpenHashSet fSet ;
    Long2IntOpenHashMap dMap = new Long2IntOpenHashMap();
    public long [] globalStats;                                     //1-numberofdocs,2-wordcounter,3-unique words
    public long doc = 0;
    int []  globalFreqMap;
    static AtomicInteger dump = new AtomicInteger(0);


    DataOutputStream    [] DOS      =   new DataOutputStream[threadNum];
    DataInputStream     [] ClueDIS  =   new DataInputStream[threadNum];
    DataInputStream     [] StatsDIS =   new DataInputStream[threadNum];
    BufferedReader      [] BR       =   new BufferedReader[threadNum];
    int             [][][] buffer   =   new int[threadNum][][];
    int [] keepPointers             =   new int[]{0,0,0,0};
    int [] pointers                 =   new int[]{0,0,0,0};



    /***CONSTRUCTORS***/

    public InvertedIndex(int distance, int numThreads) throws IOException, ClassNotFoundException {
        this.globalStats = new long[3];
        globalFreqMap = new int[91553702];
        this.distance = distance;

        for (int i = 0; i < numThreads ; i++) {
            DOS[i]      = getDOStream(docStat[i]);
            ClueDIS[i]  = getDIStream(CW[i]);
            BR[i]       = getBuffReader(docInfo[i]);
        }
    }

    public InvertedIndex(int[] globalFreqMap, long[] globalStats, int distance, int numThreads) throws IOException, ClassNotFoundException {
        this.globalFreqMap = globalFreqMap;
        this.globalStats = globalStats;
        this.distance = distance;
        this.fSet = (LongOpenHashSet) deserialize(filterSet);

        for (int i = 0; i < numThreads ; i++) {
            DOS[i]  = getDOStream(rawI2+dump.getAndAdd(1));;
            ClueDIS[i]  = getDIStream(CW[i]);
            StatsDIS[i] = getDIStream(docStat[i]);
            BR[i]       = getBuffReader(docInfo[i]);
            buffer[i]   = new int[bufferSize][4];
        }
    }


    /*** GET METADATA ***/

    /* The file is stored in binary form with the firs bit as a continuation bit.
    * 0 - document title | 1 - docID | 2 - offset (varbyte) | 3 - size (varbyte) | 4 - docLength (#words)
    */

    protected void getClueWebMetadata(int tn) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Collecting ClueWeb09 global statistics...");
        start = System.currentTimeMillis();
        String line;
        String [] field = new String[5];
        Int2IntMap position = new Int2IntOpenHashMap();
        int [] document = new int[127525*2];

        for(line = BR[tn].readLine() ; line!=null & checkProgress(doc, totNumDocs, 500000, start, testLimit) & field.length == 5; line = BR[tn].readLine()){
            field = line.split(" ");
            storeMetadata(readClueWebDocument(field, ClueDIS[tn], document), Integer.parseInt(field[1]), Integer.parseInt(field[4]), position, tn);
            position.clear();
            doc++;
        }
        DOS[tn].close();
        System.out.println("ClueWeb09 global statistics collected! " + doc);

    }
    private void storeMetadata(int [] words, int docID, int docLen, Int2IntMap position, int tn) throws IOException {
        /*this function process the single wrac files */
        int multipleOccurece = 0;
        for (int k = 0; k<docLen; k++) {
            if (position.putIfAbsent(words[k], 1) == null){
                globalFreqMap[words[k]]++;
                this.globalStats[2]++;
            }else{
                if(position.merge(words[k], 1, Integer::sum)==2) multipleOccurece++;            }
        }
        storeHashMap(position, DOS[tn], multipleOccurece);
        this.globalStats[0]++;
        this.globalStats[1]+= words.length;
    }




    /*** BUILD INVERTED INDEX ***/

    protected void buildDBigramInvertedIndex(int tn) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Building D-Bigram Inverted Index...");
        start = System.currentTimeMillis();

        String [] field;
        String line;
        int [] document = new int[127525];
        int [] buffPair = new int[2];

        Int2IntMap bufferMap = new Int2IntOpenHashMap();
        bufferMap.defaultReturnValue(1);
        LongSet noDuplicateSet = new LongOpenHashSet();


        for(line = BR[tn].readLine(); line!=null & checkProgress(doc, totNumDocs, 500000, start, testLimit); line = BR[tn].readLine()){
            field = line.split(" ");
            if(field.length != 5) break;
            readClueWebDocument(field, ClueDIS[tn], document);
            fetchHashMap(bufferMap, StatsDIS[tn]);
            bufferedIndex(document, field, bufferMap , noDuplicateSet, buffPair, tn);

            bufferMap.clear();
            noDuplicateSet.clear();
            doc++;
        }
        sampledSelection(tn);

        DOS[tn].close();
        ClueDIS[tn].close();
        StatsDIS[tn].close();
        BR[tn].close();

        System.out.println("D-Bigram Inverted Index Built!");
    }

    public void bufferedIndex(int[] words, String [] field, Int2IntMap localFreqMap, LongSet noDuplicateSet, int [] pair, int tn) throws IOException, ClassNotFoundException, InterruptedException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        int docLen =  Integer.parseInt(field[4]);
        int score1;
        int score2;
        int movingDistance = distance;

        for (int wIx = 0; wIx < docLen; wIx++) {
            if(docLen - wIx < distance) movingDistance = (docLen - wIx);
            for (int dIx = wIx+1; dIx < wIx + movingDistance; dIx++) {

                pair[0] = words[wIx] ;
                pair[1] = words[dIx] ;
                Arrays.sort(pair);

                if((noDuplicateSet.add(getPair(pair[0], pair[1])) & fSet.contains(getPair(pair[0],pair[1])))) {
                    score1 = getBM25(globalStats, docLen, localFreqMap.get(pair[0]), globalFreqMap[pair[0]]);
                    score2 = getBM25(globalStats, docLen, localFreqMap.get(pair[1]), globalFreqMap[pair[1]]);

                    if(pointers[tn] == buffer[tn].length){
                        sampledSelection(tn);
                        pointers[tn] = keepPointers[tn];
                    }

                    buffer[tn][pointers[tn]][0] = pair[0];
                    buffer[tn][pointers[tn]][1] = pair[1];
                    buffer[tn][pointers[tn]][2] = score1 + score2;
                    buffer[tn][pointers[tn]][3] = Integer.parseInt(field[1]);
                    pointers[tn]++;
                }
            }
        }
    }

    private synchronized void incrementMap(long pair){
        dMap.addTo(pair, 1);
    }

    private void sampledSelection(int tn) throws IOException {
        //System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now = System.currentTimeMillis();
        int threshold = getThreshold(tn);

        for (int k = keepPointers[tn]; k < buffer[tn].length; k++) {
            if (buffer[tn][k][2] > threshold) {
                buffer[tn][keepPointers[tn]++] = buffer[tn][k];
            }else
                incrementMap(getPair(buffer[tn][k][0], buffer[tn][k][1]));
        }


        if(keepPointers[tn] > (buffer[tn].length/100)*90){
            sortBuffer(tn);

            System.out.print("Flushing Buffer...\t");
            for (int k = 0; k < keepPointers[tn]; k++) {
                for (int elem : buffer[tn][k]) DOS[tn].writeInt(elem);
            }System.out.print("Done.");

            //System.out.println("Processed docs: " + doc + "Sampled Natural Selection:" + (System.currentTimeMillis() - now) + "ms.\tThreshold: " + threshold +"\t MaxBM25: " + maxBM25);
            DOS[tn].close();
            DOS[tn] = getDOStream(rawI2+dump.getAndAdd(1));
            keepPointers[tn] = 0;
        }


        maxBM25 = 0;
        //System.out.println("Dump size "+ dump +" " +dMap.size());
        //System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    private void sortBuffer(int tn){
        java.util.Arrays.sort(buffer[tn],0,keepPointers[tn], new Comparator<int[]>() {
            @Override
            public int compare(int[] int1, int[] int2) {
                if (int1[0] == int2[0]) {
                    if(int1[1] == int2[1]){
                        return Integer.compare(int1[2], int2[2]) * -1;
                    }else return Integer.compare(int1[1], int2[1]);
                } else return Integer.compare(int1[0], int2[0]);
            }
        });
    }

    private int getThreshold(int tn){
        int rnd;
        int[] sample = new int[(int) ((bufferSize-keepPointers[tn])*0.002)];

        for(int k = 0; k<sample.length; k++) {
            rnd = ThreadLocalRandom.current().nextInt(keepPointers[tn], bufferSize-1);
            sample[k] = buffer[tn][rnd][2];
        }

        java.util.Arrays.sort(sample);

        return sample[(int)(sample.length*0.8)];
    }
}






