package PredictiveIndex;



import com.mchange.v2.async.ThreadPoolAsynchronousRunner;
import it.unimi.dsi.fastutil.longs.*;

import java.util.*;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;


import static PredictiveIndex.utilsClass.*;

/**
 * Created by aalto on 6/24/16.
 *
 */

public class InvertedIndex extends WWW {
    static final int testLimit = 50222043;
    static final int bufferSize = (int) (3*Math.pow(10,7));
    final private int distance;
    final static int threadNum = 4;

    LongOpenHashSet bigFS ;
    LongOpenHashSet smallFS;
    IntOpenHashSet uniTerms;
    Long2LongOpenHashMap dMap       = new Long2LongOpenHashMap();          // new Long2LongOpenHashMap[threadNum];
    Int2IntOpenHashMap [] auxFMap   = new Int2IntOpenHashMap[threadNum];
    long [] globalStats;                                     //1-numberofdocs,2-wordcounter
    public long doc = 1;
    int []  locFreqArr;
    Int2IntMap locFreqMap;
    static AtomicInteger dump = new AtomicInteger(0);
    long [] dmpPost = new long[4];


    DataInputStream     [] ClueDIS  =   new DataInputStream[threadNum];
    BufferedReader      [] BR       =   new BufferedReader[threadNum];

    DataInputStream     [] localStatsDIS =   new DataInputStream[threadNum];        //in the first fase output, in the second input

    DataOutputStream    [] DOS      =   new DataOutputStream[threadNum];



    int             [][][] buffer   =   new int[threadNum][][];

    int             [] keepPointers =   new int[]{0,0,0,0};
    int             [] pointers     =   new int[]{0,0,0,0};



    /*********************************************************** CONSTRUCTORS ***********************************************************/

    public InvertedIndex(int distance, int numThreads) throws IOException, ClassNotFoundException {
        this.globalStats = new long[2];
        locFreqArr = new int[91553702];
        this.distance = distance;

        this.uniTerms = (IntOpenHashSet) deserialize(uniqueTerms);

        for (int i = 0; i < numThreads ; i++) {
            ClueDIS[i]  = getDIStream(CW[i]);
            BR[i]       = getBuffReader(docInfo[i]);
            DOS[i]      = getDOStream(docStat[i]);
            auxFMap[i]  = new Int2IntOpenHashMap();
        }
    }

    public InvertedIndex(Int2IntOpenHashMap locFreqMap, long[] globalStats, int distance, int numThreads) throws IOException, ClassNotFoundException {
        this.locFreqMap = locFreqMap;
        this.globalStats = globalStats;
        this.distance = distance;
        this.bigFS = (LongOpenHashSet) deserialize(bigFilterSet);
        this.smallFS = (LongOpenHashSet) deserialize((smallFilterSet));

        for (int i = 0; i < numThreads ; i++) {
            ClueDIS[i]  = getDIStream(CW[i]);
            System.out.println(CW[i]);
            BR[i]       = getBuffReader(docInfo[i]);

            localStatsDIS[i] = getDIStream(docStat[i]);

            DOS[i]  = getDOStream(rawI2+dump.getAndAdd(1));;

            buffer[i]   = new int[bufferSize][4];
            //dMap[i]     = new Long2LongOpenHashMap();
        }
    }


    /***********************************************************  GET METADATA ***********************************************************/

    /* The file is stored in binary form with the firs bit as a continuation bit.
    * 0 - document title | 1 - docID | 2 - ofbigFS (varbyte) | 3 - size (varbyte) | 4 - docLength (#words)
    */

    protected void getClueWebMetadata(int tn) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Collecting ClueWeb09 global statistics...");
        start = System.currentTimeMillis();
        String line;
        String [] field = new String[5];
        Int2IntMap position = new Int2IntOpenHashMap();
        int [] document = new int[127525*2];

        for(line = BR[tn].readLine() ; line!=null & checkProgress(doc, totNumDocs, 500000, start, testLimit); line = BR[tn].readLine()){
            if((field = line.split(" ")).length != 5) break;
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
        int maxFreq = Integer.MIN_VALUE;
        for (int k = 0; k<docLen; k++) {
            if (position.putIfAbsent(words[k], 1) == null) {
                locFreqArr[words[k]]++;                                  //how many documents contain words[k]
            }else if (position.merge(words[k], 1, Integer::sum) == 2) multipleOccurece++;

            if (position.get(words[k]) > maxFreq) maxFreq = position.get(words[k]);
        }
        position.put(-99, maxFreq);
        storeHashMap(position, DOS[tn], multipleOccurece);
        this.globalStats[0]++;
        this.globalStats[1]+= words.length;
    }





    /*********************************************************** BUILD INVERTED INDEX ***********************************************************/

    private void checkDecoding(int [] d, int[][] c){
        for (int i = 0; i < d.length ; i++) {
            if(d[i]!=c[(int)doc-1][i]){
                System.out.print(d[i] +","+ c[(int)doc][i] + " " );
            }else
                System.out.println("OKOK");
        }System.out.println();
    }

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
        //int [][] check = (int[][]) deserialize(array30);

        for(line = BR[tn].readLine(); line!=null & checkProgress(doc, totNumDocs, 500000, start, testLimit); line = BR[tn].readLine()){
            field = line.split(" ");
            if(field.length != 5) break;
            readClueWebDocument(field, ClueDIS[tn], document);

            fetchHashMap(bufferMap, localStatsDIS[tn]);
            bufferedIndex(document, field, bufferMap , noDuplicateSet, buffPair, tn);

            bufferMap.clear();
            noDuplicateSet.clear();
            doc++;
        }
        sampledSelection(tn, buffPair);

        DOS[tn].close();
        ClueDIS[tn].close();
        localStatsDIS[tn].close();
        BR[tn].close();

        System.out.println("D-Bigram Inverted Index Built!");
    }

    public void bufferedIndex(int[] words, String [] field, Int2IntMap localFreqMap, LongSet noDuplicateSet, int [] twoTerms, int tn) throws IOException, ClassNotFoundException, InterruptedException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        int docLen =  Integer.parseInt(field[4]);
        int score1;
        int score2;
        int movingDistance = distance;
        int localMaxFreq = localFreqMap.get(-99);
        long pair;

        for (int wIx = 0; wIx < docLen; wIx++) {
            if(docLen - wIx < distance) movingDistance = (docLen - wIx);
            for (int dIx = wIx+1; dIx < wIx + movingDistance; dIx++) {

                twoTerms[0] = words[wIx] ;
                twoTerms[1] = words[dIx] ;
                Arrays.sort(twoTerms);
                pair = getPair(twoTerms[0], twoTerms[1]);
                if(noDuplicateSet.add(pair) & bigFS.contains(pair)){
                    if(smallFS.contains(pair)){
                        if(pointers[tn] == buffer[tn].length){
                            sampledSelection(tn, twoTerms);
                            pointers[tn] = keepPointers[tn];
                        }
                        incrementPostingList(tn, twoTerms, pair);

                        score1 = getBM25(globalStats, docLen, localFreqMap.get(twoTerms[0]), localMaxFreq , locFreqMap.get(twoTerms[0]));
                        score2 = getBM25(globalStats, docLen, localFreqMap.get(twoTerms[1]), localMaxFreq , locFreqMap.get(twoTerms[1]));

                        buffer[tn][pointers[tn]][0] = twoTerms[0];
                        buffer[tn][pointers[tn]][1] = twoTerms[1];
                        buffer[tn][pointers[tn]][2] = score1 + score2;
                        buffer[tn][pointers[tn]][3] = Integer.parseInt(field[1]);
                        pointers[tn]++;
                    }else
                        dmpPost[tn]++;
                }
            }
        }
    }

    private synchronized void incrementPostingList(int tn, int [] t, long pair){
        t = getTerms(dMap.get(pair));
        dMap.put(pair,getPair(t[0]++, t[1]));
    }

    private synchronized void incrementDumpCounter(int tn, int [] t, long pair){
        t = getTerms(dMap.get(pair));
        dMap.put(pair,getPair(t[0], t[1]++));
    }



    /*private synchronized void incrementMap(long pair){
        dMap.addTo(pair, 1);
    }*/

    private void sampledSelection(int tn, int [] twoTerms) throws IOException{
        //System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now = System.currentTimeMillis();
        int threshold = getThreshold(tn);

        for (int k = keepPointers[tn]; k < buffer[tn].length; k++) {
            if (buffer[tn][k][2] > threshold) {
                buffer[tn][keepPointers[tn]++] = buffer[tn][k];
            }else
                incrementDumpCounter(tn, twoTerms, getPair(buffer[tn][k][0], buffer[tn][k][1]));
                //dMap[tn].addTo(getPair(buffer[tn][k][0], buffer[tn][k][1]),1);
                //incrementMap(getPair(buffer[tn][k][0], buffer[tn][k][1]));

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






