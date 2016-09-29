package PredictiveIndex;



import com.mchange.v2.async.ThreadPoolAsynchronousRunner;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.*;
import it.unimi.dsi.fastutil.ints.*;
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

public class InvertedIndex implements Serializable {

    static final String path = "/home/aalto/IdeaProjects/PredictiveIndex/data";
    static final String tPath = path + "/termMap";
    static final String fPath = path + "/freqMap";
    static final String docMapPath = path + "/docMapPath";
    static final String sPath = path + "/stats";
    static final String dPath = path + "/dump";
    static final String fIndexPath = path + "/FI";

    static final String globalI2 = "/home/aalto/IdeaProjects/PredictiveIndex/data/global/rawInvertedIndex/";
    private static String metadata = "/home/aalto/IdeaProjects/PredictiveIndex/data/metadata/";

    static final String ser = ".ser";
    public static double start;
    public static double now;
    static int wordsCount = 0;
    static private double maxBM25 = 0;
    static private double minBM25 =2147388309;
    public static int totNumDocs = 50220423;
    static final int testLimit = (int) (51*Math.pow(10,6));
    static final int bufferSize = (int) (2*Math.pow(10,7));
    static final Object flag = new Object();
    static AtomicInteger dump = new AtomicInteger(0);
    DataOutputStream [] DOS = new DataOutputStream[4];
    Long2IntOpenHashMap [] dumpMap = new Long2IntOpenHashMap[4];
    final Long2IntOpenHashMap relevantPairs = (Long2IntOpenHashMap) getOIStream(metadata+"queryOccurences", true).readObject();


    private DataOutputStream invertedIndexFile;
    final private int distance = 5;
    //private AtomicInteger pointer = new AtomicInteger(0);
    private int pointer =0;
    //private int [][] buffer;
    public long [] globalStats;                                     //1-numberofdocs,2-wordcounter,3-unique words
    public long doc = 0;
    //private Int2IntMap globalFreqMap;
    int []  globalFreqMap;
    //private ConcurrentMap<Integer,Integer> globalFreqMap;

    InvertedIndex(String fold) throws IOException, ClassNotFoundException {
        this.globalStats = new long[3];
        globalFreqMap = new int[91553702];
        //globalFreqMap = new int[87262395];
        //this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fold + "InvertedIndex.bin")));
    }

    InvertedIndex(int[] globalFreqMap, long[] globalStats, String fold) throws IOException, ClassNotFoundException {
        this.globalFreqMap = globalFreqMap;
        this.globalStats = globalStats;
        //buffer = new int[bufferSize][4];
        //this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fold + "InvertedIndex.bin")));
    }


    /** 1TH PHASE - GET METADATA */

    /* The file is stored in binary form with the firs bit as a continuation bit.
    * 0 - document title | 1 - docID | 2 - offset (varbyte) | 3 - size (varbyte) | 4 - docLength (#words)
    */

    protected void getClueWebMetadata(String fold) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Collecting ClueWeb09 global statistics...");
        start = System.currentTimeMillis();
        DataOutputStream localMetadata = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fold+"localTermStats.bin", false)));
        DataInputStream stream = new DataInputStream(new BufferedInputStream( new FileInputStream(fold+"clueweb.bin")));
        BufferedReader br = new BufferedReader(new FileReader(fold+"docInfo.csv"));
        String line = br.readLine();
        String [] record;
        Int2IntMap position = new Int2IntOpenHashMap();
        int [] document = new int[127525*2];
        while(line != null & checkProgress(doc, totNumDocs, 500000, start, testLimit)){
            record = line.split(" ");
            storeMetadata(readClueWebDocument(record, stream, document), Integer.parseInt(record[1]), position, Integer.parseInt(record[4]), localMetadata);
            position.clear();
            line = br.readLine();
            doc++;
        }
        localMetadata.close();
        System.out.println("ClueWeb09 global statistics collected! " + doc);

    }
    private void storeMetadata(int [] words, int docID, Int2IntMap position, int docLen, DataOutputStream forwardIndexFile) throws IOException {
        /*this function process the single wrac files */
        int multipleOccurece = 0;
        for (int k = 0; k<docLen; k++) {
            if (position.putIfAbsent(words[k], 1) == null){
                globalFreqMap[words[k]]++;
                this.globalStats[2]++;
            }else{
                if(position.merge(words[k], 1, Integer::sum)==2) multipleOccurece++;            }
        }
        storeHashMap(position, forwardIndexFile, multipleOccurece);
        this.globalStats[0]++;
        this.globalStats[1]+= words.length;
    }

    /** 2ND PHASE - BUILD INVERTED INDEX */

    protected void buildDBigramInvertedIndex(String fold, int tn) throws IOException, ClassNotFoundException, InterruptedException {
        start = System.currentTimeMillis();
        int [][] buffer = new int[bufferSize][4];
        int pointer = 0;
        System.out.println("Building D-Bigram Inverted Index...");

        dumpMap[tn] = new Long2IntOpenHashMap();
        DOS[tn] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(globalI2+dump.getAndAdd(1), false)));
        DataInputStream stream = new DataInputStream( new BufferedInputStream( new FileInputStream(fold + "clueweb.bin")));
        DataInputStream DIS = new DataInputStream(new BufferedInputStream(new FileInputStream(fold + "localTermStats.bin")));
        BufferedReader br = new BufferedReader(new FileReader(fold + "docInfo.csv"));

        String[] line;
        String record;
        int [] document = new int[127525];
        Int2IntMap bufferMap = new Int2IntOpenHashMap();
        LongSet bufferSet = new LongOpenHashSet();
        int [] buffPair = new int[2];
        bufferMap.defaultReturnValue(1);
        int debug=0;
        for(record = br.readLine(); record!=null & checkProgress(doc, totNumDocs, 500000, start, testLimit); record = br.readLine()){
            line = record.split(" ");
            readClueWebDocument(line, stream, document);
            fetchHashMap(bufferMap, DIS);
            pointer = bufferedIndex(document, line, bufferMap , bufferSet, buffPair, pointer, buffer, tn);
            bufferMap.clear();
            bufferSet.clear();
            doc++;
        }
        sampledSelection(buffer, tn);
        serialize(dumpMap[tn], globalFold+"/dumped/"+tn );
        DOS[tn].close();
        //this.invertedIndexFile.close();
        DIS.close();
        System.out.println("D-Bigram Inverted Index Built!");
    }

    public int bufferedIndex(int[] words, String [] line, Int2IntMap localFreqMap, LongSet bufferSet, int [] pair, int pointer, int[][] buffer, int tn) throws IOException, ClassNotFoundException, InterruptedException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        wordsCount += words.length;
        int docLen =  Integer.parseInt(line[4]);
        int score1;
        int score2;
        int movingDistance = distance;
        for (int wIx = 0; wIx < docLen; wIx++) {
            if(docLen - wIx < distance) movingDistance = (docLen - wIx);
            for (int dIx = wIx+1; dIx < wIx + movingDistance; dIx++) {
                pair[0] = words[wIx] ;
                pair[1] = words[dIx] ;
                Arrays.sort(pair);
                if((bufferSet.add(getPair(pair[0], pair[1])) & relevantPairs.containsKey(getPair(pair[0],pair[1])))) {
                    score1 = getBM25(globalStats, docLen, localFreqMap.get(pair[0]), globalFreqMap[pair[0]]);
                    score2 = getBM25(globalStats, docLen, localFreqMap.get(pair[1]), globalFreqMap[pair[1]]);
                    if(pointer == buffer.length){
                        sampledSelection(buffer, tn);
                        DOS[tn].close();
                        DOS[tn] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(globalI2+dump.getAndAdd(1), false)));
                        pointer = 0;
                    }
                    buffer[pointer][0] = pair[0];
                    buffer[pointer][1] = pair[1];
                    buffer[pointer][2] = score1 + score2;
                    buffer[pointer][3] = Integer.parseInt(line[1]);
                    pointer++;
                }
            }//if(getLocalFreq(localFreqMap, words[wIx])==1) ones++;
        }
        return pointer;
        //System.out.println("Ones: " +title+ "\t" + ones);
    }


    public void storeSelectionStats(Long2IntOpenHashMap map) throws IOException {
        int [] terms;
        for(long pair : map.keySet())
        {
            terms = getTerms(pair);
            this.invertedIndexFile.writeInt(terms[0]);
            this.invertedIndexFile.writeInt(terms[1]);
            this.invertedIndexFile.writeInt(-1);
            this.invertedIndexFile.writeInt(map.get(pair));
        }
    }

    /*else{
                pair = getPair(buffer[k][0],buffer[k][1]);
                if(dumpCounter.putIfAbsent(pair,1) != null){
                    dumpCounter.merge(pair, 1, Integer::sum);
                }
            }
        }
        storeSelectionStats(dumpCounter);*/

    private void sampledSelection(int[][] buffer, int tn) throws IOException {
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now = System.currentTimeMillis();
        int threshold = getThreshold(buffer);
        long pair;
        int keep = 0;
        for (int k = 0; k < buffer.length; k++) {
            if (buffer[k][2] > threshold) {
                //buffer[keep++] = buffer[k];
                for (int elem : buffer[k]) DOS[tn].writeInt(elem);
                if(maxBM25<buffer[k][2]) maxBM25 = buffer[k][2];
            }else
                dumpMap[tn].addTo(getPair(buffer[k][0],buffer[k][1]), 1);
        }
        System.out.println(keep);
        /*java.util.Arrays.sort(buffer,0, keep, new Comparator<int[]>() {
            @Override
            public int compare(int[] int1, int[] int2) {
                if (int1[0] == int2[0]) {
                    if(int1[1] == int2[1]){
                        return Integer.compare(int1[2], int2[2]) * -1;
                    }else return Integer.compare(int1[1], int2[1]);
                } else return Integer.compare(int1[0], int2[0]);
            }
        });
        for (int k = 0; k < keep; k++) {
            for (int elem : buffer[k]) DOS[tn].writeInt(elem);
        }*/
        System.out.println("Sampled Natural Selection:" + (System.currentTimeMillis() - now) + "ms.\tThreshold: " + threshold +"\t MaxBM25: " + maxBM25);
        maxBM25 = 0;
        System.out.println("Dump size "+ dump +" " +dumpMap[tn].size());
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    private int getThreshold(int [][] buffer){
        int rnd;
        int sampleLength= (int) (bufferSize*0.002);
        //sampleLength= bufferSize;

        int[] sample = new int[sampleLength];
        for(int k = 0; k<sample.length; k++) {
            rnd = ThreadLocalRandom.current().nextInt(0, bufferSize-1);
            //System.out.println(buffer[rnd][2]);
            sample[k] = buffer[rnd][2];
        }
        java.util.Arrays.sort(sample);
        return sample[(int) (sampleLength*0.8)];
    }
}






