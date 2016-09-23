package PredictiveIndex;



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
    static final String ser = ".ser";
    public static double start;
    public static double now;
    static int wordsCount = 0;
    static private double maxBM25 = 0;
    static private double minBM25 =2147388309;
    public static int totNumDocs = 50220423;
    static final int testLimit = (int) (5*Math.pow(10,8));
    static final int bufferSize = (int) (1*Math.pow(10,7));


    private DataOutputStream invertedIndexFile;
    final private int distance = 10;
    private int pointer = 0;
    private int [][] buffer;
    public int[] globalStats;                                     //1-numberofdocs,2-wordcounter,3-unique words
    public int doc = 0;
    //private Int2IntMap globalFreqMap;
    short []  globalFreqMap;
    //private ConcurrentMap<Integer,Integer> globalFreqMap;

    InvertedIndex(String fold) throws IOException {
        this.globalStats = new int[3];
        globalFreqMap = new short[87262395];
        this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fold + "InvertedIndex.bin")));
    }

    InvertedIndex(short[] globalFreqMap, int[] globalStats, String fold) throws IOException {
        this.globalFreqMap = globalFreqMap;
        this.globalStats = globalStats;
        this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fold + "InvertedIndex.bin")));
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
        storeHashMap(position, forwardIndexFile ,multipleOccurece);
        this.globalStats[0]++;
        this.globalStats[1]+= words.length;
    }

    /** 2ND PHASE - BUILD INVERTED INDEX */

    protected void buildDBigramInvertedIndex(String info) throws IOException, ClassNotFoundException, InterruptedException {
        start = System.currentTimeMillis();
        System.out.println("Building D-Bigram Inverted Index...");
        this.buffer = new int[bufferSize][4];
        DataInputStream stream = new DataInputStream( new BufferedInputStream( new FileInputStream("/home/aalto/dio/compressedIndex")));
        BufferedReader br = new BufferedReader(new FileReader(info));
        DataInputStream DIS = new DataInputStream(new BufferedInputStream(new FileInputStream(fIndexPath + "/forwardIndexMetadata")));
        String[] line = br.readLine().split(" ");
        int [] document = new int[127525];
        Int2IntMap bufferMap = new Int2IntOpenHashMap();
        while(line[0] != null & checkProgress(doc, totNumDocs, 500000, start, testLimit)){
            bufferedIndex(readClueWebDocument(line, stream, document), Integer.parseInt(line[1]), fetchHashMap(bufferMap, DIS));
            bufferMap.clear();
            line = br.readLine().split(" ");
            doc++;
        }
        sampledSelection();
        this.invertedIndexFile.close();
        DIS.close();
        System.out.println("D-Bigram Inverted Index Built!");
    }

    public void bufferedIndex(int[] words, int title, Int2IntMap localFreqMap) throws IOException, ClassNotFoundException, InterruptedException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        wordsCount += words.length;
        int score1;
        int score2;
        int ones=0;
        int movingDistance = distance;
        LongSet auxPair = new LongOpenHashSet();
        localFreqMap.defaultReturnValue(1);
        for (int wIx = 0; wIx < words.length; wIx++) {
            if(words.length - wIx < distance) movingDistance = (words.length - wIx);
            for (int dIx = wIx+1; dIx < wIx + movingDistance; dIx++) {
                int [] pair = {words[wIx], words[dIx]};
                Arrays.sort(pair);
                if(auxPair.add(getPair(pair[0], pair[1]))) {
                    score1 = getBM25(globalStats, words.length, localFreqMap.get(pair[0]), globalFreqMap[pair[0]]);
                    score2 = getBM25(globalStats, words.length, localFreqMap.get(pair[1]), globalFreqMap[pair[1]]);
                    this.buffer[pointer] = new int[]{pair[0], pair[1], score1+score2, title};
                    checkBuffer();
                }
            }//if(getLocalFreq(localFreqMap, words[wIx])==1) ones++;
        }
        //System.out.println("Ones: " +title+ "\t" + ones);
    }

    private static int getLocalFreq(Int2IntMap docStat, int termID){
        /*We try to get the frequency of a term using the termID-freq hashmap. If a NullPointerException error is raised
        * than it mean that the term freq is just 1.*/
        int freq = docStat.get(termID);
        if(freq == 0) return 1;
        else return freq;
    }

    private void checkBuffer() throws IOException {
        if (pointer == buffer.length - 1){
            sampledSelection();
            pointer = 0;
        }else pointer++;
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
                pair = getPair(this.buffer[k][0],this.buffer[k][1]);
                if(dumpCounter.putIfAbsent(pair,1) != null){
                    dumpCounter.merge(pair, 1, Integer::sum);
                }
            }
        }
        storeSelectionStats(dumpCounter);*/
    private void sampledSelection() throws IOException {
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        Long2IntOpenHashMap dumpCounter = new Long2IntOpenHashMap();
        now = System.currentTimeMillis();
        int threshold = getThreshold();
        long pair;
        int keep = 0;
        for (int k = 0; k < this.buffer.length; k++) {
            if (this.buffer[k][2] > threshold) {
                this.buffer[keep] = this.buffer[k];
                //for (int elem : this.buffer[k]) this.invertedIndexFile.writeInt(elem);
                if(maxBM25<this.buffer[k][2]) maxBM25 = this.buffer[k][2];
                keep++;
            }
        }
        /*java.util.Arrays.parallelSort(this.buffer,0, keep, new Comparator<int[]>() {
            @Override
            public int compare(int[] int1, int[] int2) {
                if (int1[0] == int2[0]) {
                    if(int1[1] == int2[1]){
                        return Integer.compare(int1[2], int2[2]) * -1;
                    }else return Integer.compare(int1[1], int2[1]);
                } else return Integer.compare(int1[0], int2[0]);
            }
        });*/
        for (int k = 0; k < keep; k++) {
            for (int elem : this.buffer[k]) this.invertedIndexFile.writeInt(elem);
        }
        System.out.println("Sampled Natural Selection:" + (System.currentTimeMillis() - now) + "ms.\tThreshold: " + threshold +"\t MaxBM25: " + maxBM25);
        maxBM25 = 0;
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    private int getThreshold(){
        int rnd;
        int sampleLength= (int) (bufferSize*0.002);
        //sampleLength= bufferSize;

        int[] sample = new int[sampleLength];
        for(int k = 0; k<sample.length; k++) {
            rnd = ThreadLocalRandom.current().nextInt(0, bufferSize-1);
            //System.out.println(this.buffer[rnd][2]);
            sample[k] = this.buffer[rnd][2];
        }
        java.util.Arrays.parallelSort(sample);
        return sample[(int) (sampleLength*0.8)];
    }
}






