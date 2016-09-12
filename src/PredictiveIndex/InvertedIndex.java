package PredictiveIndex;



import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.*;
import it.unimi.dsi.fastutil.ints.*;
import java.io.*;
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
    static final int testLimit = (int) (5*Math.pow(10,6));
    static final int bufferSize = (int) (1*Math.pow(10,7));


    private DataOutputStream invertedIndexFile;
    private ObjectOutputStream forwardIndexFile;
    final private int distance = 10;
    private int pointer = 0;
    private int [][] buffer;
    private int[] globalStats;                                     //1-numberofdocs,2-wordcounter,3-unique words
    public int doc = 0;
    private Int2IntMap globalFreqMap;

    InvertedIndex() throws IOException {
        globalFreqMap = new Int2IntOpenHashMap();
        this.globalStats = new int[3];
        this.forwardIndexFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fIndexPath + "/forwardIndexMetadata" + ".bin", true)));
        this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dPath + "/InvertedIndex.bin", true)));
    }

    InvertedIndex(Int2IntMap globalFreqMap, int[] globalStats) throws IOException {
        this.globalFreqMap = globalFreqMap;
        this.globalStats = globalStats;
        this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dPath + "/InvertedIndex.bin", true)));
    }


    /** 1TH PHASE - GET METADATA */

    /* The file is stored in binary form with the firs bit as a continuation bit.
    * 0 - document title | 1 - docID | 2 - offset (varbyte) | 3 - size (varbyte) | 4 - docLength (#words)
    */

    protected void getClueWebMetadata(String info) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Collecting ClueWeb09 global statistics...");
        start = System.currentTimeMillis();
        DataInputStream stream = new DataInputStream(new BufferedInputStream( new FileInputStream("/home/aalto/dio/compressedIndex")));
        BufferedReader br = new BufferedReader(new FileReader(info));
        String[] line = br.readLine().split(" ");
        int [] document;
        while(line[0] != null & checkProgress(doc, totNumDocs, 500000, start, testLimit)){
            document = readClueWebDocument(line, stream);
            storeMetadata(document, Integer.parseInt(line[1]));
            line = br.readLine().split(" ");
            doc++;
        }

        serialize(this.globalFreqMap, fPath);
        serialize(this.globalStats, sPath);
        this.forwardIndexFile.close();
        System.out.println("ClueWeb09 global statistics collected!");

    }

    private void storeMetadata(int [] words, int docID) throws IOException {
        /*this function process the single wrac files */
        Int2IntMap position = new Int2IntOpenHashMap();
        int multipleOccurece = 0;
        for (int k = 0; k<words.length; k++) {
            if (position.putIfAbsent(words[k], 1) == null){
                if(this.globalFreqMap.putIfAbsent(words[k], 1)!=null) {
                    this.globalFreqMap.merge(words[k], 1, Integer::sum);
                    this.globalStats[2]++;
                }
            }else{
                position.merge(words[k], 1, Integer::sum);
                if(position.get(words[k]) ==2) multipleOccurece++;
            }
        }
        //System.out.print("Removed " + docID + "\t" + (position.keySet().size()-multipleOccurece));
        this.forwardIndexFile.writeObject(hashMapToArray(position, multipleOccurece));
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
        ObjectInputStream OIStream = getOIStream(fIndexPath + "/forwardIndexMetadata" , true);
        String[] line = br.readLine().split(" ");
        int [] document;
        while(line[0] != null & checkProgress(doc, totNumDocs, 500000, start, testLimit)){
            document = readClueWebDocument(line, stream);
            bufferedIndex(document, Integer.parseInt(line[1]), arrayToHashMap((int[])  OIStream.readObject()));
            line = br.readLine().split(" ");
            doc++;
        }
        sampledSelection();
        this.invertedIndexFile.close();
        OIStream.close();
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
        for (int wIx = 0; wIx < words.length; wIx++) {
            if(words.length - wIx < distance) movingDistance = (words.length - wIx);
            for (int dIx = wIx+1; dIx < wIx + movingDistance; dIx++) {
                int [] pair = {words[wIx], words[dIx]};
                Arrays.sort(pair);
                if(auxPair.add(getPair(pair[0], pair[1]))) {

                    score1 = getBM25(globalStats, words.length, getLocalFreq(localFreqMap, pair[0]), globalFreqMap.get(pair[0]));
                    score2 = getBM25(globalStats, words.length, getLocalFreq(localFreqMap, pair[1]), globalFreqMap.get(pair[1]));
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
        for (int k = 0; k < this.buffer.length; k++) {
            if (this.buffer[k][2] > threshold) {
                if(maxBM25<this.buffer[k][2]) maxBM25 = this.buffer[k][2];

                for (int elem : this.buffer[k]) this.invertedIndexFile.writeInt(elem);
            }
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






