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
    static double start;
    static double now;
    static int wordsCount = 0;
    static private double maxBM25 = 0;
    static private double minBM25 =2147388309;
    public static int totNumDocs = 50220423;


    private DataOutputStream invertedIndexFile;
    private ObjectOutputStream forwardIndexFile;
    final private int distance = 5;
    private int pointer = 0;
    private int [][] buffer = new int[10000000][4];
    private int[] stats;                                     //1-numberofdocs,2-wordcounter,3-unique words
    private int doc = 0;
    private Int2IntMap freqTermDoc;

    InvertedIndex() throws IOException {
        freqTermDoc = new Int2IntOpenHashMap();
        this.stats = new int[3];
        this.forwardIndexFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fIndexPath + "/forwardIndexMetadata" + ".bin", true)));
        this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dPath + "/InvertedIndex.bin", true)));
    }

    InvertedIndex(Int2IntMap freqTermDoc, int[] stats) throws IOException {
        this.freqTermDoc = freqTermDoc;
        this.stats = stats;
        this.invertedIndexFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dPath + "/InvertedIndex.bin", true)));
    }


    /** 1TH PHASE - GET METADATA */

    /* The file is stored in binary form with the firs bit as a continuation bit.
    *
    * 0 - document title | 1 - docID | 2 - offset (varbyte) | 3 - size (varbyte) | 4 - docLength (#words)
    */

    public void readClueWeb(String data) throws IOException, ClassNotFoundException, InterruptedException {
        start = System.currentTimeMillis();
        ObjectInputStream OIStream = getOIStream(fIndexPath + "/forwardIndexMetadata.bin" , true);
        DataInputStream stream = new DataInputStream( new BufferedInputStream( new FileInputStream("/home/aalto/dio/compressedIndex")));
        BufferedReader br = new BufferedReader(new FileReader(data));
        String[] line = br.readLine().split(" ");
        byte [] rawDoc;
        int docID;
        int b2read;
        int docLen;
        int [] document;
        while(line[0] != null){
            checkProgress(doc, totNumDocs, 500000, start);
            if (doc % 100000 == 0 & doc!=0) break;

            docID = Integer.parseInt(line[1]);
            b2read= Integer.parseInt(line[3]);
            docLen= Integer.parseInt(line[4]);
            rawDoc = new byte[b2read];
            for (int i = 0; i < rawDoc.length; i++) {
                rawDoc[i] = stream.readByte();
            }
            document = decodeRawDoc(rawDoc, docLen);

            //storeMetadata(document);
            bufferedIndex(document, docID, arrayToHashMap((int[])  OIStream.readObject()));

            line = br.readLine().split(" ");
            doc++;
        }

        //serialize(this.freqTermDoc, fPath);
        //serialize(this.stats, sPath);
        //sampledNaturalSelection();
        this.invertedIndexFile.close();
        this.forwardIndexFile.close();
        System.out.println(this.stats[0]);
        System.out.println("Max BM25: " + maxBM25);
        System.exit(1);
    }

    private void storeMetadata(int [] words) throws IOException {
        /*this function process the single wrac files */
        Int2IntMap position = new Int2IntOpenHashMap();
        for (int k = 0; k<words.length-1; k++) {
            if (position.putIfAbsent(words[k], 1) == null){
                if(this.freqTermDoc.putIfAbsent(words[k], 1)!=null) {
                    this.freqTermDoc.merge(words[k], 1, Integer::sum);
                    this.stats[2]++;
                }
            }else{
                position.merge(words[k], 1, Integer::sum);
            }
        }
        this.forwardIndexFile.writeObject(hashMapToArray(position));
        this.stats[0]++;
        this.stats[1]+= words.length;
    }

    /** 2ND PHASE - BUILD INVERTED INDEX */

    private static int getTermFreq(Int2IntMap docStat, int termID){
        /*We try to get the frequency of a term using the termID-freq hashmap. If a NullPointerException error is raised
        * than it mean that the term freq is just 1.*/
        int freq = docStat.get(termID);
        if(freq == 0) return 1;
        else return freq;
    }

    private void checkBuffer() throws IOException {
        if (pointer == buffer.length - 1){
            sampledNaturalSelection();
            pointer = 0;
        }else pointer++;
    }

    public void bufferedIndex(int[] words, int title, Int2IntMap freqDocMap) throws IOException, ClassNotFoundException, InterruptedException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        int f1;
        int f2;
        wordsCount += words.length;
        LongSet auxPair = new LongOpenHashSet();
        for (int wIx = 0; wIx < words.length - this.distance; wIx++) {
            for (int dIx = wIx+1; dIx < wIx + this.distance; dIx++) {
                int [] pair = {words[wIx], words[dIx]};
                Arrays.sort(pair);
                f1 = getTermFreq(freqDocMap, pair[0]);
                f2 = getTermFreq(freqDocMap, pair[1]);
                if(auxPair.add(getPair(pair[0], pair[1]))) {
                    this.buffer[pointer] = new int[]{pair[0], pair[1], getBM25(pair[0], words.length, f1) + getBM25(pair[1], words.length, f2), title};
                    checkBuffer();
                }
            }
        }
    }

    private int getBM25(int id, int docLen, int f) {
        /*global statistics for BM25*/
        int N = this.stats[0];
        int n = this.freqTermDoc.get(id);
        double avg = this.stats[1] / N;
        double k = 1.6;
        double b = 0.75;
        double IDF = java.lang.Math.log((N - n + 0.5 )/( n + 0.5));
        double BM25 = (IDF * f * k + 1) / (f + k * (1 - b + (b* docLen / avg)));
        return (int) (BM25*Math.pow(10, 8));
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
    private void sampledNaturalSelection() throws IOException {
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
        System.out.println("Sampled Natural Selection:" + (System.currentTimeMillis() - now) + "ms. Threshold: " + threshold);
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    private int getThreshold(){
        int rnd;
        int sampleLength=10000;
        int[] sample = new int[sampleLength];
        for(int k = 0; k<sample.length; k++) {
            rnd = ThreadLocalRandom.current().nextInt(0, 10000000);
            sample[k] = this.buffer[rnd][2];
        }
        java.util.Arrays.parallelSort(sample);
        return sample[(int) (sampleLength*0.8)];
    }
}






