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
 * distance 2   143347 hits {6.7GB}     / t-0.5 - 95221 - 2.9GB / t-0.2 - 134626 -4.9GB
 * distance 5   188800 hits {11.6 GB} /
 * distance 10  68k hits 3.3GB
 *
 */




public class InvertedIndex extends WWW {
    static final int testLimit = 50222043;
    static final int bufferSize = (int) (2.8*Math.pow(10,7));
    private int distance;
    private boolean isBigram;
    private String prefix;
    final static int threadNum = 4;

    LongOpenHashSet bigFS ;
    LongOpenHashSet smallFS;
    IntOpenHashSet uniTerms;
    Long2LongOpenHashMap dBiMap       = new Long2LongOpenHashMap();          // new Long2LongOpenHashMap[threadNum];
    Int2LongOpenHashMap  uniMap = new Int2LongOpenHashMap();
    Int2LongOpenHashMap  hitMap = new Int2LongOpenHashMap();
    IntOpenHashSet missingWords = (IntOpenHashSet) deserialize(results+"missingSet");

    Int2IntOpenHashMap [] auxFMap   = new Int2IntOpenHashMap[threadNum];
    long [] globalStats;                                     //1-numberofdocs,2-wordcounter
    public long doc = 1;
    int []  termFreqArray;
    int [] HITS;
    Int2IntMap termFreqMap;
    static AtomicInteger dump = new AtomicInteger(0);
    static int gThreshold = 0;
    long [] dmpPost = new long[4];


    DataInputStream     [] ClueDIS  =   new DataInputStream[threadNum];
    BufferedReader      [] BR       =   new BufferedReader[threadNum];

    DataInputStream     [] localStatsDIS =   new DataInputStream[threadNum];        //in the first fase output, in the second input

    DataOutputStream    [] DOS      =   new DataOutputStream[threadNum];
    int                 [][] sample   =   new int[4][((int) (bufferSize*0.002))];



    int             [][][] buffer   =   new int[threadNum][][];

    int             [] keepPointers =   new int[]{0,0,0,0};
    int             [] pointers     =   new int[]{0,0,0,0};




    /*********************************************************** CONSTRUCTORS ***********************************************************/

    public InvertedIndex(int distance, int numThreads) throws IOException, ClassNotFoundException {
        this.globalStats = new long[2];
        termFreqArray = new int[91553702];
        this.distance = distance;

        this.uniTerms = (IntOpenHashSet) deserialize(uniqueTerms);

        for (int i = 0; i < numThreads ; i++) {
            ClueDIS[i]  = getDIStream(CW[i]);
            BR[i]       = getBuffReader(docInfo[i]);
            DOS[i]      = getDOStream(docStat[i]);
            auxFMap[i]  = new Int2IntOpenHashMap();
        }
    }

    public InvertedIndex(Int2IntOpenHashMap termFreqMap, int [] HITS, long[] globalStats, int distance, boolean isBgram, String prefix, int numThreads) throws IOException, ClassNotFoundException {
        if(!isBgram){
            this.uniTerms = (IntOpenHashSet) deserialize(uniqueTerms);
            this.HITS = HITS;
        }
        this.termFreqMap = termFreqMap;
        this.globalStats = globalStats;
        this.distance = distance;
        this.isBigram = isBgram;
        this.prefix = prefix ;
        this.bigFS = (LongOpenHashSet) deserialize(bigFilterSet);
        this.smallFS = (LongOpenHashSet) deserialize((smallFilterSet));

        for (int tn = 0; tn < numThreads ; tn++) {
            ClueDIS[tn]  = getDIStream(CW[tn]);
            System.out.println(CW[tn]);
            BR[tn]       = getBuffReader(docInfo[tn]);

            localStatsDIS[tn] = getDIStream(docStat[tn]);

            DOS[tn]  = getDOStream(prefix+rawI2+dump.getAndAdd(1));

            buffer[tn]   = new int[bufferSize][4];
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
            //checkMissingWords(readClueWebDocument(field, ClueDIS[tn], document), Integer.parseInt(field[4]));
            position.clear();
            doc++;
        }
        DOS[tn].close();
        System.out.println("ClueWeb09 global statistics collected! " + doc);

    }

    private void checkMissingWords(int [] words, int docLen){
        for (int k = 0; k<docLen; k++) {
            if(missingWords.remove(words[k])) System.out.println(missingWords.size());
        }
    }


    private void storeMetadata(int [] words, int docID, int docLen, Int2IntMap position, int tn) throws IOException {
        /*this function process the single wrac files */
        int multipleOccurece = 0;
        int maxFreq = Integer.MIN_VALUE;
        for (int k = 0; k<docLen; k++) {
            if (position.putIfAbsent(words[k], 1) == null) {
                termFreqArray[words[k]]++;                                  //how many documents contain words[k]
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

    private boolean consistencyCheck(int [] document, int docLen, BufferedWriter bw, String docID, int tn) throws IOException {
        bw.write("\n#doc: " + docID + "\n ---------------------------------------------------- \n");
        for (int k = 0; k< docLen; k++){
          bw.write(id2TermMap.get(document[k])+ " ");
        }
        //bw.write(Arrays.toString(Arrays.copyOfRange(document,0, docLen)));
        bw.newLine();
        if(doc>100){
            /*
            bw.close();
            Thread.currentThread().interrupt();
            */
            return true;
        }
        return false;
    }

    protected void buildDBigramInvertedIndex(int tn) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Building D-Bigram Inverted Index. Posting list number:" + smallFS.size() );
        start = System.currentTimeMillis();

        String [] field;
        String line;
        int [] document = new int[127525];
        int [] twoTerms = new int[2];

        Int2IntMap bufferMap = new Int2IntOpenHashMap();
        bufferMap.defaultReturnValue(1);
        LongSet noDuplicateSet = new LongOpenHashSet();
        //int [][] check = (int[][]) deserialize(array30);
        //BufferedWriter bw = getBuffWriter(results+tn+"converted");

        for(line = BR[tn].readLine(); line!=null & checkProgress(doc, totNumDocs, 500000, start, testLimit); line = BR[tn].readLine()){
            field = line.split(" ");
            if(field.length != 5) break;
            readClueWebDocument(field, ClueDIS[tn], document);

            //if(consistencyCheck(document, Integer.parseInt(field[4]), bw, field[0], tn)) break;
            //if(doc>10000) break;

            fetchHashMap(bufferMap, localStatsDIS[tn]);

            if(isBigram)
                bufferedIndex(      document, field, bufferMap , noDuplicateSet, twoTerms, tn);
            else
                singleBufferedIndex(document, field, bufferMap , noDuplicateSet, twoTerms, tn);

            bufferMap.clear();
            noDuplicateSet.clear();
            doc++;
        }
        if(isBigram)
            sampledSelection(tn, twoTerms, true);
        else{
            singleFlush(singleComparator, singleIndex, twoTerms, 1, true, tn);
            singleFlush(hitComparator, HITIndex, twoTerms, 2, true, tn);
        }


        /*for (int [] aux: buffer[tn]
             ) {
            bw.write(Arrays.toString(aux) + "\n");
        }
        bw.close();*/

        DOS[tn].close();
        ClueDIS[tn].close();
        localStatsDIS[tn].close();
        BR[tn].close();

        System.out.println("D-Bigram Inverted Index Built!");
    }

    public void bufferedIndex(int[] words, String [] field, Int2IntMap localFrequencyMap, LongSet noDuplicateSet, int [] twoTerms, int tn) throws IOException, ClassNotFoundException, InterruptedException {
        /* For each document we take the pairs between documents within a distance. We add each entry to a buffer and
        * compute the BM25 for that specific term-pair*/

        int docLen =  Integer.parseInt(field[4]);
        int score1;
        int score2;
        int movingDistance = distance;
        int localMaxFreq = localFrequencyMap.get(-99);
        long pair;

        for (int wIx = 0; wIx < docLen; wIx++) {
            //System.out.println(movingDistance);
            if(wIx + movingDistance > docLen) movingDistance = (docLen - wIx);
            for (int dIx = wIx+1; dIx <= wIx + movingDistance; dIx++) {

                twoTerms[0] = words[wIx] ;
                twoTerms[1] = words[dIx] ;
                //System.out.println(Arrays.toString(twoTerms));
                Arrays.sort(twoTerms);
                pair = getPair(twoTerms[0], twoTerms[1]);
                if(noDuplicateSet.add(pair) & smallFS.contains(pair)/*bigFS.contains(pair)*/){
                    if(smallFS.contains(pair)){
                        if(pointers[tn] == buffer[tn].length){
                            sampledSelection(tn, twoTerms, false);
                            pointers[tn] = keepPointers[tn];
                        }
                        incrementPostingList(tn, twoTerms, pair);


                        score1 = getBM25(globalStats, docLen, localFrequencyMap.get(twoTerms[0]), localMaxFreq , termFreqMap.get(twoTerms[0]));
                        score2 = getBM25(globalStats, docLen, localFrequencyMap.get(twoTerms[1]), localMaxFreq , termFreqMap.get(twoTerms[1]));

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

    private void writeEntry(String pre, int i, int tn) throws IOException {
        DOS[tn].writeInt(buffer[tn][i][0]);

        if (pre == singleIndex)
            DOS[tn].writeInt(buffer[tn][i][1]);
        else
            DOS[tn].writeInt(buffer[tn][i][2]);

        DOS[tn].writeInt(buffer[tn][i][3]);

    }

    private void singleFlush(Comparator c, String pre, int [] twoTerms, int entry, boolean end, int tn) throws IOException {
        System.out.println("Flushing " + pre);
        int threshold = getThreshold(entry, tn);
        DOS[tn]  = getDOStream(pre+rawI2+dump.getAndAdd(1));
        Arrays.sort(buffer[tn], 0 , pointers[tn], c);
        getThreshold(1,tn);
        int currentTerm = -1;
        int counter = 0;
        for (int i = 0; i < pointers[tn]; i++) {
            writeEntry(pre, i , tn);
            /*if(buffer[tn][i][0] != currentTerm){
                currentTerm = buffer[tn][i][0];
                counter = 0;
            }
            if(counter<500) {
                if (counter < 50){
                    writeEntry(pre, i, tn);
                    counter++;
                } else if (buffer[tn][i][entry] > threshold){
                    writeEntry(pre, i, tn);
                    counter++;
                } else uniIncrementDumpCounter(pre, tn, twoTerms, buffer[tn][i][0]);
            }else uniIncrementDumpCounter(pre, tn, twoTerms, buffer[tn][i][0]); */
        }
        if(end) {
            if (pre == singleIndex)
                serialize(uniMap, unigramDumpMap);
            else
                serialize(hitMap, hitDumpMap);
        }
        DOS[tn].close();
    }

    public void singleBufferedIndex(int[] words, String [] field, Int2IntMap localFrequencyMap, LongSet noDuplicateSet, int [] twoTerms, int tn) throws IOException, ClassNotFoundException, InterruptedException {
        //System.out.println(pointers[tn]);
        for (int wIx = 0; wIx < Integer.parseInt(field[4]); wIx++) {
            if(noDuplicateSet.add(words[wIx]) & uniTerms.contains(words[wIx])) {
                buffer[tn][pointers[tn]][0] = words[wIx];
                buffer[tn][pointers[tn]][1] = getBM25(globalStats, Integer.parseInt(field[4]), localFrequencyMap.get(words[wIx]), localFrequencyMap.get(-99), termFreqMap.get(words[wIx]));
                buffer[tn][pointers[tn]][2] = HITS[Integer.parseInt(field[1])];
                buffer[tn][pointers[tn]][3] = Integer.parseInt(field[1]);
                uniIncrementPostingList(tn, twoTerms, words[wIx]);
                uniIncrementPostingList(tn, twoTerms, words[wIx]);
                pointers[tn]++;

                if (pointers[tn] == buffer[tn].length) {
                    singleFlush(singleComparator, singleIndex, twoTerms, 1, false, tn);
                    singleFlush(hitComparator, HITIndex, twoTerms, 2, false, tn);
                    pointers[tn]=0;
                }
            }
        }
    }

    private synchronized void uniIncrementDumpCounter(String pre, int tn, int [] t, int term){
        if (pre == singleIndex) {
            t = getTerms(uniMap.get(term));
            uniMap.put(term, getPair(t[0], t[1]++));
        }else {
            t = getTerms(hitMap.get(term));
            hitMap.put(term, getPair(t[0], t[1]));
        }
    }

    private synchronized void uniIncrementPostingList(int tn, int [] t, int term){
        t = getTerms(uniMap.get(term));
        uniMap.put(term,getPair(t[0]++, t[1]));
        t = getTerms(hitMap.get(term));
        hitMap.put(term,getPair(t[0]++, t[1]));
    }



    //hack of the structure. This justifies the previous code. I can use the same function to do this.
    private synchronized void incrementPostingList(int tn, int [] t, long pair){
        t = getTerms(dBiMap.get(pair));
        dBiMap.put(pair,getPair(t[0]++, t[1]));
    }

    private synchronized void incrementDumpCounter(int tn, int [] t, long pair){
        t = getTerms(dBiMap.get(pair));
        dBiMap.put(pair,getPair(t[0], t[1]++));
    }



    /*private synchronized void incrementMap(long pair){
        dMap.addTo(pair, 1);
    }*/


    private void sampledSelection(int tn, int [] twoTerms, boolean end) throws IOException{
        //System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now = System.currentTimeMillis();

        if(gThreshold==0)
            gThreshold = getThreshold(2, tn);
        else
            gThreshold = (getThreshold(2,tn)+gThreshold)/2;

        System.out.println("This is the threshold: " + gThreshold);

        int counter = 0;
        int current=-1;

        for (int k = keepPointers[tn]; k < buffer[tn].length; k++) {

            if(buffer[tn][k][0]!= current){
                current = buffer[tn][k][0];
                counter = 0;
            }
            if(counter<1000) {
                if (counter < 100){
                    buffer[tn][keepPointers[tn]++] = buffer[tn][k];
                }else if (buffer[tn][k][2] > gThreshold){
                    buffer[tn][keepPointers[tn]++] = buffer[tn][k];
                }else incrementDumpCounter(tn, twoTerms, getPair(buffer[tn][k][0], buffer[tn][k][1]));
            }else incrementDumpCounter(tn, twoTerms, getPair(buffer[tn][k][0], buffer[tn][k][1]));
        }

        if(keepPointers[tn] > (buffer[tn].length/100)*90 | end) flushBuffer(tn, end);

        maxBM25 = 0;
        //System.out.println("Dump size "+ dump +" " +dMap.size());
        //System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    /*if (buffer[tn][k][2] > gThreshold) {
                buffer[tn][keepPointers[tn]++] = buffer[tn][k];
            }else
                incrementDumpCounter(tn, twoTerms, getPair(buffer[tn][k][0], buffer[tn][k][1]));
                //dMap[tn].addTo(getPair(buffer[tn][k][0], buffer[tn][k][1]),1);
                //incrementMap(getPair(buffer[tn][k][0], buffer[tn][k][1]));*/

    private void flushBuffer(int tn, boolean end) throws IOException {
        System.out.print("Flushing Buffer...\t");
        if (isBigram)
            java.util.Arrays.sort(buffer[tn], 0, keepPointers[tn], bigramBufferComparator); //testComparator); //
        else
            java.util.Arrays.sort(buffer[tn], 0, keepPointers[tn], unigramBufferComparator);

        for (int k = 0; k < keepPointers[tn]; k++) {
            for (int elem : buffer[tn][k])
                DOS[tn].writeInt(elem);
        }

        System.out.print("Done.");
        //System.out.println("Processed docs: " + doc + "Sampled Natural Selection:" + (System.currentTimeMillis() - now) + "ms.\tThreshold: " + threshold +"\t MaxBM25: " + maxBM25);
        DOS[tn].close();
        if(end)
            serialize(dBiMap, dBigramDumpMap);
        else
            DOS[tn]  = getDOStream(prefix+rawI2+dump.getAndAdd(1));
        keepPointers[tn] = 0;

    }

    private int getThreshold(int entry, int tn){
        int rnd;
        //int[] sample = new int[(int) ((buffer[tn].length)*0.002)];

        for(int k = 0; k<sample[tn].length; k++) {
            rnd = ThreadLocalRandom.current().nextInt(keepPointers[tn], buffer[tn].length-1);
            sample[tn][k] = buffer[tn][rnd][entry];
        }

        java.util.Arrays.sort(sample[tn]);

        return sample[tn][(int)(sample[tn].length*0.8)];
    }
}






