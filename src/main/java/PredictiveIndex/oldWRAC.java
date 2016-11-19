package PredictiveIndex;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.zip.GZIPInputStream;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import me.lemire.integercompression.differential.IntegratedByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.differential.IntegratedVariableByte;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static PredictiveIndex.VariableByteCode.encodeInterpolate;
import static PredictiveIndex.utilsClass.arrayToHashMap;
import static PredictiveIndex.utilsClass.getOIStream;
import static PredictiveIndex.utilsClass.getPair;
import static java.util.Arrays.asList;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.Hash;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;


public class oldWRAC{
    /*

    static double deserializeTime = 0;
    static final int min = 50;
    static final int max = 500;
    static int removed =0;
    private int wrac;
    private Lock lock;
    private static int records = 0;
    private int protectList = 0;
    private int lists = 0;
    PrintWriter writer = new PrintWriter(path+"/file.csv", "UTF-8");


    protected void readClueWeb(String data) throws IOException, ClassNotFoundException, InterruptedException {
        start = System.currentTimeMillis();
        ObjectInputStream OIStream = getOIStream(fIndexPath + "/forwardIndexMetadata" , true);
        DataInputStream stream = new DataInputStream( new BufferedInputStream( new FileInputStream("/home/aalto/dio/compressedIndex")));
        BufferedReader br = new BufferedReader(new FileReader(data));
        String[] line = br.readLine().split(" ");
        byte [] rawDoc;
        int docID;
        int b2read;
        int docLen;
        int [] document;
        while(line[0] != null & checkProgress(doc, totNumDocs, 500000, start, 2)){

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

    public void getCollectionMetadata(String data) throws IOException {
        doc = 0;
        int collection = 0;
        start = System.currentTimeMillis();
        for (File file : new File(data).listFiles()){
            this.forwardIndexFile = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fIndexPath + "/forwardIndex" + doc + ".dat", true)));
            System.out.println("Now processing file: " + file);
            GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(file));
            DataInputStream inStream = new DataInputStream(gzInputStream);
            WarcRecord thisWarcRecord;
            while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
                if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                    String [] recordData = thisWarcRecord.getCleanRecord2();
                    this.processWARCRecord(recordData, recordData[recordData.length-1]);
                    doc++;
                }
                if (doc % 1000 == 0) System.out.println(doc);
            }
            collection++;
            inStream.close();
            this.forwardIndexFile.close();
            System.out.println("New Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
            //System.out.println(saved);
            //if(collection ==5) break;
        }
        this.savePSMetadata();
    }



    public void processWARCRecord(String[] words, String title) throws IOException this function process the single wrac files

        int docID = this.stats[0];
        this.docsMap.putIfAbsent(title, docID);
        int [] intWords = new int[words.length+1];
        intWords[0] = this.stats[0];                                        //frist element is the doc title
        HashMap<Integer, Integer> position = new HashMap<>();
        ArrayList<Integer> singeDocStats = new ArrayList<>();
        int pos;
        int uniqueWords = 0;

        int termID;
        for (int k = 0; k<words.length-1; k++) {
            termID = putWordIfAbstent(words[k]);
            intWords[k+1] = termID;
            //We use and auxiliary hashmap to store the frequency and then we convert it to an array

            if (position.putIfAbsent(termID, 1) == null){
                this.freqTermDoc.merge(termID, 1, Integer::sum);
            }else{
                position.merge(termID, 1, Integer::sum);
            }
        }
        this.forwardIndexFile.writeObject(intWords);
        //this.forwardIndexFile.writeObject(hashMapToArray(position));
        this.stats[0]++;
        this.stats[1] += words.length;
    }
    This function checks if the term is in our term-termID map. If a term is present it return the termID, if not
    * adds a new entry to the hashmap. The termID is the number of unique words that we have encountered up to that
    * moment.
    *
    * OBSOLETE: WE HAVE A FIXED DICTIONARY NOW
    *
    *
    public int putWordIfAbstent(String word){
        int termID;
        try{
            termID = getWId(word);
        }catch(NullPointerException e){               // add new document in our dictionary
            termID = stats[2];
            this.stats[2]++;
            this.termsMap.put(word, termID);
            this.freqTermDoc.put(termID, 0);
        }
        return termID;
    }



    // *****************************************************************************************
    // 2ND PHASE - BUILD INVERTED INDEX
    // *****************************************************************************************

    public void buildIndex() throws IOException, ClassNotFoundException {
        To build the inverted index we retrive the collection of document that have been serialized in a list of
        * array strings. For each of the element of this list, which is a document, we call buffered index which process
        * its content and give add it to the buffered index (temporary has map that will be dumped in the array)

        doc = 0;
        start = System.currentTimeMillis();
        for (File file : new File(fIndexPath).listFiles()) {
            wracNowProcessing = file.getName();
            System.out.println(wrac + ")Processing WRAC: " + wracNowProcessing);
            this.readFromBinaryFile(file.getAbsolutePath());
            wrac++;
        }
        //we need to flush out to memory the partial buffer even if is not full
        this.buffer = Arrays.copyOfRange(this.buffer, 0, pointer);
        //this.naturalSelection();
        sampledNaturalSelection();
        System.out.println("Total Processing Time:" + (System.currentTimeMillis() - start) * 1000 + " s.");
        this.invertedIndexFile.close();
        writer.close();
        System.out.print(records);
    }

    public int getDId(String word) {
        return this.docsMap.get(word);
    }

    private int getWId(String word) {
        return this.termsMap.get(word);
    }

    private static double boundedKeep(int diff) {we want to preserve the short lists and cut the long ones. The bounds are min and max. We have four cases
        * 20% > max  ->    keep max
        * 20% > min   ->    keep 20%
        * diff > min  ->    keep min
        * diff < min  ->    keep diff

        //return (diff * 0.2 > max) ? max : (diff * 0.2 > min) ? diff * 0.2 : (diff > min) ? min : diff;
        return (diff < min) ?  diff : (diff * 0.2 < min ) ? min : (diff * 0.2 < max) ? diff * 0.2 : max;
        //return diff*0.2;
    }

    public void naturalSelection() throws IOException, ClassNotFoundException {We want to keep just the 20% of each posting list. If the remainig list is bigger or smaller of min and max
        * we protect or truncate it.
        * WE CAN IMPROVE THIS FUNCTION BY USING AN HASMAP THAT SAVES FOR EACH PAIR HOW MANY ENTRIES IT HAS. I DON'T KNOW
        * HOW MUCH SPACE IT WOULD REQUIRE. IN THIS WAY WE COULD SCAN THE ARRAY JUST ONE TIME

        removed=0;
        int diff = 0;
        double aLength = 0;
        lists = 0;
        protectList = 0;
        int maxLength = 0;
        int [] toPlot = new int [50000];
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        this.sortBuffer();
        now = System.currentTimeMillis();
        int [] nowPosting = this.buffer[0];
        int startPointer = 0;
        int keep;
        HashMap<Integer, Integer> stat = new HashMap<>();
        for (int k = 0; k < this.buffer.length; k++) {
            if (this.buffer[k][0] != nowPosting[0] | this.buffer[k][1] != nowPosting[1] | k == this.buffer.length-1) {
                //System.out.println((k - startPointer)+"-"+nowPosting);
                lists++;
                diff = k-startPointer;
                aLength += diff;
                toPlot[k-startPointer]++;
                keep = (int) boundedKeep(diff);
                //if ((25840 < diff) & (diff < 25870)) System.out.println(termsMap.inverse().get(this.buffer[k][0]) + "-" + termsMap.inverse().get(this.buffer[k][1])+"-"+diff);
                //System.out.print(keep);
                //this.invertedIndexFile.writeObject(Arrays.copyOfRange(this.buffer, startPointer, startPointer+keep));
        for (int k2 = startPointer; k2 < startPointer + keep; k2++){
                    //this.invertedIndexFile.writeObject(this.buffer[k2]);
        for(int elem : this.buffer[k2]){
                        this.invertedIndexFile.writeInt(elem);
                    }
                    records++;
                }
                removed += ((k-startPointer) - keep);
                startPointer = k;
                nowPosting = this.buffer[startPointer];
            }
        }
        writer.print(wracNowProcessing+",");
        for(int k =0; k<toPlot.length ; k++) writer.print((toPlot[k]*k)+",");
        writer.println();
        System.out.println("Lists: " + lists + " - Protected Lists: " + protectList + " - Percentage: " + protectList*1.0/lists + " - Average Lenght: " + aLength/lists + " Max" +maxLength);
        System.out.println("Entries removed: " + (removed*1.0/(1000000)) +"M");
        System.out.println("Flush Time:" + (System.currentTimeMillis() - now) + "ms");
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
    }

    public void sortBuffer() {Sorting  function.
        * WE CAN IMPROVE THIS ASSINING AN UNIQUE IDENTIFIER TO EACH PAIR OF TERMS

        now = System.currentTimeMillis();
        java.util.Arrays.parallelSort(this.buffer, new Comparator<int[]>() {
            @Override
            public int compare(int[] pairID1, int[] pairID2) {
                //if we have the same doc ids sort them based on the bm25
                if (pairID1[0] == pairID2[0]) {
                    if(pairID1[1] == pairID2[1]){
                        return Integer.compare(pairID1[2], pairID2[2]) * -1;
                    }
                    else return Integer.compare(pairID1[1], pairID2[1]);
                } else return Integer.compare(pairID1[0], pairID2[0]);
            }
        });
        System.out.println("Sorting Time: " + (System.currentTimeMillis() - now) + "ms");
    }

    //#####################################################################################################################


    public static int[][][] getQualityModel() throws IOException, ClassNotFoundException {
        int hit = 0;
        int maxBM25 = 0;
        int minBM25 = 0;
        int maxLength = 0;
        int[] lRanges = computelRanges(1.1);
        int[] rRanges = computerRanges(1.4);
        int[][][] qualityModel = new int[lRanges.length][rRanges.length][2];
        //BufferedReader br = new BufferedReader(new FileReader("readIndexInfo"));
        DataInputStream inStream = new DataInputStream(new BufferedInputStream(new FileInputStream(dPath + "/InvertedIndex.dat")));
        ObjectInputStream obInStream = getOIStream(metadata + "fastQueryTrace", true);
        System.out.println("Fast Query Trace fetched!\nProcessing Inverted Index...");
        HashMap<Long, HashMap<Integer, Integer>> fastQueryTrace = (HashMap<Long, HashMap<Integer, Integer>>) obInStream.readObject(); //conversion seems to work
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        auxPostingList.add(3);
        String[] line;
        long numberOfPostingLists = 0;
        byte[] byteStream;
        int[] postingList;
        long pair;
        int lenBucket;
        int rankBucket = 0;
        HashMap<Integer, Integer> aggregatedTopK;
        int increment;
        int[] posting;
        int[] currentPair = new int[]{-1, -1};
        while (true) {
            posting = getEntry(inStream);
            if (posting[0] == -1) break;
            if (posting[0] != currentPair[0] | posting[1] != currentPair[1]) {

        if(maxBM25<posting[2]) maxBM25 = posting[2];
                else if(minBM25 >posting[2]) minBM25= posting[2];
                if(auxPostingList.size()>maxLength) maxLength = auxPostingList.size();

                if (fastQueryTrace.get(getPair(currentPair[0], currentPair[1])) != null) {
                    //System.out.println(fastQueryTrace.get(getPair(currentPair[0], currentPair[1])).size());
                    processPostingList(Ints.toArray(auxPostingList), qualityModel, fastQueryTrace.get(getPair(currentPair[0], currentPair[1])), rRanges, lRanges, hit);
                }
                //System.out.println(fastQueryTrace.keySet().size());

                numberOfPostingLists++;
                auxPostingList.clear();
                currentPair[0] = posting[0];
                currentPair[1] = posting[1];

            }
            auxPostingList.addLast(posting[2]); //BM25
            auxPostingList.addLast(posting[3]); //docid
        }while((line = br.readLine().split(","))[0] != null){ *
            pair = Integer.valueOf(line[0]);
            byteStream = new byte[Integer.valueOf(line[1])];
            inStream.read(byteStream);
            postingList = decodeInterpolate(byteStream);
            lenBucket = getLenBucket(Integer.valueOf(line[2]), lRanges);
            aggregatedTopK = fastQueryTrace.get(pair);
            for (int i = 0; i < postingList.length ; i += 2) {
                increment = aggregatedTopK.get(postingList[i]);
                rankBucket = getRankBucket(rankBucket, postingList[i+1], rRanges);

                //bucket hit by this posting
                qualityModel[lenBucket][rankBucket][0] += increment ;
                for (int j = 0; j < rankBucket+1 ; j++) {
                    //previous buckets hit by this posting
                    qualityModel[lenBucket][j][1] += increment;
                }
            }

        }
        System.out.println("Posting List: " + numberOfPostingLists);
        System.out.println("max: " + maxBM25 + ". min: " + minBM25 + ". len: " + maxLength);
        ObjectOutputStream oStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(metadata + "qualityModel.bin")));
        oStream.writeObject(qualityModel);
        return qualityModel;

        Once the inverted index is sorted we compress it using variable byte encoding.
     * In this way we achive a compression of x3 and achiving a computation speed up of x10
     *
     * OPEN ISSUES:
     * - THIS IS NOT GOOD

        public static int[][] compressInvertedIndex () throws IOException {
            int p = 0;
            int maxLength = 0;
            PrintWriter totalIndexStats = new PrintWriter(path + "/totalStatsLength.csv", "UTF-8");
            int[] lengthStats = new int[5000000];
            long now = System.currentTimeMillis();
            DataInputStream dataStream = new DataInputStream(new BufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.dat")));
            DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream((new FileOutputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/compressedSortedInvertedIndex.dat"))));
            int[][] invertedIndex = new int[528184109][];
            LinkedList<Integer> auxPostingList = new LinkedList<>();
            getEntry(dataStream);
            int[] nowPair = new int[]{-1, -1};
            int[] aux;
            byte[] byteSteam;
            while (true) {
                aux = getEntry(dataStream);
                if (aux[0] != nowPair[0] | aux[1] != nowPair[1]) {
                    checkProgress(p);
                    if (auxPostingList.size() > maxLength) maxLength = auxPostingList.size();
                    //lengthStats[auxPostingList.size()]++;
                    //invertedIndex[p] = Ints.toArray(auxPostingList);
                    byteSteam = encodeInterpolate(auxPostingList);
                    //need to add the docInfo file for the compressed inverted index
                    for (byte b : byteSteam) {
                        outStream.writeByte(b);
                    }
                    p++;
                    auxPostingList.clear();
                    nowPair[0] = aux[0];
                    nowPair[1] = aux[1];
                    auxPostingList.addLast(aux[3]);

                } else if (aux[0] == -1) {
                    break;
                } else {
                    auxPostingList.addLast(aux[2]);
                    auxPostingList.addLast(aux[3]);
                }
            }
            for (int k = 0; k < lengthStats.length; k++) totalIndexStats.print((lengthStats[k] * k) + ",");
            System.out.println(maxLength);
            System.exit(1);
            return invertedIndex;
        }


    }

    public void readFromBinaryFile (String filename) {For each document we retrieve the document itself, its title and statistics

        File file = new File(filename);

        if (file.exists()) {
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(filename)));
                while (true) {
                    int [] intWords = (int[])  ois.readObject();    //title + document
                    //HashMap<Integer,Integer> docTermsStats = arrayToHashMap((int[])  ois.readObject()); //docstats
                    //this.bufferedIndex(intWords, intWords[0], docTermsStats);
                    this.bufferedIndex(intWords, intWords[0], (Int2IntMap) arrayToHashMap((int[])  ois.readObject()));
                    doc++;
                    //System.out.println(wordsCount/doc);
                }
            }catch (EOFException e) {
            }catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    if (ois != null) ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    */
}

