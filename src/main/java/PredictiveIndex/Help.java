package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

import static PredictiveIndex.utilsClass.getOOStream;

/**
 * Created by aalto on 7/20/16.
 */
public class Help {

    public static void main(String [] args){

    }
}



/*
        IMPORTANT THIS IS THE EFFICINET WAY TO READ THE INVERTED INDEX
         while((line = br.readLine().split(","))[0] != null){ //**
            pair = Integer.valueOf(line[0]);
            byteStream = new byte[Integer.valueOf(line[1])];
            inStream.read(byteStream);
            postingList = decodeInterpolate(byteStream);
            lenBucket = getLenBucket(Integer.valueOf(line[2]), lenRanges);
            aggregatedTopK = fastQueryTrace.get(pair);
            for (int i = 0; i < postingList.length ; i += 2) {
                increment = aggregatedTopK.get(postingList[i]);
                rankBucket = getRankBucket(rankBucket, postingList[i+1], rankRanges);

                //bucket hit by this posting
                qualityModel[lenBucket][rankBucket][0] += increment ;
                for (int j = 0; j < rankBucket+1 ; j++) {
                    //previous buckets hit by this posting
                    qualityModel[lenBucket][j][1] += increment;
                }
            }

        }*/
      /*

          if((rawDoc[i] & 0xff)  >= 128){
                    //System.out.println(rawDoc[i]);
                    flippedContinuationBit++;
                }else{
                    continuationBit++;
                }

                    //System.out.println("#Byte>=128: " + (flippedContinuationBit) + ". Expected: " + (docLen) + ". #Bytes<128: " + continuationBit);
                    //document = decodeRawDoc(rawDoc, docLen);

       switch(round){
                case 0:
                    //collecting metadata

                    storeMetadata(document);
                    break;
                case 1:
                    //building invertedindex

                    //bufferedIndex(document, title, arrayToHashMap((int[])  ois.readObject()));
                    break;
            }
            line = br.readLine().split(" ");

      THIS CODE IS A BENCHMARK FOR SORTING ALGORITHMS OF RANDOM ARRAYS

      public static void superMagic(){
        float[][] kickerNumbers = new float[50000000][4];
        for (int i = 0; i < kickerNumbers.length; i++) {
            for (int j = 0; j < kickerNumbers[0].length ; j++) {
                kickerNumbers[i][j] = (float) Math.random();
            }
        }
        long now = System.currentTimeMillis();
        java.util.Arrays.parallelSort(kickerNumbers, new Comparator<float[]>() {
            @Override
            public int compare(float[] int1, float[] int2) {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0] == int2[0]) {
                    if(int1[1] == int2[1]){
                            return Float.compare(int1[2], int2[2]) * -1;
                    }else return Float.compare(int1[1], int2[1]);
                } else return Float.compare(int1[0], int2[0]);
            }
        });
        System.out.print(System.currentTimeMillis() - now);
        System.exit(1);
    }

    public static void superMagic2(){
        double[][] kickerNumbers = new double[50000000][2];
        for (int i = 0; i < kickerNumbers.length; i++) {
            for (int j = 0; j < kickerNumbers[0].length ; j++) {
                kickerNumbers[i][j] = Math.random();
            }
        }
        long now = System.currentTimeMillis();
        java.util.Arrays.parallelSort(kickerNumbers, new Comparator<double[]>() {
            @Override
            public int compare(double[] int1, double[] int2) {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0] == int2[0]) {
                    return Double.compare(int1[1], int2[1]) * -1;
                } else return Double.compare(int1[0], int2[0]);
            }
        });
        System.out.print(System.currentTimeMillis() - now);
        System.exit(1);
    }




        THIS CODE COUNTS THE LINES IN A FILE

        public static void readLinez() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/home/aalto/dio/docInfo"));
        long k=0;
        String line = br.readLine();
        while(line != null){
          line = br.readLine();
            k++;

        }
        System.out.print(k);
        System.exit(1);
    }


      public static HashMap<Integer, LinkedList<int[]>> reformatQueryTrace(String file) throws IOException {
        //we read the query trace and covert it to a hasmap-integer[][] map1
        // I need the term-termID and doc-docID map
        //this is the best way because immediatly we would know statistics using length
        //we scan the inverted index once and evry time we move in parallel on this structure to get what we have
        //stopping rule would be an array make 0


        HashMap<Integer, LinkedList<int[]>> queries = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        LinkedList<int []> auxLList;
        int [] auxTopK;
        int [] keys;
        while((line = br.readLine()) != null){
            auxTopK = getQueryTopkDocIDs(line);
            keys = getQueryBigrams(line);
            for(Integer key : keys) {
                auxLList = queries.get(key);
                if (auxLList == null) {
                    auxLList = new LinkedList<> ();
                    queries.put(key, auxLList);
                }
                auxLList.add(auxTopK);
            }

        }
        ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("ciao", true)));
        out.writeObject(queries);
        return queries;
    }

    public int[][] qualityModel() throws IOException, ClassNotFoundException {
        HashMap<Integer,LinkedList<int[]>> queries = fetchQueries();
        int [][] qualityModel= new int[?][?];
        int [] aux;
        DataInputStream dataStream = new DataInputStream( new BufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.dat")));
        while(true){
            aux = getEntry(dataStream);
            if(aux[0] != nowPair[0] | aux[1] != nowPair[1]){
                if(IIPointer++ % 10000000 == 0){
                    percentage = (long) (IIPointer*100.0)/528184109;
                    System.out.println("Work in progress: " + percentage+ "% completed.");
                    //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
                }
                IIPointer++;
                auxPostingList.clear();
                nowPair[0]= aux[0];
                nowPair[1]= aux[1];
                auxPostingList.addLast(aux[3]);

            }else if(aux[0]==-1){
                break;
            }else{
                auxPostingList.addLast(aux[3]);
            }
        }
    }


      int [][] aux = new int[][] {
                {3L,4L},
                {1L,5L},
                {3L,6L},
                {2L,10L},
                {2L,11L},
                {2L,12L},
                {1L,2L},
                {3L,1L},
                {3L,2L},
                {2L,9L},
                {1L,3L},
                {3L,4L},
                {1L,5L},
                {3L,6L},
                {2L,1L},
                {2L,1L},
                {2L,12L},
                {1L,1L},
                {1L,2L},
                {3L,7L},
                {3L,8L},
                {2L,9L},
                {1L,3L},
                {1L,2L},
                {1L,2L},
                {3L,7L},
                {3L,8L},
                {2L,9L},
                {2L,44L}
        };
        this.buffer= aux;


        int [][] aux = new int[][] {
                {3,4},
                {1,5},
                {3,6},
                {2,10},
                {2,11},
                {2,12},
                {1,2},
                {3,1},
                {3,2},
                {2,9},
                {1,3},
                {3,4},
                {1,5},
                {3,6},
                {2,1},
                {2,1},
                {2,12},
                {1,1},
                {1,2},
                {3,7},
                {3,8},
                {2,9},
                {1,3},
                {1,2},
                {1,2},
                {3,7},
                {3,8},
                {2,9},
                {2,44}
        };
        this.buffer= aux;*/

/*for(int k = 0; k<aux.length; k++){
            System.out.println(this.buffer[k][0]+" "+this.buffer[k][1]);
        }*/

   /*for(int k = 0; k<aux.length*0.2; k++){
            System.out.println(this.buffer[k][0]+" "+this.buffer[k][1]);
        }*/


/*  public static void main(String [] args) throws IOException {
        String data = "/home/aalto/IdeaProjects/PredictiveIndex/data/test";
        InvertedIndex ps;
        if (Files.exists(Paths.get(tPath+ser))) {
            System.out.println("Deserializing Predictive Inverted...");
            ps = new InvertedIndex((HashMap<String, Integer>) deserialize(tPath+ser),  (HashMap<Integer, Integer>) deserialize(fPath+ser),
                    (HashMap<String, Integer>) deserialize(docMapPath+ser), (int[]) deserialize(sPath+ser));
            System.out.println("Predictive Index Deserialized");
        }else {
            ps = new InvertedIndex();
            ps.getCollectionMetadata(data);
        }
        ps.buildIndex();//
        //ImpactModel.buildImpactModel();
    }
}



    Integer t1 = -1;
        Integer t2 = -1;
        String prefix="";
        StringBuilder toFlush = new StringBuilder();
        for(int [] entry : this.buffer){
            if(entry[0]!=t1 | entry[1]!=t2){
                toFlush.append(prefix+entry[0]+","+entry[1]+","+entry[2]);
                prefix="\n";
            }else toFlush.append(","+entry[2]);
            t1=entry[0];
            t2=entry[1];
        }


        *********************************************************
        *
        *
        * StringBuilder toFlush = new StringBuilder();
        for(int [] entry : this.buffer){
            toFlush.append(entry[0]+","+entry[1]+","+(entry[3]+entry[4])+","+entry[2]+"\n");

        }
 */


    /*public ArrayList deserialize(File file){
        ArrayList<LinkedList> e = null;
        try
        {
            FileInputStream fileIn = new FileInputStream(file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            e = (ArrayList<LinkedList>) in.readObject();
            in.close();
            fileIn.close();
            return e;
        }catch(IOException i)
        {
            i.printStackTrace();
            return null;
        }catch(ClassNotFoundException c)
        {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }

    }

    ############## DUMP DUMP
        public void dumpBuffer(String name) {
        // Save the partial inverted index (a.k.a buffer) to disk

        String prefix;
        StringBuilder toFlush = new StringBuilder();
        for (int[] entry : this.buffer) {
            prefix = "";
            for (int num : entry) {
                toFlush.append(prefix + num);
                prefix = ",";
            }
            toFlush.append("\n");
        }
        File file = new File("dumps/" + name + ".txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dumpBuffer2(String name) {
        // Save the partial inverted index (a.k.a buffer) to disk

        this.sortBuffer();
        StringBuilder toFlush = new StringBuilder();
        for (int[] entry : this.buffer) {
            toFlush.append(entry[0] + "," + entry[1] + "," + (entry[3] + entry[4]) + "," + entry[2] + "\n");
        }
        File file = new File("dumps/" + name + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dumpBuffer3(String name, int limit) {
        // Save the partial inverted index (a.k.a buffer) to disk

        //this.sortBuffer();
        StringBuilder toFlush = new StringBuilder();
        int entry[];
        for (int k = 0; k < limit; k++) {
            entry = this.buffer[k];
            toFlush.append(entry[0] + "," + entry[1] + "," + (entry[3] + entry[4]) + "," + entry[2] + "\n");
        }
        System.out.println(limit);
        File file = new File(dPath + "/" + name + ".txt");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(toFlush);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

    public void naturalSelection() {
        //this function can be used if we want to keep to use the array
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        now = System.currentTimeMillis();
        this.sortBuffer();
        int[] nowPosting = this.buffer[0];
        int shifter = 0;
        int startPointer = 0;
        int keep;
        for (int k = 0; k < this.buffer.length; k++) {
            if (this.buffer[k][0] != nowPosting[0] | this.buffer[k][1] != nowPosting[1] | k == this.buffer.length - 1) {
                keep = (int) boundedKeep(k - startPointer);
                for (int k2 = startPointer; k2 < startPointer + keep; k2++) {
                    this.buffer[k2 - shifter][0] = this.buffer[k2][0];
                    this.buffer[k2 - shifter][1] = this.buffer[k2][1];
                    this.buffer[k2 - shifter][2] = this.buffer[k2][2];
                    this.buffer[k2 - shifter][3] = this.buffer[k2][3];
                    this.buffer[k2 - shifter][4] = this.buffer[k2][4];
                }
                //this.buffer[startPointer + keep - 1 - shifter][3] = 123456789;
                shifter += (k - startPointer) - keep; //we are going to overwrite the elements that we don't want to keep
                startPointer = k;
                nowPosting = this.buffer[startPointer];
            }

        }
    }

    public void map2Buffer(HashMap<Integer, Integer> docFreq, HashSet<String> pairs, int docSize, int docId) {
        //We want to avoid huge hashmaps and for each warc document we flush it in a static data structure

        String pair;
        String[] words;
        Iterator it = pairs.iterator();
        while (it.hasNext()) {
            pair = it.next().toString();
            words = pair.split("-");
            int e1 = getWId(words[0]);
            int e2 = getWId(words[1]);
            //System.out.println(pair);
            this.buffer[pointer][0] = e1;
            this.buffer[pointer][1] = e2;
            this.buffer[pointer][2] = docId;
            this.buffer[pointer][3] = getBM25(e1, docSize, docFreq.get(e1));
            this.buffer[pointer][4] = getBM25(e2, docSize, docFreq.get(e2));
            if (pointer == buffer.length - 1) {
                naturalSelection2();
                //pointer = (this.buffer.length/100)*20; we don't want to reuse it
                pointer = 0;
            } else pointer++;
            it.remove(); // avoids a ConcurrentModificationException
        }
    }*/


/*
* public void bufferedIndex(String[] words, String title) {
        We use a temporaney HashMap to process the single documents present in the WRAC document and the we flush it
        and then we flush it in the buffer
         non calcola le ultime combinazioni


    HashMap<Integer, Integer> docFreq = new HashMap<>();    //auxFreq counts for each term how many times is used into the document
    HashSet<String> auxPair = new HashSet<>();
for (int wIx = 0; wIx < words.length; wIx++) {
        if (wIx < words.length - this.distance) {
        for (int dIx = 1; dIx < this.distance; dIx++) {
        String[] pair2Sort = {words[wIx], words[wIx + dIx]};
        Arrays.sort(pair2Sort);
        String pair = pair2Sort[0] + "-" + pair2Sort[1];
        auxPair.add(pair);
        }
        }
        if (docFreq.putIfAbsent((getWId(words[wIx])), 1) != null) {
        docFreq.merge(getWId(words[wIx]), 1, Integer::sum);    //modified from this.freqTermDoc errore
        }
        }
        this.map2Buffer(docFreq, auxPair, words.length, getDId(title));
        }
*
*
*
* //STORE INFO *******************************************************************************************************

    public void fetchGInfo() throws FileNotFoundException {
        //this methods returns the global statistics about the dataset if already processed.

        Scanner scanner = new Scanner(new File("gstats.txt"), "UTF-8");
        for (int k = 0; scanner.hasNext(); k++) {
            this.stats[k] = Integer.valueOf(scanner.next());
        }
    }

    public void dumpGInfo() throws FileNotFoundException {
        //this methods saves the global statistics about the whole dataset.

        try (FileWriter fw = new FileWriter("gstats.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            for (int k = 0; k < this.stats.length; k++) {
                out.println(this.stats[k]);
            }
        } catch (IOException e) {
            System.out.println("No dump, No party!");
            System.exit(1);
        }
    }

    public void hash2CSV() throws IOException {
        //this function save the map term-freq to disk

        Iterator it = this.freqTermDoc.entrySet().iterator();
        try (FileWriter fw = new FileWriter("termsOccurence.csv", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                String aux = pair.getKey() + "," + pair.getValue();
                out.println(aux);
                it.remove(); // avoids a ConcurrentModificationException
            }
        }
    }

    public void naturalSelection() {
        //csv file
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        this.sortBuffer();
        now = System.currentTimeMillis();
        int[] nowPosting = this.buffer[0];
        int startPointer = 0;
        int keep;
        StringBuilder toFlush = new StringBuilder();
        File file = new File(dPath + "/" + "upto" + doc + ".dat");
        try (FileWriter FW = new FileWriter(file);
                BufferedWriter writer = new BufferedWriter(FW)) {
            for (int k = 0; k < this.buffer.length; k++) {
                if (this.buffer[k][0] != nowPosting[0] | this.buffer[k][1] != nowPosting[1] | k == this.buffer.length - 1) {
                    keep = (int) boundedKeep(k - startPointer);
                    for (int k2 = startPointer; k2 < startPointer + keep; k2++) {
                        writer.append(this.buffer[k2][0] + "," + this.buffer[k2][1] + "," + (this.buffer[k2][3] + this.buffer[k2][4]) + "," + this.buffer[k2][2] + "\n");
                    }
                    startPointer = k;
                    nowPosting = this.buffer[startPointer];
                }
            }writer.close();
            FW.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Flush Time:" + (System.currentTimeMillis() - now) + "ms");
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start - deserializeTime)) * 1000 + " doc/s");
    }

    public void buildIndex() throws IOException {
         To build the inverted index we retrive the collection of document that have been serialized in a list of
         array strings. For each of the element of this list, which is a document, we call buffered index which process
         its content and give add it to the buffered index (temporary has map that will be dumped in the array)

doc = 0;
        start = System.currentTimeMillis();
        this.buffer = new int[10000000][4];
        for (File file : new File(fIndexPath).listFiles()) {
        now = System.currentTimeMillis();
        ArrayList<LinkedList<Object>> fowardIndex = (ArrayList<LinkedList<Object>>) deserialize(file.getAbsolutePath());
        System.out.println("Processing WRAC: " + file.getName());
        LinkedList<Object> aux;
        Iterator it = fowardIndex.iterator();
        deserializeTime += (System.currentTimeMillis() - now);
        while (it.hasNext()) {
        aux = (LinkedList) it.next();
        this.bufferedIndex((String[]) aux.getLast(), (String) aux.getFirst());
        doc++;
        }

        public void naturalSelection2() {
        //csv file
        System.out.println("TIME TO CLEAN. Processed docs: " + doc);
        this.sortBuffer();
        now = System.currentTimeMillis();
        int[] nowPosting = this.buffer[0];
        int startPointer = 0;
        int keep;
        StringBuilder toFlush = new StringBuilder();
        File file = new File(dPath + "/" + "upto" + doc + ".txt");
        try (FileWriter FW = new FileWriter(file);
             BufferedWriter writer = new BufferedWriter(FW)) {
            for (int k = 0; k < this.buffer.length; k++) {
                if (this.buffer[k][0] != nowPosting[0] | this.buffer[k][1] != nowPosting[1] | k == this.buffer.length - 1) {
                    keep = (int) boundedKeep(k - startPointer);
                    for (int k2 = startPointer; k2 < startPointer + keep; k2++) {
                        writer.append(this.buffer[k2][0] + "," + this.buffer[k2][1] + "," + (this.buffer[k2][3] + this.buffer[k2][4]) + "," + this.buffer[k2][2] + "\n");
                    }
                    startPointer = k;
                    nowPosting = this.buffer[startPointer];
                }
            }writer.close();
            FW.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Flush Time:" + (System.currentTimeMillis() - now) + "ms");
        System.out.println("Processing Time:" + (doc / (System.currentTimeMillis() - start - deserializeTime)) * 1000 + " doc/s");
    }
        }
        }

        public void getCollectionMetadata(String data) throws IOException {
        doc = 0;
        start = System.currentTimeMillis();
        for (File file : new File(data).listFiles()) {
            ArrayList<LinkedList> forwardIndex = new ArrayList<LinkedList>();
            System.out.println("Now processing file: " + file);
            GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(file));
            DataInputStream inStream = new DataInputStream(gzInputStream);
            WarcRecord thisWarcRecord;
            while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
                if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                    //processWARCRecord((String []) recordData.getLast());
                    LinkedList<Object> recordData = thisWarcRecord.getCleanRecord();
                    this.processWARCRecord((String[]) recordData.getLast(), (String) recordData.getFirst());
                    forwardIndex.add(recordData);
                    doc++;
                }
                if (doc % 1000 == 0) System.out.println(doc);
            }
            inStream.close();
            serialize(forwardIndex, fIndexPath + "/" + file.getName());
            System.out.println("Old Processing Time:" + (doc / (System.currentTimeMillis() - start)) * 1000 + " doc/s");
        }
        this.savePSMetadata();


    }
* */


/* class ThreadDemo extends Thread {
        private Thread t;
        private String threadName;
        private int tCounter;
        InvertedIndex  PI;

        ThreadDemo( String name,  InvertedIndex pred) throws IOException {
            threadName = name;
            //tCounter =  new FastBufferedOutputStream(new ObjectOutputStream( new FileOutputStream(dPath + "/InvertedIndex"+threadName+".dat", true)));
            tCounter = pointer;
            PI = pred;
        }
        public void run() {
            try {
                //System.out.println(threadName + PI + t);
                PI.buildIndex();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Thread " +  threadName + " exiting.");
        }

        public void start()
        {
            //System.out.println("Starting " +  threadName );
            if (t == null)
            {
                t = new Thread (this, threadName);
                t.start ();
            }
        }

    }


    public void threads() throws IOException {
        ThreadDemo T1 = new ThreadDemo( "thread1", this );
        T1.start();
        ThreadDemo T2 = new ThreadDemo( "thread2", this );
        T2.start();
        ThreadDemo T3 = new ThreadDemo( "thread3", this );
        T3.start();
        ThreadDemo T4 = new ThreadDemo( "thread4", this );
        T4.start();

    }*/

 /*public void sortBuffer() {
        /*Sorting  function.
        * WE CAN IMPROVE THIS ASSINING AN UNIQUE IDENTIFIER TO EACH PAIR OF TERMS*/

    /*    now = System.currentTimeMillis();
        java.util.Arrays.sort(this.buffer, new Comparator<int[]>() {
            @Override
            public int compare(int[] int1, int[] int2) {
                //if we have the same doc ids sort them based on the bm25
                if (int1[0] == int2[0]) {
                    if (int1[1] == int2[1]) {
                        return Integer.compare(int1[2], int2[2]) * -1;
                    } else return Integer.compare(int1[1], int2[1]);
                } else return Integer.compare(int1[0], int2[0]);
            }
        });
        System.out.println("Sorting Time: " + (System.currentTimeMillis() - now) + "ms");
    }

    public void sortBuffer() {


    now = System.currentTimeMillis();

    it.unimi.dsi.fastutil.longs.LongBigArrays.quickSort(this.buffer);
    System.out.println("Sorting Time: " + (System.currentTimeMillis() - now) + "ms");
}

        //globalFreqMap = new Int2IntOpenHashMap();
        //DB db = DBMaker.fileDB("termFreq.db").fileMmapEnable().cleanerHackEnable().fileMmapPreclearDisable().make();
        //globalFreqMap = db.hashMap("map", Serializer.INTEGER_DELTA, Serializer.INTEGER_DELTA).createOrOpen();
    */

    /*this function process the single wrac files*/
    /*private void storeMetadata(int [] words Int2IntMap position) throws IOException {
        int multipleOccurece = 0;
        for (int k = 0; k<words.length; k++) {
            if (position.putIfAbsent(words[k], 1) == null){
                if(this.globalFreqMap.putIfAbsent(words[k], 1)!=null) {
                    this.globalFreqMap.merge(words[k], 1, Integer::sum);
                    this.globalStats[2]++;
                }
            }else{
                if(position.merge(words[k], 1, Integer::sum)==2) multipleOccurece++;
            }
        }
        if(globalFreqMap.size()%100000 ==0 ) System.out.println(globalFreqMap.size());
        System.out.print("Removed " + docID + "\t" + (position.keySet().size()-multipleOccurece));
        this.forwardIndexFile.writeObject(hashMapToArray(position, multipleOccurece));
        this.globalStats[0]++;
        this.globalStats[1]+= words.length;
    }*/


    /*public static void binaryMassiveSort(String input, String output, int recordsNumber) throws IOException {
        now = System.currentTimeMillis();
        int p=0;
        DataInputStream DIStream = new DataInputStream(new BufferedInputStream(new FileInputStream(input)));
        System.out.print("Allocating space...\t");
        partialNow = System.currentTimeMillis();
        bucket = new long[250000000][2];
        System.out.println("done: " + (System.currentTimeMillis() - partialNow) / 1000 + "s");
        DataOutputStream DOStream;
        for (int k = 1; true; k++) {
            DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output + k)));
            System.out.print("Loading data...");
            partialNow = System.currentTimeMillis();
            try {
                for (p = 0; p < bucket.length; p++) {
                    bucket[p][0] = DIStream.readLong();
                    bucket[p][1] = DIStream.readLong();
                }
            } catch (EOFException e) {
                routine(DOStream, p+1);
                break;
            }
            routine(DOStream, bucket.length);
        }
        System.out.print((System.currentTimeMillis() - now)/60000+"min");
    }

    static void routine(DataOutputStream DOStream, int p) throws IOException {
        System.out.println("done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
        System.out.print("Sorting...\t");
        partialNow = System.currentTimeMillis();
        java.util.Arrays.parallelSort(bucket, 0, p, comp);
        System.out.println("done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
        System.out.print("Writing on disk...\t");
        partialNow = System.currentTimeMillis();
        for(int x =0; x<p; x++) for (long entry: bucket[x]) DOStream.writeLong(entry);
        DOStream.close();
        System.out.println("done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
    }

    /*public static void readFiles(File folder, String output) throws IOException {
        long [][][] bucketsA;
        long [][][] bucketsB;
        File [] files = folder.listFiles();
        DataInputStream [] DIStreams = new DataInputStream[files.length];
        DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output)));
        int section =  250000000/(files.length+2);
        bucketsA = new long[files.length][section][2];
        for(int k =0; k< files.length; k++){
            DIStreams[k] =  new DataInputStream(new BufferedInputStream(new FileInputStream(files[k])));
        }
        for(int y = 0; y< DIStreams.length; y++){
            for(int z = 0; z < section; z++){
                bucketsA[y][z][0] = DIStreams[y].readLong() ;
                bucketsA[y][z][1] = DIStreams[y].readLong() ;
            }
        }
        for (int x = files.length/2; bucketsA.length>2 ; x /=2) {
            System.out.println("Merging " + x);
            bucketsB = new long[x][section][2];
            for (int i = 0; i < bucketsB.length ; i++) {
                System.out.println("Merging " + x +"-"+i);
                bucketsB[i] = mergeSortedMatrix(bucketsA[(i*2)], bucketsA[(i*2)+1]);
                bucketsA[(i*2)]   = null;
                bucketsA[(i*2)+1] = null;
            }
            x/=2;
            if(bucketsB.length==2){
                writeMergeSortedMatrix(bucketsB[0], bucketsB[1], DOStream);
                break;
            }
            bucketsA = new long[x][section][2];
            for (int i = 0; i < bucketsB.length ; i++) {
                System.out.println("Merging:" + x +"-"+i);
                bucketsA[i] = mergeSortedMatrix(bucketsB[(i*2)], bucketsB[(i*2)+1],);
                bucketsB[(i*2)]   = null;
                bucketsB[(i*2)+1] = null;
            }
        }
        writeMergeSortedMatrix(bucketsA[0], bucketsA[1], DOStream);


    }*/


    /* TEST CLASS
    Test class to cheeck if is possible to serialize and deserialize an Hashmap of Hashmaps

    private static void provaSer() throws IOException, ClassNotFoundException {
        Long2ObjectOpenHashMap<Int2IntMap> mappa = new Long2ObjectOpenHashMap<>();
        Int2IntMap mappetta1 = new Int2IntOpenHashMap();
        mappetta1.put(10, 10);
        mappetta1.put(20, 23);
        Int2IntMap mappetta2 = new Int2IntOpenHashMap();
        mappetta2.put(-114, 13);
        mappetta2.put(-87, -423);
        mappa.put((long) 1, mappetta1);
        mappa.put((long) 2, mappetta2);
        ObjectOutputStream OOStream = getOOStream("out.bin", true);
        OOStream.writeObject(mappa);
        Long2ObjectOpenHashMap<Int2IntMap> fastQueryTrace = (Long2ObjectOpenHashMap<Int2IntMap>) (new ObjectInputStream(new FileInputStream("out.bin"))).readObject(); //**
        //System.out.println(fastQueryTrace.get((long) 1).get(10));
        System.out.println(fastQueryTrace.get((long) 2).get(-87));
        System.exit(1);
    }*/