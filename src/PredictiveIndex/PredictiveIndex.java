package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

import static PredictiveIndex.InvertedIndex.*;


/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {
    static int counter = 1;
    static int counter2 = 1;

    public static void main(String [] args) throws IOException, ClassNotFoundException {
        /*We get the global statistics of the collection (fetch from memory if present, compute them if the opposite)
        * and than build the pair-distance from memory.*/
        superMagic();
        //s2();
        //read();
        fetchInvertedIndex();

        String data = "/home/aalto/IdeaProjects/PredictiveIndex/data/en0000";
        InvertedIndex ps;


        if (Files.exists(Paths.get(tPath+ser))) {
            System.out.println("Deserializing Predictive Inverted...");
            ps = new InvertedIndex((HashMap<Integer, Integer>) deserialize(fPath+ser), (int[]) deserialize(sPath+ser));
            System.out.println("Predictive Index Deserialized");
        }else {
            ps = new InvertedIndex();
            ps.getCollectionMetadata(data);
        }
        //ps.threads();
        ps.buildIndex();

    }
    public static void superMagic(){
        LinkedList<int[]> prova = new LinkedList<>();
        prova.add(new int[]{1,2});
        for (int i : prova.getFirst()) {
            System.out.print(i);
        }
        double[][] kickerNumbers = new double[50000000][2];
        for (int i = 0; i < kickerNumbers.length; i++) {
            for (int j = 0; j < kickerNumbers[0].length ; j++) {
                kickerNumbers[i][j] = Math.random();
            }
        }
        long now = System.currentTimeMillis();
        java.util.Arrays.sort(kickerNumbers, new Comparator<double[]>() {
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

    public static void s2(){
        double aux = 2.333345634335;
        System.out.println(((float) aux*(Math.pow(10, String.valueOf(aux).length()-2))));
        System.exit(1);
    }

    public static void read() throws IOException {
        DataInputStream aux = new DataInputStream( new FastBufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/src/PredictiveIndex/tmp/dump/zio2.bin")));
        int k=0;
        long zio = aux.readLong();
        while (true) {
            try {
                while (k < 3) {
                    System.out.print(zio+"-");
                    zio = aux.readLong();
                    //TimeUnit.MILLISECONDS.sleep(100);
                    k++;
                }
                k=0;
                System.out.println();
            }catch (Exception a){
                break;
            }
        }System.exit(1);
    }

    public static int[] getEntry(DataInputStream dataStream) throws IOException {
        //OK
        int [] aux = new int[4];
        try{
            for(int k = 0; k<aux.length; k++) aux[k] = dataStream.readInt();
            counter += 1;
            if(counter % 200000000 == 0) System.out.println("Up to record #" + (counter));
            return aux;
        }catch(EOFException exception){
            System.out.println("Fetching Time: " + (System.currentTimeMillis() - now) + "ms");
            aux[0] = -1;
            return aux;
        }
    }

    public static int [][] fetchInvertedIndex() throws IOException {
        // it is not necessary load the whole inverted index in main memory because that was exactly what we are trying to avoid.

        int IIPointer = 0;
        int maxLength = 0;
        PrintWriter totalIndexStats = new PrintWriter(path+"/totalStatsLength.csv", "UTF-8");
        int [] lengthStats = new int[5000000];
        long now = System.currentTimeMillis();
        long percentage;
        DataInputStream dataStream = new DataInputStream( new BufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.dat")));
        int [][] invertedIndex = new int[528184109][];
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        getEntry(dataStream);
        int [] nowPair = new int[]{-1, -1};
        int [] aux;
        while(true){
            aux = getEntry(dataStream);
            if(aux[0] != nowPair[0] | aux[1] != nowPair[1]){
               if(IIPointer++ % 10000000 == 0){
                   percentage = (long) (IIPointer*100.0)/528184109;
                   System.out.println("Work in progress: " + percentage+ "% completed.");
                   //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
               }
               if(auxPostingList.size()>maxLength) maxLength = auxPostingList.size();
               //lengthStats[auxPostingList.size()]++;
               //invertedIndex[IIPointer] = Ints.toArray(auxPostingList);
               IIPointer++;
               auxPostingList.clear();
               nowPair[0]= aux[0];
               nowPair[1]= aux[1];
               auxPostingList.addLast(aux[0]);
               auxPostingList.addLast(aux[1]);
               auxPostingList.addLast(aux[3]);

           }else if(aux[0]==-1){
                break;
           }else{
               auxPostingList.addLast(aux[3]);
           }
        }
        for(int k =0; k<lengthStats.length ; k++) totalIndexStats.print((lengthStats[k]*k)+",");
        System.out.println(maxLength);
        System.exit(1);
        return invertedIndex;
    }

    static void combinationUtil(int arr[], int data[], int start,
                                int end, int index, int r)
    {
        // Current combination is ready to be printed, print it
        if (index == r) return data;

        // replace index with all possible elements. The condition
        // "end-i+1 >= r-index" makes sure that including one element
        // at index will make a combination with remaining elements
        // at remaining positions
        for (int i=start; i<=end && end-i+1 >= r-index; i++)
        {
            data[index] = arr[i];
            combinationUtil(arr, data, i+1, end, index+1, r);
        }
    }


    public static int[] getQueryBigrams(String line){
        //this method return all the combination of the docID in the document
        String [] query = line.split(",");
        int [] queryInt = new int[query.length];
        for (int i = 0; i <  query.length; i++) {
            queryInt[i] = termsMap.get(String);
        }
        return combinationUtil(queryInt, new int[2], 0, query.length-1, 0, 2 );
    }

    public static int[] getQueryTopkDocIDs(String line){
        //declare a new object every time

        return new int[]{1,2};
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

    public static HashMap<Integer,LinkedList<int[]>> fetchQueries() throws IOException, ClassNotFoundException {

        if(Files.exists(Paths.get("path"))){
            ObjectInputStream out = new ObjectInputStream(new BufferedInputStream(new FileInputStream("ciao")));
            return (HashMap<Integer,LinkedList<int[]>>) out.readObject();
        }else{
            return reformatQueryTrace("file");
        }
    }

    public int getBucketLength(int size) {

        switch (size) {
            case size < 10:
                return ;
            case size >10
        }
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
                auxPostingList.addLast(aux[0]);
                auxPostingList.addLast(aux[1]);
                auxPostingList.addLast(aux[3]);

            }else if(aux[0]==-1){
                break;
            }else{
                auxPostingList.addLast(aux[3]);
            }
        }

        }



    }





}






