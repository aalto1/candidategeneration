package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.*;
import java.util.Arrays;

/**
 * Created by aalto on 9/11/16.
 */
class utilsClass {

    static int[] readClueWebDocument(String [] line, DataInputStream stream) throws IOException {
        byte [] rawDoc = new byte[Integer.parseInt(line[3])];
        for (int i = 0; i < rawDoc.length; i++) {
            rawDoc[i] = stream.readByte();
        }
        return decodeRawDoc(rawDoc, Integer.parseInt(line[4]));
    }

    protected static int getBM25(int [] globalStats, int docLen, int f, int n) {
        /*global statistics for BM25*/
        int N = globalStats[0];
        double avg = globalStats[1] / N;
        double k = 1.6;
        double b = 0.75;
        double IDF = java.lang.Math.log((N - n + 0.5 )/( n + 0.5));
        double BM25 = (IDF * f * k + 1) / (f + k * (1 - b + (b* docLen / avg)));
        //if(BM25>100) System.out.println(BM25);
        return (int) (BM25*Math.pow(10, 7));
    }


    static int [] hashMapToArray(Int2IntMap map, int multipleOccurence){
        /*This function convert an HashMap to an 1d array with the dimension doubled respect to the key-value pairs
        * with a value bigger than one*/

        int [] array = new int[multipleOccurence*2];
        int value;
        int k = 0;
        for(int key : map.keySet()){
            value = map.get(key);
            if((value)>1){                          //becasue we want to reduce the overhead (20% space saved per dump)
                array[k*2] = key;
                array[k*2+1] = value;
                k++;
            }
        }
        return Arrays.copyOf(array, k*2+1);
    }

    static Int2IntMap arrayToHashMap(int [] array){
        /*This function converst an 1d array to an hashmap. We use this function to get the term freq. for a specific
        * term within a doc*/

        Int2IntMap map = new Int2IntOpenHashMap();
        for(int k = 0; k<array.length-1; k+=2){
            map.put(array[k], array[k+1]);
        }
        return map;
    }

    static long getPair(int a , int b){
        return (long)a << 32 | b & 0xFFFFFFFFL;
    }

    static int[] getTerms(long c){
        int aBack = (int)(c >> 32);
        int bBack = (int)c;
        return new int[]{aBack,bBack};
    }

    static ObjectInputStream getOIStream(String filename, boolean buffered) throws IOException {
        if(buffered) return new ObjectInputStream(new FileInputStream(filename+".bin"));
        else return new ObjectInputStream( new BufferedInputStream(new FileInputStream(filename+".bin")));

    }

    static ObjectOutputStream getOOStream(String filename, boolean buffered) throws IOException {
        if(buffered) return new ObjectOutputStream(new FileOutputStream(filename+".bin"));
        else return new ObjectOutputStream( new BufferedOutputStream(new FileOutputStream(filename+".bin")));
    }

    static void serialize(Object e, String filename) {
        try {
            ObjectOutputStream OOStream = getOOStream(filename, true);
            OOStream.writeObject(e);
            OOStream.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    static Object deserialize(String file) {
        System.out.print("Fetching: " + file + "...");
        Object e = null;
        try {
            ObjectInputStream OIStream = getOIStream(file, true);
            e = OIStream.readObject();
            OIStream.close();
            System.out.println("\tfetched!");
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }
    /*528184109
    * #documnets: 50220423
    * rate: 10000000*/
    static boolean checkProgress(int p, int max, int rate, double start, int limit){
        if(p % rate == 0){
            int percentage = (int) (p*100.0)/max;
            System.out.println("Work in progress: " + percentage+ "%\tProcessing Time: " + (p / (System.currentTimeMillis() - start)) * 1000 + "doc/s. \tProcessed: " +p);
            System.out.println("Expected Remaining Time: "+ (((System.currentTimeMillis() - start)/percentage)*(10-percentage))/60000 + " minutes");
            //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
        }
        if(p>limit) return false;
        else return true;
    }

    /*  This function decompress the forward index using opposite variable byte decoding
    0xff = 255 = 11111111 = u-byte */

    static int[] decodeRawDoc(byte[] rawDoc, int docLen) {
        int k = 0;
        int [] numbers = new int[docLen];
        int n = 0;
        for (byte b : rawDoc) {
            if ((b & 0xff) >= 128) {
                n = 128 * n + (b & 0xff);
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                numbers[k] = num;
                if(num<0) System.out.println(num);
                k++;
                n = 0;
            }
        }
        return numbers;
    }




}
